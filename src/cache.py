#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import struct
from datetime import datetime, timedelta
from pytz import utc
import filestuff, worker, common
import fcntl, os.path, stat, os, itertools
from os import fstat, mkdir, fchmod, chmod, utime, remove
from os.path import join as path_join, isdir, isfile, normpath, dirname, relpath
from traceback import print_exc
from collections import namedtuple
from queue import Queue, Empty
import logging
from threading import Lock
from shutil import copyfileobj


LOGGER = logging.getLogger(__name__)


def timestamps_equivalent(t1, t2, tolerance = 0.001):
	"""
		This function is required when comparing timestamps when the standard
		datetime.timestamp() result is used.  The fixed-point method used now
		obviates the need for this.
	"""
	if isinstance(tolerance, timedelta):
		tolerance = abs(tolerance.total_seconds())
	return abs((t1 - t2).total_seconds()) <= tolerance

class EntryHeader(object):
	__slots__ = 'size', 'cached', 'timestamp', 'checksum'
	MAGIC = b'\xCA\xCE01'
	struct_fmt = '!I?QIH'
	minsize = len(MAGIC) + len(struct.pack(struct_fmt, 0, False, 0, 0, 0))
	def __init__(self, size, cached, timestamp, checksum):
		if size > 0xFFFFFFFF:
			raise ValueError('Size is too large')
		if len(checksum) > 0xFFFF:
			raise ValueError('Checksum is too long')
		self.size, self.cached, self.timestamp, self.checksum = size, bool(cached), timestamp, checksum
	def __eq__(self, other):
		if not all((hasattr(other, attr) for attr in ['size', 'cached', 'timestamp', 'checksum'])):
			return False
		elif not all([self.cached, other.cached]):
			return False
		else:
			return self.size == other.size and self.timestamp == other.timestamp and self.checksum == other.checksum
	@staticmethod
	def datetime2fp(dt):
		seconds = int(dt.timestamp())
		if seconds < 0:
			seconds -= 1
		return seconds, dt.microsecond
	@staticmethod
	def fp2datetime(s, ms, tzinfo):
		return datetime.utcfromtimestamp(s).replace(microsecond = ms, tzinfo = tzinfo)
	def write(self, stream):
		seconds, microseconds = self.datetime2fp(self.timestamp)
		count = stream.write(self.MAGIC)
		count += stream.write(struct.pack(self.struct_fmt, self.size, self.cached, seconds, microseconds, len(self.checksum)))
		count += stream.write(self.checksum)
		return count
	@classmethod
	def read(cls, stream):
		buff = stream.read(cls.minsize)
		if buff[:len(cls.MAGIC)] != cls.MAGIC:
			raise ValueError('This is not a recognized format')
		try:
			size, cached, seconds, microseconds, cksum_len = struct.unpack(cls.struct_fmt, buff[len(cls.MAGIC):])
		except struct.error:
			raise IOError
		timestamp = cls.fp2datetime(seconds, microseconds, utc)
		checksum = None
		if cksum_len > 0:
			checksum = stream.read(cksum_len)
		if len(checksum) < cksum_len:
			raise ValueError('Invalid checksum length')
		return cls(size, cached, timestamp, checksum)


class EntryWrapper(object):
	__slots__ = '__key', '__source', '__entry'
	def __init__(self, key, source):
		self.__key, self.__source = key, source
		self.__entry = None
	def __enter__(self):
		if self.__key is None or self.__entry is not None:
			raise RuntimeError
		self.__entry = self.__source(self.__key)
		self.__key = None
		return self.__entry
	def __exit__(self, type, value, tb):
		self.__entry.close()
		self.__entry = None


class Entry(object):
	__slots__ = '__handle', '__header', '__payload_start', '__active'
	def __init__(self, handle):
		self.__handle = handle
		fcntl.lockf(self.__handle, fcntl.LOCK_EX)

		info = fstat(self.__handle.fileno())
		self.__active = (info.st_size >= EntryHeader.minsize)

		try:
			self.__header = EntryHeader.read(self.__handle) if self.__active else None
		except ValueError:
			self.__active = False
			self.__header = None
		self.__payload_start = self.__handle.tell() if self.__active else None
	def close(self):
		if self.__handle is not None:
			utime(self.__handle.fileno())
			fcntl.lockf(self.__handle, fcntl.LOCK_EX)
			self.__handle.close()
			self.__handle = None
	def __call__(self, outf):
		copyfileobj(self, outf)
	def __del__(self):
		self.close()
	@property
	def active(self):
		return self.__active
	@property
	def header(self):
		return self.__header
	@header.setter
	def header(self, header):
		"Marks the file for truncation and recreation"
		if not isinstance(header, EntryHeader):
			raise ValueError('Invalid EntryHeader')
		self.__header = header
		self.__handle.seek(0)
		self.__handle.truncate(0)
		self.__header.write(self.__handle)
		self.__handle.flush()
		self.__payload_start = self.__handle.tell()
		self.__active = True
	def seek(self, pos):
		if not self.__active:
			raise RuntimeError('Entry is not available for seeking')
		self.__handle.seek(self.__payload_start + pos)
	def read(self, length = None):
		if not self.__active:
			raise RuntimeError('Entry is not available for reading')
		return self.__handle.read(length)
	def write(self, s):
		return self.__handle.write(s)
	@property
	def name(self):
		return self.__handle.name
	def fileno(self):
		return self.__handle.fileno()


class FileLock(object):
	EXCLUSIVE, SHARED = range(2)
	__slots__ = '__path', '__mode', '__fd'
	def __init__(self, path, mode):
		if mode not in [self.EXCLUSIVE, self.SHARED]:
			raise ValueError('Invalid mode')
		self.__path = path
		self.__mode = mode
		with open(self.__path, 'wb'):
			pass
		self.__fd = None
	def acquire(self):
		if self.__mode == self.EXCLUSIVE:
			self.__fd = open(self.__path, 'wb')
			fcntl.lockf(self.__fd, fcntl.LOCK_EX)
		else:
			self.__fd = open(self.__path, 'rb')
			fcntl.lockf(self.__fd, fcntl.LOCK_SH)
	def release(self):
		fcntl.lockf(self.__fd, fcntl.LOCK_UN)
		self.__fd.close()
		self.__fd = None
	def __enter__(self):
		self.acquire()
	def __exit__(self, type, value, tb):
		self.release()


class NoCache(Exception):
	pass


class AutoProcess(object):
	__slots__ = '__inf', '__method', '__header'
	def __init__(self, header, inf, method):
		self.__header = header
		self.__inf = inf
		self.__method = method
	@property
	def header(self):
		return self.__header
	def __call__(self, outf):
		LOGGER.debug('Autprocess executing')
		with self.__inf as inf:
			return self.__method(inf.handle, outf, False)
	def close(self):
		pass


class Cache(object):
	@staticmethod
	def find_files(root):
		for path, dnames, fnames in os.walk(root):
			filtered_dnames = [d for d in dnames if not d.startswith('.')]
			del dnames[:]
			dnames.extend(filtered_dnames)
			for fname in fnames:
				if not fname.startswith('.'):
					yield path_join(path, fname)
	@staticmethod
	def find_dirs(root):
		for path, dnames, fname in os.walk(root, topdown = False):
			if path == root:
				continue
			elif any((d.startswith('.') for d in path.split(os.path.sep))):
				continue
			else:
				yield path
	@staticmethod
	def mkdir_p(root, name):
		root_parts = root.split(os.path.sep)
		parts = name.split(os.path.sep)
		for i in range(len(parts)):
			if i < len(root_parts):
				if root_parts[i] != parts[i]:
					raise ValueError('name is not under root')
			else:
				try:
					path = os.path.join(*parts[:i + 1])
					if parts[0] == '':
						path = '/' + path
					mkdir(path)
				except OSError:
					continue

	Options = namedtuple('Options', ['max_age', 'max_entries', 'auto_scrub'])
	__slots__ = '__root', '__filter_function', '__checksum_function', '__source_root', '__lock', '__known_entry_count', '__options',
	def __init__(self, root, source_root, checksum_function, filter_function, max_age = None, max_entries = None, auto_scrub = False):
		self.__root = root
		if not isdir(source_root):
			raise ValueError('Not a directory: %s' % source_root)
		self.__source_root = source_root
		self.__checksum_function = checksum_function
		self.__filter_function = filter_function
		self.__known_entry_count = None
		
		# Create files
		if not isdir(root):
			mkdir(root)
		common.fix_dir_perms(self.__root)
		with open(self.lockfile, 'wb') as lockf:
			common.fix_perms(lockf)

		# Store options
		if max_age is not None and not isinstance(max_age, timedelta):
			max_age = timedelta(seconds = max_age)
		if max_entries is not None:
			max_entries = int(max_entries)
			if max_entries < 2:
				raise ValueError('Invalid number of maximum entries: %d' % max_entries)
		auto_scrub = bool(auto_scrub)
		self.__options = self.Options(max_age, max_entries, auto_scrub)

		# Scrub to set up the structures for the first time
		self.scrub()
	
	def __enter__(self):
		return self
	def __exit__(self, type, value, tb):
		self.close()
	def close(self):
		pass
	def __str__(self):
		return 'Cache at %s mirroring original %s' % (self.__root, self.__source_root)
	def schedule_scrub(self, tentative = False):
		return self.scrub(tentative)
	def __get_entry(self, path):
		path = normpath(path)
		if any((part.startswith('.') for part in path.split(os.path.sep))):
			raise ValueError('Path entries cannot start with "."')

		if self.options.auto_scrub and self.options.max_entries is not None:
			LOGGER.debug('Scheduling a scrub because max_entries=%s and auto_scrub=True' % self.options.max_entries)
			self.schedule_scrub(True)
		
		with FileLock(self.lockfile, FileLock.SHARED):
			LOGGER.debug('Got original at %s' % path)
			entry = None
			try:
				original_path = normpath(path_join(self.__source_root, path))
				with filestuff.LockedFile(original_path) as original:
					LOGGER.debug('Opening entry at %s' % path)
					cache_path = normpath(path_join(self.__root, path))
					self.mkdir_p(self.__root, dirname(cache_path))
					handle = None
					update = False
					try:
						handle = open(cache_path, 'r+b')
						LOGGER.debug('Entry exists at %s' % path)
						update = True
					except IOError:
						handle = open(cache_path, 'w+b')
						LOGGER.debug('Entry does not exist at %s' % path)
						update = False
					common.fix_perms(handle)
					entry = Entry(handle)

					header = entry.header
					new_header = EntryHeader(original.size, True, original.modified, original.checksum(self.__checksum_function))
					if header is not None and not header.cached:
						LOGGER.debug('Not cached for %s' % path)
						if header != new_header:
							# If anything has changed, update the entry.
							entry.header = EntryHeader(new_header.size, False, new_header.timestamp, new_header.checksum)
						# The lock will be acquired after original has been freed
						return AutoProcess(entry.header, filestuff.LockedFile(original_path), self.__filter_function)
					if header != new_header:
						LOGGER.debug('Calling processor for %s' % path)
						try:
							entry.header = new_header
							self.__filter_function(original.handle, entry, True)
						except NoCache:
							LOGGER.debug('%s does not want to be cached' % path)
							# Flag the entry as no-cache
							entry.header = EntryHeader(new_header.size, False, new_header.timestamp, new_header.checksum)
							try:
								entry.close()
							except:
								LOGGER.exception('When closing entry %s' % entry.header)
							# The lock will be acquired after original has been freed
							return AutoProcess(entry.header, filestuff.LockedFile(original_path), self.__filter_function)
						except NotImplementedError:
							# Truncate the entry
							entry.header = new_header
					entry.seek(0)
					if not update:
						LOGGER.debug('Adding new entry for %s' % path)
						self.__known_entry_count += 1
					return entry
			except IOError:
				print('FOO')
				if entry is not None:
					try:
						entry.close()
					except:
						pass
				raise KeyError(path)
			except:
				if entry is not None:
					try:
						remove(entry.name)
						entry.close()
					except:
						pass
				raise
	def __getitem__(self, path):
		return EntryWrapper(path, self.__get_entry)
	@property
	def lockfile(self):
		return path_join(self.__root, '.lock')
	@property
	def source_root(self):
		return self.__source_root
	@property
	def options(self):
		return self.__options
	def __len__(self):
		"Only call this from outside of this class."
		with FileLock(self.lockfile, FileLock.EXCLUSIVE):
			return self.__known_entry_count
	def scrub(self, tentative = False):
		if tentative and self.options.max_entries is not None:
			LOGGER.debug('Performing check because tentative = True')
			with FileLock(self.lockfile, FileLock.SHARED):
				if self.__known_entry_count < self.options.max_entries:
					# This is < because when tentative == True, an entry
					# may be inserted.
					return False
		LOGGER.info('Scrubbing cache %s' % self)

		with FileLock(self.lockfile, FileLock.EXCLUSIVE):
			entries = []
			cutoff = None
			if self.options.max_age is not None:
				cutoff = datetime.utcnow().replace(tzinfo = utc) - self.options.max_age
			for fname in self.find_files(self.__root):
				with filestuff.ExclusivelyLockedFile(fname) as entry:
					relative = relpath(fname, self.__root)
					# Make sure original still exists, deleting cache entry otherwise
					original = path_join(self.__source_root, relative)
					if not isfile(original):
						remove(entry.name)
						continue
					timestamp = entry.modified
					# Check age
					if cutoff is not None and timestamp < cutoff:
						remove(entry.name)
						continue
					# Count as entry if it is young enough
					entries.append((fname, timestamp))

			# Check timestamps when seeing if a file should be deleted in LRU mode
			#     if they differ, skip that file
			# Stop when enough files have been deleted
			ecount = len(entries)
			if self.options.max_entries is not None and ecount >= self.options.max_entries:
				equeue = Queue()
				for entry in sorted(entries, key = lambda x: x[1]):
					equeue.put(entry)

				try:
					while ecount > 0 and ecount >= self.options.max_entries:
						fname, timestamp = equeue.get(False)
						with filestuff.ExclusivelyLockedFile(fname) as entry:
							if entry.modified > timestamp:
								equeue.put((fname, timestamp))
							else:
								remove(fname)
								ecount -= 1
							equeue.task_done()
				except Empty:
					pass

			
			for dname in self.find_dirs(self.__root):
				try:
					rmdir(dname)
				except OSError:
					# Directory is not empty
					continue

			self.__known_entry_count = ecount
			return True

class DispatcherCache(Cache):
	__slots__ = '__worker', '__wlock',
	def __init__(self, root, source_root, checksum_function, filter_function, max_age = None, max_entries = None, auto_scrub = False):
		Cache.__init__(self, root, source_root, checksum_function, filter_function, max_age, max_entries, auto_scrub)
		self.__wlock = Lock()
		self.__worker = worker.Worker(autostart = True)
	def schedule_scrub(self, tentative = False):
		return self.__worker.schedule(self.scrub)
	def close(self):
		with self.__wlock:
			if self.__worker is not None:
				self.__worker.finish()
				self.__worker.join()
				self.__worker = None

if __name__ == '__main__':
	import unittest
	from os import remove, stat
	from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp
	from hashlib import md5
	from codecs import getreader, getwriter
	from shutil import copyfileobj, rmtree
	from traceback import print_stack
	from time import sleep

	logging.basicConfig(level = logging.DEBUG)

	def hashstring(s, cksum_type):
		hasher = cksum_type()
		hasher.update(s)
		return hasher.digest()
	

	class DateTest(unittest.TestCase):
		NEG_TIMESTAMP = datetime(1900, 1, 1, 5, 30, 29, 12345, utc)
		POS_TIMESTAMP = datetime(2000, 1, 1, 5, 30, 29, 12345, utc)
		ZERO_TIMESTAMP = datetime.utcfromtimestamp(.001).replace(tzinfo = utc)
		def test_negative(self):
			seconds, microseconds = EntryHeader.datetime2fp(self.NEG_TIMESTAMP)
			self.assertLess(seconds, 0)
			self.assertGreater(microseconds, 0)
			self.assertEqual(self.NEG_TIMESTAMP, EntryHeader.fp2datetime(seconds, microseconds, self.NEG_TIMESTAMP.tzinfo))
		def test_positive(self):
			seconds, microseconds = EntryHeader.datetime2fp(self.POS_TIMESTAMP)
			self.assertGreater(seconds, 0)
			self.assertGreater(microseconds, 0)
			self.assertEqual(self.POS_TIMESTAMP, EntryHeader.fp2datetime(seconds, microseconds, self.POS_TIMESTAMP.tzinfo))
		def test_zero(self):
			seconds, microseconds = EntryHeader.datetime2fp(self.ZERO_TIMESTAMP)
			self.assertEqual(seconds, 0)
			self.assertGreater(microseconds, 0)
			self.assertEqual(self.ZERO_TIMESTAMP, EntryHeader.fp2datetime(seconds, microseconds, self.ZERO_TIMESTAMP.tzinfo))

	class EntryHeaderTest(unittest.TestCase):
		FILE_TEXT = 'TEST FILE\n'.encode('ascii')
		FILE_CHECKSUM = hashstring(FILE_TEXT, md5)
		def setUp(self):
			with NamedTemporaryFile(delete = False) as tmp:
				self.path = tmp.name
			self.timestamp = datetime.utcnow().replace(tzinfo = utc)
		def tearDown(self):
			remove(self.path)
		def test_basic(self):
			test = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			self.assertEqual(len(self.FILE_TEXT), test.size)
			self.assertEqual(self.timestamp, test.timestamp)
			self.assertEqual(self.FILE_CHECKSUM, test.checksum)
		def test_bad_checksum(self):
			self.assertRaises(ValueError, EntryHeader, 0, True, self.timestamp, ' ' * (0xFFFF + 1))
		def test_bad_size(self):
			self.assertRaises(ValueError, EntryHeader, 0xFFFFFFFF + 1, True, self.timestamp, ' ')
		def test_write(self):
			test = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			with open(self.path, 'wb') as outf:
				self.assertEqual(test.write(outf), EntryHeader.minsize + len(self.FILE_CHECKSUM))
				self.assertEqual(len(self.FILE_TEXT), outf.write(self.FILE_TEXT))
		def test_read(self):
			test = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			with open(self.path, 'wb') as outf:
				test.write(outf)
				outf.write(self.FILE_TEXT)

			test2 = None
			with open(self.path, 'rb') as inf:
				test2 = EntryHeader.read(inf)
				self.assertIsNotNone(test2)
				self.assertEqual(inf.read(), self.FILE_TEXT)

			self.assertEqual(len(self.FILE_TEXT), test2.size)
			self.assertEqual(self.timestamp, test2.timestamp)
			self.assertTrue(timestamps_equivalent(self.timestamp, test2.timestamp))
			self.assertEqual(self.FILE_CHECKSUM, test2.checksum)
	
	class EntryWrapperTest(unittest.TestCase):
		class MockCache(object):
			class Entry(object):
				def __init__(self, key, close):
					self.key, self.__close = key, close
				def close(self):
					self.__close(self.key)
				def __del__(self):
					self.close()
			def __init__(self):
				self.entries = {}
			def get_entry(self, key):
				self.entries[key] = True
				return self.Entry(key, self.close_entry)
			def close_entry(self, key):
				self.entries[key] = False
			def __getitem__(self, key):
				return EntryWrapper(key, self.get_entry)
		def setUp(self):
			self.cache = self.MockCache()
		def tearDown(self):
			pass
		def test_basic(self):
			key = 'TEST'
			with self.cache[key] as entry:
				self.assertEqual(entry.key, key)
				self.assertGreater(len(self.cache.entries), 0)
				self.assertIn(key, self.cache.entries)
				self.assertIs(self.cache.entries[key], True)
			self.assertIn(key, self.cache.entries)
			self.assertIs(self.cache.entries[key], False)

	class EntryTest(unittest.TestCase):
		# Need to use r+b for reading because of LOCK_EX
		FILE_TEXT = 'TEST FILE\n'.encode('ascii')
		FILE_CHECKSUM = hashstring(FILE_TEXT, md5)

		FILE_TEXT2 = 2 * FILE_TEXT
		FILE_CHECKSUM2 = hashstring(FILE_TEXT2, md5)
		def setUp(self):
			with NamedTemporaryFile(delete = False) as tmp:
				self.path = tmp.name
			self.timestamp = datetime.utcnow().replace(tzinfo = utc)
			self.timestamp2 = self.timestamp + timedelta(days = 1)
		def tearDown(self):
			remove(self.path)
		def test_fresh(self):
			entry = Entry(open(self.path, 'r+b'))
			try:
				self.assertFalse(entry.active)
				self.assertIsNone(entry.header)
			finally:
				entry.close()
		def test_create(self):
			header = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			entry = Entry(open(self.path, 'w+b'))
			try:
				entry.header = header
				self.assertTrue(entry.active)
				self.assertIsNotNone(entry.header)
				self.assertEqual(entry.header, header)
				self.assertGreater(entry.write(self.FILE_TEXT), 0)
				entry.seek(0)
				self.assertEqual(entry.read(), self.FILE_TEXT)
			finally:
				entry.close()
		def test_create_read(self):
			header = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			entry = Entry(open(self.path, 'w+b'))
			try:
				entry.header = header
				self.assertGreater(entry.write(self.FILE_TEXT), 0)
			finally:
				entry.close()
			entry = Entry(open(self.path, 'r+b'))
			try:
				self.assertTrue(entry.active)
				self.assertIsNotNone(entry.header)
				self.assertEqual(entry.header, header)
				self.assertEqual(entry.read(), self.FILE_TEXT)
			finally:
				entry.close()
		def test_create_overwrite(self):
			header = EntryHeader(len(self.FILE_TEXT), True, self.timestamp, self.FILE_CHECKSUM)
			entry = Entry(open(self.path, 'w+b'))
			try:
				entry.header = header
				self.assertGreater(entry.write(self.FILE_TEXT), 0)
			finally:
				entry.close()
			header2 = EntryHeader(len(self.FILE_TEXT2), True, self.timestamp2, self.FILE_CHECKSUM2)
			entry = Entry(open(self.path, 'r+b'))
			try:
				self.assertTrue(entry.active)
				entry.header = header2
				entry.write(self.FILE_TEXT2)
			finally:
				entry.close()

			entry = Entry(open(self.path, 'r+b'))
			try:
				self.assertTrue(entry.active)
				self.assertIsNotNone(entry.header)
				self.assertEqual(entry.header, header2)
				self.assertEqual(entry.read(), self.FILE_TEXT2)
			finally:
				entry.close()
	
	class FindTest(unittest.TestCase):
		def setUp(self):
			self.tmpdir = mkdtemp()

			good_root = path_join(self.tmpdir, 'good')
			mkdir(good_root)

			good_sub = path_join(good_root, 'subgood')
			mkdir(good_sub)

			good_good_file = path_join(good_root, 'good.txt')
			with open(good_good_file, 'w') as f:
				print('good', file = f)
			good_bad_file = path_join(good_root, '.bad.txt')
			with open(good_bad_file, 'w') as f:
				print('bad', file = f)

			bad_root = path_join(self.tmpdir, '.bad')
			mkdir(bad_root)

			bad_sub = path_join(bad_root, 'subbad')
			mkdir(bad_sub)

			bad_sub = path_join(bad_root, '.subbad')
			mkdir(bad_sub)

			bad_sub = path_join(bad_sub, 'subbad2')
			mkdir(bad_sub)

			bad_good_file = path_join(bad_root, 'good.txt')
			with open(bad_good_file, 'w') as f:
				print('_good', file = f)
			bad_bad_file = path_join(bad_root, '.bad.txt')
			with open(bad_bad_file, 'w') as f:
				print('_bad', file = f)
		def tearDown(self):
			rmtree(self.tmpdir)
		def test_find_files(self):
			files = [relpath(x, self.tmpdir) for x in Cache.find_files(self.tmpdir)]
			self.assertEqual(files, ['good/good.txt'])
		def test_find_dirs(self):
			dirs = [relpath(x, self.tmpdir) for x in Cache.find_dirs(self.tmpdir)]
			self.assertEqual(dirs, ['good/subgood', 'good'])

	class BaseCacheTest(unittest.TestCase):
		def process(self, inf, outf, cached):
			print('PROCESS(INF=%s, OUTF=%s)' % (inf.name, outf.name))
			self.count += 1
			outf.write('TOUCHED\n'.encode('ascii'))
			copyfileobj(inf, outf)
		def get_cache(self, cachedir, tmpdir):
			raise NotImplementedError
		def setUp(self):
			self.tmpdir = mkdtemp()
			self.cachedir = mkdtemp()
			self.count = 0
			self.cache = self.get_cache(self.cachedir, self.tmpdir)
		def tearDown(self):
			self.cache.close()
			self.cache = None
			rmtree(self.cachedir)
			rmtree(self.tmpdir)
	
	class CacheTest(BaseCacheTest):
		def get_cache(self, cachedir, tmpdir):
			return Cache(self.cachedir, self.tmpdir, md5, self.process)
		def test_exists(self):
			self.assertTrue(isfile(self.cache.lockfile))
		def test_invalid_file(self):
			self.assertRaises(KeyError, self.cache['invalid'].__enter__)
			self.assertEqual(self.count, 0)
			self.assertRaises(KeyError, self.cache['invalid'].__enter__)
			self.assertEqual(self.count, 0)
		def test_initial_miss(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			self.assertTrue(isfile(temporary_path))
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, True, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)
		def test_subdir(self):
			temporary = 'parent/test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			mkdir(dirname(temporary_path))
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			self.assertTrue(isfile(temporary_path))
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, True, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)
		def test_update(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with self.cache[temporary] as entry:
				pass
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			test_string = test_string * 2
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, True, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobarfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 2)
			self.assertEqual(len(self.cache), 1)
		def test_removal(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with self.cache[temporary] as entry:
				pass
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			remove(temporary_path)

			self.assertRaises(KeyError, self.cache[temporary].__enter__)
			self.assertEqual(self.count, 1)

			# As configured, this will only delete cache files without any original present
			self.cache.scrub()
			self.assertEqual(list(Cache.find_files(self.cachedir)), [])
			self.assertEqual(list(Cache.find_dirs(self.cachedir)), [])

			self.assertRaises(KeyError, self.cache[temporary].__enter__)
			self.assertEqual(self.count, 1)
		def test_multiple_hit(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			self.assertTrue(isfile(temporary_path))
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, True, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)
	class ExpiringCacheTest(BaseCacheTest):
		def get_cache(self, cachedir, tmpdir):
			return Cache(self.cachedir, self.tmpdir, md5, self.process, max_age = timedelta(seconds = 1))
		def test_expiration(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			self.assertTrue(isfile(temporary_path))
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, True, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			sleep(3)
			self.cache.scrub()

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 2)
			self.assertEqual(len(self.cache), 1)
	class LRUCacheTest(BaseCacheTest):
		def get_cache(self, cachedir, tmpdir):
			return Cache(self.cachedir, self.tmpdir, md5, self.process, max_entries = 5)
		def test_exceed(self):
			infiles = ['%d.txt' % i for i in range(1, 7)]
			content = {}
			for infile in infiles:
				with open(path_join(self.tmpdir, infile), 'w', encoding = 'ascii') as f:
					content[infile] = ('FILE=%s' % infile)
					f.write(content[infile])
					sleep(0.1)

			for infile in infiles[:5]:
				with self.cache[infile] as entry:
					reader = getreader('ascii')(entry)
					data = reader.read()
					self.assertEqual('TOUCHED\n' + content[infile], data)
					sleep(0.1)
			self.assertEqual(self.count, 5)
			self.assertEqual(len(self.cache), 5)

			infile = infiles[5]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 6)

			self.cache.scrub()

			infile = infiles[0]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 7)
			self.assertEqual(len(self.cache), 5)
	class AutoLRUCacheTest(BaseCacheTest):
		def get_cache(self, cachedir, tmpdir):
			return Cache(self.cachedir, self.tmpdir, md5, self.process, max_entries = 5, auto_scrub = True)
		def test_exceed(self):
			infiles = ['%d.txt' % i for i in range(1, 7)]
			content = {}
			for infile in infiles:
				with open(path_join(self.tmpdir, infile), 'w', encoding = 'ascii') as f:
					content[infile] = ('FILE=%s' % infile)
					f.write(content[infile])
					sleep(0.1)

			for infile in infiles[:5]:
				with self.cache[infile] as entry:
					reader = getreader('ascii')(entry)
					data = reader.read()
					self.assertEqual('TOUCHED\n' + content[infile], data)
					sleep(0.1)
			self.assertEqual(self.count, 5)
			self.assertTrue(len(self.cache) <= 5)

			infile = infiles[5]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 6)

			# Should cause an entry to be dropped (it blocks while scrubbing)
			infile = infiles[0]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 7)
			self.assertTrue(len(self.cache) <= 5)
	class DispatcherCacheTest(BaseCacheTest):
		def get_cache(self, cachedir, tmpdir):
			return DispatcherCache(self.cachedir, self.tmpdir, md5, self.process, max_entries = 5, auto_scrub = True)
		def test_exceed(self):
			infiles = ['%d.txt' % i for i in range(1, 7)]
			content = {}
			for infile in infiles:
				with open(path_join(self.tmpdir, infile), 'w', encoding = 'ascii') as f:
					content[infile] = ('FILE=%s' % infile)
					f.write(content[infile])
					sleep(0.1)

			for infile in infiles[:5]:
				with self.cache[infile] as entry:
					reader = getreader('ascii')(entry)
					data = reader.read()
					self.assertEqual('TOUCHED\n' + content[infile], data)
					sleep(0.1)
			self.assertEqual(self.count, 5)
			self.assertTrue(len(self.cache) <= 5)

			infile = infiles[5]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 6)


			# Should cause an entry to be dropped
			infile = infiles[0]
			with self.cache[infile] as entry:
				reader = getreader('ascii')(entry)
				data = reader.read()
				self.assertEqual('TOUCHED\n' + content[infile], data)
				sleep(0.1)
			self.assertEqual(self.count, 7)
			# Wait for thread to do its business
			sleep(0.5)
			self.assertTrue(len(self.cache) <= 5)
	unittest.main()
