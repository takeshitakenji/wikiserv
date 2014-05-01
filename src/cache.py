#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import struct
from datetime import datetime, timedelta
from pytz import utc
import filestuff
import fcntl, os.path, stat, os
from os import fstat, mkdir, fchmod, chmod
from os.path import join as path_join, isdir, isfile, normpath



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
	__slots__ = 'size', 'timestamp', 'checksum'
	MAGIC = b'\xCA\xCE01'
	struct_fmt = '!IQIH'
	minsize = len(MAGIC) + len(struct.pack(struct_fmt, 0, 0, 0, 0))
	def __init__(self, size, timestamp, checksum):
		if size > 0xFFFFFFFF:
			raise ValueError('Size is too large')
		if len(checksum) > 0xFFFF:
			raise ValueError('Checksum is too long')
		self.size, self.timestamp, self.checksum = size, timestamp, checksum
	def __eq__(self, other):
		if not all((hasattr(other, attr) for attr in ['size', 'timestamp', 'checksum'])):
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
		count += stream.write(struct.pack(self.struct_fmt, self.size, seconds, microseconds, len(self.checksum)))
		count += stream.write(self.checksum)
		return count
	@classmethod
	def read(cls, stream):
		buff = stream.read(cls.minsize)
		if buff[:len(cls.MAGIC)] != cls.MAGIC:
			raise ValueError('This is not a recognized format')
		size, seconds, microseconds, cksum_len = struct.unpack(cls.struct_fmt, buff[len(cls.MAGIC):])
		timestamp = cls.fp2datetime(seconds, microseconds, utc)
		checksum = None
		if cksum_len > 0:
			checksum = stream.read(cksum_len)
		if len(checksum) < cksum_len:
			raise ValueError('Invalid checksum length')
		return cls(size, timestamp, checksum)


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
			fcntl.lockf(self.__handle, fcntl.LOCK_EX)
			self.__handle.close()
			self.__handle = None
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
	def __enter__(self):
		if self.__mode == self.EXCLUSIVE:
			self.__fd = open(self.__path, 'wb')
			fcntl.lockf(self.__fd, fcntl.LOCK_EX)
		else:
			self.__fd = open(self.__path, 'rb')
			fcntl.lockf(self.__fd, fcntl.LOCK_SH)
	def __exit__(self, type, value, tb):
		fcntl.lockf(self.__fd, fcntl.LOCK_UN)
		self.__fd.close()
		self.__fd = None


class Cache(object):
	__slots__ = '__root', '__filter_function', '__checksum_function', '__source_root', '__lock',

	allbits = stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO

	root_perms = stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR
	file_perms = stat.S_IRUSR|stat.S_IWUSR

	@classmethod
	def fix_perms(cls, handle):
		info = fstat(handle.fileno())
		if (info.st_mode & cls.allbits) != cls.root_perms:
			fchmod(handle.fileno(), cls.root_perms)

	def __init__(self, root, source_root, checksum_function, filter_function):
		self.__root = root
		if not isdir(source_root):
			raise ValueError('Not a directory: %s' % source_root)
		self.__source_root = source_root
		self.__checksum_function = checksum_function
		self.__filter_function = filter_function
		if not isdir(root):
			mkdir(root)
		info = os.stat(root)
		if (info.st_mode & self.allbits) != self.root_perms:
			chmod(self.__root, self.root_perms)
		with open(self.lockfile, 'wb') as lockf:
			self.fix_perms(lockf)
	def __get_entry(self, path):
		path = normpath(path)
		if any((part.startswith('.') for part in path.split(os.path.sep))):
			raise ValueError('Path entries cannot start with "."')
		with FileLock(self.lockfile, FileLock.SHARED):
			entry = None
			try:
				original_path = normpath(path_join(self.__source_root, path))
				with filestuff.LockedFile(original_path) as original:
					cache_path = normpath(path_join(self.__root, path))
					handle = None
					try:
						handle = open(cache_path, 'r+b')
					except IOError:
						handle = open(cache_path, 'w+b')
					self.fix_perms(handle)
					entry = Entry(handle)

					header = entry.header
					new_header = EntryHeader(original.size, original.modified, original.checksum(self.__checksum_function))
					if header != new_header:
						entry.header = new_header
						self.__filter_function(original.handle, entry)
					entry.seek(0)
					return entry
			except IOError:
				if entry is not None:
					entry.close()
				raise KeyError(path)
			except:
				if entry is not None:
					entry.close()
				raise
	def __getitem__(self, path):
		return EntryWrapper(path, self.__get_entry)
	@property
	def lockfile(self):
		return path_join(self.__root, '.lock')


if __name__ == '__main__':
	import unittest
	from os import remove, stat
	from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp
	from hashlib import md5
	from codecs import getreader, getwriter
	from shutil import copyfileobj, rmtree
	from traceback import print_stack

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
			test = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
			self.assertEqual(len(self.FILE_TEXT), test.size)
			self.assertEqual(self.timestamp, test.timestamp)
			self.assertEqual(self.FILE_CHECKSUM, test.checksum)
		def test_bad_checksum(self):
			self.assertRaises(ValueError, EntryHeader, 0, self.timestamp, ' ' * (0xFFFF + 1))
		def test_bad_size(self):
			self.assertRaises(ValueError, EntryHeader, 0xFFFFFFFF + 1, self.timestamp, ' ')
		def test_write(self):
			test = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
			with open(self.path, 'wb') as outf:
				self.assertEqual(test.write(outf), EntryHeader.minsize + len(self.FILE_CHECKSUM))
				self.assertEqual(len(self.FILE_TEXT), outf.write(self.FILE_TEXT))
		def test_read(self):
			test = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
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
			header = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
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
			header = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
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
			header = EntryHeader(len(self.FILE_TEXT), self.timestamp, self.FILE_CHECKSUM)
			entry = Entry(open(self.path, 'w+b'))
			try:
				entry.header = header
				self.assertGreater(entry.write(self.FILE_TEXT), 0)
			finally:
				entry.close()
			header2 = EntryHeader(len(self.FILE_TEXT2), self.timestamp2, self.FILE_CHECKSUM2)
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
	
	class CacheTest(unittest.TestCase):
		def process(self, inf, outf):
			print('PROCESS(INF=%s, OUTF=%s)' % (inf.name, outf.name))
			self.count += 1
			outf.write('TOUCHED\n'.encode('ascii'))
			copyfileobj(inf, outf)
		def setUp(self):
			self.tmpdir = mkdtemp()
			self.cachedir = mkdtemp()
			self.count = 0
			self.cache = Cache(self.cachedir, self.tmpdir, md5, self.process)
		def tearDown(self):
			self.cache = None
			rmtree(self.cachedir)
			rmtree(self.tmpdir)
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
				header = EntryHeader(info.size, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
		def test_update(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with self.cache[temporary] as entry:
				pass
			self.assertEqual(self.count, 1)

			test_string = test_string * 2
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobarfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 2)
		def test_removal(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			with self.cache[temporary] as entry:
				pass
			self.assertEqual(self.count, 1)

			remove(temporary_path)

			self.assertRaises(KeyError, self.cache['invalid'].__enter__)
			self.assertEqual(self.count, 1)
		def test_multiple_hit(self):
			temporary = 'test.txt'
			test_string = 'foobar'
			temporary_path = path_join(self.tmpdir, temporary)
			with open(temporary_path, 'w', encoding = 'ascii') as tmp:
				tmp.write(test_string)
			self.assertTrue(isfile(temporary_path))
			with filestuff.File(temporary_path) as info:
				header = EntryHeader(info.size, info.modified, info.checksum(md5))

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)

			with self.cache[temporary] as entry:
				self.assertEqual(header, entry.header)
				data = entry.read()
				self.assertIsNotNone(data)
				self.assertEqual('TOUCHED\nfoobar'.encode('ascii'), data)
			self.assertEqual(self.count, 1)
	unittest.main()