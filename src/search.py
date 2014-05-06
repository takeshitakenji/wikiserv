#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import logging, os, codecs, shelve, pickle, os.path, stat
import config, cache, processors, filestuff, common
from datetime import datetime, timedelta
from pytz import utc
import itertools, functools
from os.path import relpath, basename, join as path_join, dirname
from collections import namedtuple
from threading import Semaphore
from queue import Queue


LOGGER = logging.getLogger(__name__)


def scrub_terms(string, term_cleaner = lambda term: term):
	if not string:
		raise ValueError(string)
	terms = (term_cleaner(x.strip().lower()) for x in string.split())
	# Clean up the mess
	terms = tuple(sorted({x for x in terms if x}))
	if not terms:
		raise ValueError(string)
	return terms

class Filter(object):
	__slots__ = '__string',
	def __init__(self, string):
		if string.startswith('='):
			raise ValueError('Filter string cannot start with =')
		self.__string = string
	def __repr__(self):
		return self.__string
	def __str__(self):
		return self.__string
	def __call__(self, path, root):
		raise NotImplementedError

class PathFilter(Filter):
	__slots__ = 'terms',
	def __init__(self, string):
		self.terms = scrub_terms(string, lambda term: term.replace('/', os.path.sep))
		Filter.__init__(self, 'path=%s' % ' '.join(self.terms))
	def __call__(self, path, root):
		path = path.lower()
		LOGGER.debug('PathFilter query=%s path=%s' % (self.terms, path))
		return any((term in path for term in self.terms))

class CompoundFilter(Filter):
	__slots__ = 'subfilters',
	def __init__(self, subfilters):
		if not subfilters or not all((callable(sf) for sf in subfilters)) or any((isinstance(sf, CompoundFilter) for sf in subfilters)):
			raise ValueError(subfilters)
		self.subfilters = subfilters
		strings = sorted({str(sf) for sf in self.subfilters})
		Filter.__init__(self, '\t'.join(strings))
	def __call__(self, path, root):
		return all((sf(path, root) for sf in self.subfilters))

class ContentFilter(Filter):
	__slots__ = 'terms',
	def __init__(self, string):
		self.terms = scrub_terms(string)
		Filter.__init__(self, 'content=%s' % ' '.join(self.terms))
	def __call__(self, path, root):
		LOGGER.debug('PathFilter query=%s path=%s' % (self.terms, path))
		path = path_join(root, path)
		with filestuff.LockedFile(path) as f:
			# Don't search binary files
			try:
				info = processors.AutoBaseProcessor.auto_header(f.handle.read(2048))
			finally:
				f.handle.seek(0)
			if info.encoding is None:
				return False
			reader = codecs.getreader(info.encoding)(f.handle)
			line = reader.readline()
			found = set()
			while line:
				found.update((term for term in self.terms if term in line))
				if len(found) == len(self.terms):
					return True
				line = reader.readline()
			return False




class BaseSearchCache(object):
	__slots__ = '__db', '__sorted_scan', '__latest_mtime_callback', '__options', '__lock', '__length',
	@staticmethod
	def utcnow():
		return datetime.utcnow().replace(tzinfo = utc)
	@staticmethod
	def get_db():
		raise NotImplementedError
	def __init__(self, db, sorted_scan, latest_mtime_callback, max_age = None, max_entries = None, auto_scrub = False):
		self.__db = db
		self.__length = sum((1 for key in self.__db if not key.startswith('=date:')))
		self.__sorted_scan = sorted_scan
		self.__latest_mtime_callback = latest_mtime_callback

		# Store options
		if max_age is not None and not isinstance(max_age, timedelta):
			max_age = timedelta(seconds = max_age)
		if max_entries is not None:
			max_entries = int(max_entries)
			if max_entries < 2:
				raise ValueError('Invalid number of maximum entries: %d' % max_entries)
		auto_scrub = bool(auto_scrub)
		self.__options = cache.Cache.Options(max_age, max_entries, auto_scrub)
		self.scrub()
	def __len__(self):
		with self:
			return self.__length
	def __del__(self):
		self.close()
	def __enter__(self):
		raise NotImplementedError
	def __exit__(self, type, value, tb):
		raise NotImplementedError
	def close(self):
		raise NotImplementedError
	def __call__(self, search_filter):
		if not isinstance(search_filter, Filter):
			raise ValueError(search_filter)
		if self.options.auto_scrub and self.options.max_entries is not None:
			LOGGER.debug('Scheduling a scrub because max_entries=%s and auto_scrub=True' % self.options.max_entries)
			self.schedule_scrub(True)


		str_filter = str(search_filter)
		LOGGER.debug('Cached search using %s' % str_filter)
		date_key = '=date:' + str_filter
		mtime = self.__latest_mtime_callback(False)
		updating = True
		with self:
			try:
				entry_timestamp = self.__db[date_key]
				if mtime is None or entry_timestamp < mtime:
					# If mtime is None, then there are no files left
					raise ValueError
				self.__db[date_key] = self.utcnow()

				LOGGER.debug('Returning cached result for %s' % str_filter)
				return self.__db[str_filter]
			except KeyError:
				updating = False
			except ValueError:
				pass

		LOGGER.debug('No matches for %s; calling sorted scan' % str_filter)
		entry_content = list(self.__sorted_scan(search_filter))
		new_entry_timestamp = self.utcnow()
		with self:
			other_updated = False
			try:
				# Done this way because another thread might've updated it while DB wasn't locked.
				other_updated = (self.__db[date_key] > entry_timestamp)
			except KeyError:
				pass
			if not other_updated:
				print(entry_content)
				self.__db[date_key] = new_entry_timestamp
				self.__db[str_filter] = entry_content
				if not updating:
					# Inside of "not other_updated" because another thread might've inserted this while DB wasn't locked.
					self.__length += 1
		return entry_content
	@property
	def options(self):
		return self.__options
	def schedule_scrub(self, tentative = False):
		self.scrub(tentative)
	def __remove(self, key, date_key):
		del self.__db[key]
		del self.__db[date_key]
	def scrub(self, tentative = False):
		if tentative and self.options.max_entries is not None:
			LOGGER.debug('Performing check because tentative = True')
			with self:
				if self.__length < self.options.max_entries:
					# This is < because when tentative == True, an entry
					# may be inserted.
					return False
		mtime = self.__latest_mtime_callback(True)
		cutoff = None
		if self.options.max_age is not None:
			cutoff = datetime.utcnow().replace(tzinfo = utc) - self.options.max_age
		entries = []
		LOGGER.info('Scrubbing cache %s' % self)
		with self:
			for key in list(self.__db):
				if key.startswith('=date:'):
					continue
				date_key = '=date:' + key
				entry_timestamp = self.__db[date_key]
				if mtime is None or entry_timestamp < mtime:
					# If mtime is none, then there are no files left
					self.__remove(key, date_key)
					continue
				elif cutoff is not None and entry_timestamp < cutoff:
					self.__remove(key, date_key)
					continue
				entries.append((key, date_key, entry_timestamp))
			ecount = len(entries)
			if self.options.max_entries is not None and ecount > self.options.max_entries:
				entries.sort(key = lambda x: x[2])
				for key, date_key, entry_timestamp in itertools.islice(entries, ecount - self.options.max_entries):
					self.__remove(key, date_key)
					ecount -= 1
			self.__length = ecount
		return True

class SearchCache(BaseSearchCache):
	allbits = stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO
	file_perms = stat.S_IRUSR|stat.S_IWUSR

	__slots__ = '__lock',
	def __init__(self, dbfile, sorted_scan, latest_mtime_callback, max_age = None, max_entries = None, auto_scrub = None):
		lockfile = path_join(dirname(dbfile), '.lock-' + basename(dbfile))
		self.__lock = cache.FileLock(lockfile, cache.FileLock.EXCLUSIVE)
		with open(lockfile, 'wb') as f:
			common.fix_perms(f)
		BaseSearchCache.__init__(self, shelve.open(dbfile, 'c', pickle.HIGHEST_PROTOCOL), \
				sorted_scan, latest_mtime_callback, max_age, max_entries, auto_scrub)
		common.fix_perms(dbfile)
	def close(self):
		if self._BaseSearchCache__db is not None:
			self._BaseSearchCache__db.close()
			self._BaseSearchCache__db = None
	def __enter__(self):
		self.__lock.acquire()
	def __exit__(self, type, value, tb):
		self.__lock.release()

class TemporarySearchCache(BaseSearchCache):
	__slots__ = '__lock',
	def __init__(self, sorted_scan, latest_mtime_callback, max_age = None, max_entries = None, auto_scrub = None):
		self.__lock = Semaphore()
		BaseSearchCache.__init__(self, {}, sorted_scan, latest_mtime_callback, max_age, max_entries, auto_scrub)
	def close(self):
		if self._BaseSearchCache__db is not None:
			self._BaseSearchCache__db = None
	def __enter__(self):
		self.__lock.acquire()
	def __exit__(self, type, value, tb):
		self.__lock.release()





FileInfo = namedtuple('FileInfo', ['name', 'modified', 'size'])

class Search(object):
	__slots__ = 'server', '__cache',
	@staticmethod
	def find_files(root):
		for path, dnames, fnames in os.walk(root):
			filtered_dnames = [d for d in dnames if not d.startswith('.')]
			del dnames[:]
			dnames.extend(filtered_dnames)
			for fname in fnames:
				if not fname.startswith('.'):
					yield path_join(path, fname)
	def __init__(self, server, cache_path = None, max_age = None, max_entries = None, auto_scrub = False):
		self.server = server
		self.__cache = None
		if cache_path is not None:
			self.__cache = SearchCache(cache_path, \
					(lambda filter_func: sorted(self.filter_files(filter_func), key = lambda x: x.name)),
					self.get_latest_mtime,
					max_age, max_entries, auto_scrub)
	def __del__(self):
		self.close()
	def close(self):
		if self.__cache is not None:
			self.__cache.close()
			self.__cache = None
		if self.server is not None:
			self.server = None
	def scrub(self):
		if self.__cache is not None:
			self.__cache.schedule_scrub()
			return True
		else:
			return False
	@staticmethod
	def itertree(root):
		for path, dnames, fnames in os.walk(root):
			yield path, True
			for fname in fnames:
				yield path_join(path, fname), False
	def get_latest_mtime(self, refresh = False):
		# Will only be called if caching is enabled
		latest_mtime = self.server.getvar('LATEST_MTIME')
		if not refresh and latest_mtime is None:
			return latest_mtime
		for path, path_isdir in self.itertree(self.server.cache.source_root):
			try:
				info = os.stat(path)
			except OSError:
				continue
			modified = datetime.utcfromtimestamp(info.st_mtime).replace(tzinfo = utc)
			if latest_mtime is None or modified > latest_mtime:
				latest_mtime = modified
		self.server.setvar('LATEST_MTIME', latest_mtime)
		return latest_mtime
	def filter_files(self, filter_func):
		latest_mtime = self.server.getvar('LATEST_MTIME')
		root = self.server.cache.source_root
		try:
			for path, path_isdir in self.itertree(root):
				try:
					info = os.stat(path)
				except OSError:
					continue
				modified = datetime.utcfromtimestamp(info.st_mtime).replace(tzinfo = utc)
				if latest_mtime is None or modified > latest_mtime:
					latest_mtime = modified
				if path_isdir:
					continue

				if filter_func(relpath(path, root), root):
					try:
						with filestuff.LockedFile(path) as f:
							info = FileInfo(relpath(path, root), f.modified, f.size)
						# Refresh this, just in case
						if latest_mtime is None or modified > latest_mtime:
							latest_mtime = modified
						if any((x is None for x in info._asdict())):
							raise RuntimeError(info)
						yield info
					except OSError:
						pass
		finally:
			self.server.setvar('LATEST_MTIME', latest_mtime)
	def find_by_path(self, start, end, filter_func = None):
		found = []
		if filter_func is None or self.__cache is None:
			LOGGER.debug('Going around cache because filter_func=%s or cache=%s' % (filter_func, self.__cache))
			if filter_func is None:
				filter_func = lambda path, root: True
			all_found = sorted(self.filter_files(filter_func), key = lambda x: x.name)
		else:
			all_found = self.__cache(filter_func)
		found = list(itertools.islice(all_found, start, end, 1))
		return found, (start > 0), (end < len(all_found) - 1)





if __name__ == '__main__':
	import unittest
	from time import sleep
	logging.basicConfig(level = logging.DEBUG)
	class FilterTest(unittest.TestCase):
		def test_scrub(self):
			self.assertEqual(scrub_terms('\nfoo bar\t'), ('bar', 'foo'))
		def test_path(self):
			func = PathFilter('foo bAr')
			self.assertTrue(func('./foo/bar', None))
			self.assertTrue(func('Bar/foo', None))
			self.assertTrue(func('fOo', None))
			self.assertTrue(func('baR', None))
	
	class BaseSearchCacheTest(unittest.TestCase):
		FILES = [
			'foo',
			'bar',
			'baz',
			'x/y/z',
			'x/y/a',
			'x/a/z',
			'1/4/6/12',
		]
		def sorted_scan(self, filter_func):
			self.count += 1
			return sorted((f for f in self.FILES if filter_func(f, None)))
		def get_mtime(self, freshen):
			return self.mtime
		def get_cache(self):
			raise NotImplementedError
		def setUp(self):
			self.mtime = TemporarySearchCache.utcnow()
			self.cache = self.get_cache()
			self.count = 0
		def tearDown(self):
			self.cache.close()
	class SearchCacheTest(BaseSearchCacheTest):
		def get_cache(self):
			return TemporarySearchCache(self.sorted_scan, self.get_mtime)
		def test_miss(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
		def test_miss_2(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			func = PathFilter('/')
			results = self.cache(func)
			self.assertEqual(results, ['1/4/6/12', 'x/a/z', 'x/y/a', 'x/y/z'])
			self.assertEqual(self.count, 2)
			self.assertEqual(len(self.cache), 2)
		def test_miss_hit(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)
		def test_miss_hit_invalid_mtime(self):
			self.mtime = None
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
			self.assertEqual(len(self.cache), 1)

			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 2)
			self.assertEqual(len(self.cache), 1)
		def test_new_mtime(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)

			sleep(0.5)
			self.mtime = TemporarySearchCache.utcnow()

			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 2)
		def test_new_mtime_scrub(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)

			sleep(0.5)
			self.mtime = TemporarySearchCache.utcnow()
			self.cache.scrub()

			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 2)
		def test_new_invalid_mtime_scrub(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)

			self.mtime = None
			self.cache.scrub()

			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 2)
	class ExpiringSearchCacheTest(BaseSearchCacheTest):
		def get_cache(self):
			return TemporarySearchCache(self.sorted_scan, self.get_mtime, max_age = 1)
		def test_expire(self):
			func = PathFilter('a')
			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)
			sleep(2)

			self.cache.scrub()

			results = self.cache(func)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 2)
	class LRUCacheTest(BaseSearchCacheTest):
		def get_cache(self):
			return TemporarySearchCache(self.sorted_scan, self.get_mtime, max_entries = 2)
		def test_expire(self):
			func1 = PathFilter('a')
			results = self.cache(func1)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)

			func2 = PathFilter('b')
			results = self.cache(func2)
			self.assertEqual(results, ['bar', 'baz'])
			self.assertEqual(self.count, 2)

			func3 = PathFilter('c')
			results = self.cache(func3)
			self.assertEqual(results, [])
			self.assertEqual(self.count, 3)

			self.cache.scrub()
			results = self.cache(func1)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 4)
	class AutoLRUCacheTest(BaseSearchCacheTest):
		def get_cache(self):
			return TemporarySearchCache(self.sorted_scan, self.get_mtime, max_entries = 2, auto_scrub = True)
		def test_expire(self):
			func1 = PathFilter('a')
			results = self.cache(func1)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 1)

			func2 = PathFilter('b')
			results = self.cache(func2)
			self.assertEqual(results, ['bar', 'baz'])
			self.assertEqual(self.count, 2)

			func3 = PathFilter('c')
			results = self.cache(func3)
			self.assertEqual(results, [])
			self.assertEqual(self.count, 3)

			results = self.cache(func1)
			self.assertEqual(results, ['bar', 'baz', 'x/a/z', 'x/y/a'])
			self.assertEqual(self.count, 4)
	unittest.main()
