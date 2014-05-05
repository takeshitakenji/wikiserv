#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import logging, os, codecs, shelve, pickle, os.path
import config, cache, processors, filestuff
from datetime import datetime, timedelta
from pytz import utc
import itertools, functools
from os.path import relpath, basename, join as path_join
from collections import namedtuple
from threading import Semaphore

LOGGER = logging.getLogger('wikiserv')


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


class SearchCache(object):
	__slots__ = '__db', '__sorted_scan', '__latest_mtime_callback', '__options', '__lock',
	@staticmethod
	def utcnow():
		return datetime.utcnow().replace(tzinfo = utc)
	def __init__(self, dbfile, sorted_scan, latest_mtime_callback, max_age = None, max_entries = None, auto_scrub = False):
		# sorted_scan(search_filter) should return (latest_mtime, sorted_list)
		if callable(dbfile):
			self.__db = dbfile()
		else:
			self.__db = shelve.open(dbfile, 'c', protocol = pickle.HIGHEST_PROTOCOL)
		self.__sorted_scan = sorted_scan
		self.__latest_mtime_callback = latest_mtime_callback
		self.__lock = Semaphore()

		# Store options
		if max_age is not None and not isinstance(max_age, timedelta):
			max_age = timedelta(seconds = max_age)
		if max_entries is not None:
			max_entries = int(max_entries)
			if max_entries < 2:
				raise ValueError('Invalid number of maximum entries: %d' % max_entries)
		auto_scrub = bool(auto_scrub)
		self.__options = cache.Cache.Options(max_age, max_entries, auto_scrub)
	def __len__(self):
		with self.__lock:
			return sum((1 for key in self.__db if not key.startswith('=date:')))
	def __del__(self):
		self.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, tb):
		self.close()
	def close(self):
		if self.__db is not None:
			try:
				self.__db.close()
			except AttributeError:
				pass
			self.__db = None
	def __call__(self, search_filter):
		if not isinstance(search_filter, Filter):
			raise ValueError(search_filter)
		# NOTE: latest_mtime_callback should be called to check mtime before going through with a scan.
		# Once search results have been computed, get utcnow().replace(tzinfo = utc)
		do_scrub = False
		# Scrub at some point!


		str_filter = str(search_filter)
		LOGGER.debug('Cached search using %s' % str_filter)
		date_key = '=date:' + str_filter
		mtime = self.__latest_mtime_callback()
		with self.__lock:
			try:
				entry_timestamp = self.__db[date_key]
				if entry_timestamp < mtime:
					raise ValueError
				self.__db[date_key] = self.utcnow()

				return self.__db[str_filter]
			except (KeyError, ValueError):
				pass

		LOGGER.debug('No matches for %s; calling sorted scan' % str_filter)
		entry_content = list(self.__sorted_scan(search_filter))
		entry_timestamp = self.utcnow()
		with self.__lock:
			self.__db[date_key] = entry_timestamp
			self.__db[str_filter] = entry_content
		return entry_content
	def schedule_scrub(self, tentative = False):
		self.scrub(tentantive)
	def scrub(self, tentative = False):
		# Same features as cache.Cache
		raise NotImplementedError

class Search(object):
	Info = namedtuple('Info', ['name', 'modified', 'size'])
	__slots__ = 'server',
	@staticmethod
	def find_files(root):
		for path, dnames, fnames in os.walk(root):
			filtered_dnames = [d for d in dnames if not d.startswith('.')]
			del dnames[:]
			dnames.extend(filtered_dnames)
			for fname in fnames:
				if not fname.startswith('.'):
					yield path_join(path, fname)
	def __init__(self, server):
		self.server = server
	def __del__(self):
		self.close()
	def close(self):
		self.server = None
	@classmethod
	def get_info(cls, path, root):
		with filestuff.LockedFile(path) as f:
			return cls.Info(relpath(path, root), f.modified, f.size)
	def find_by_path(self, start, end, filter_func = None):
		if filter_func is None:
			filter_func = lambda path, root: True
		root = self.server.cache.source_root
		find_files = [path for path in sorted(cache.Cache.find_files(root)) if filter_func(relpath(path, root), root)]
		# Cache [filter] = find_files, newest_mtime
		found = []
		for path in itertools.islice(find_files, start, end, 1):
			try:
				with filestuff.LockedFile(path) as f:
					found.append(self.Info(relpath(path, root), f.modified, f.size))
			except OSError:
				pass
		return found, (start > 0), (end < len(find_files) - 1)





if __name__ == '__main__':
	import unittest
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
	class SearchCacheTest(unittest.TestCase):
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
		def setUp(self):
			self.mtime = datetime.utcnow().replace(tzinfo = utc)
			self.cache = SearchCache(dict, self.sorted_scan, lambda: self.mtime)
			self.count = 0
		def tearDown(self):
			self.cache.close()
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
	unittest.main()
