#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import logging, os, codecs, shelve, pickle
import config, cache, processors, filestuff
from pytz import utc
import itertools, functools
from os.path import relpath, basename, join as path_join
from collections import namedtuple

LOGGER = logging.getLogger('wikiserv')


def scrub_terms(string):
	if not string:
		raise ValueError(string)
	terms = (x.strip().lower() for x in string.split())
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
		self.terms = scrub_terms(string)
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
	__slots__ = '__db', '__miss_method',
	def __init__(self, dbfile, miss_method):
		self.__db = shelve.open(dbfile, 'c', protocol = pickle.HIGHEST_PROTOCOL)
		self.__miss_method = miss_method
	def __del__(self):
		self.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, tb):
		self.close()
	def close(self):
		if self.__db is not None:
			self.__db.close()
			self.__db = None
	def __getitem__(self, search_filter):
		if not isinstance(search_filter, Filter):
			raise ValueError(search_filter)
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
