#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import logging, os
import config, cache, processors, filestuff
from pytz import utc
import itertools, functools
from os.path import relpath, basename
from collections import namedtuple

LOGGER = logging.getLogger('wikiserv')


class Filter(object):
	__slots__ = '__string',
	def __init__(self, string):
		self.__string = string
	def __repr__(self):
		return self.__string
	def __str__(self):
		return self.__string
	def __call__(self, path):
		raise NotImplementedError

class PathFilter(Filter):
	__slots__ = 'terms',
	def __init__(self, string):
		if not string:
			raise ValueError(string)
		terms = (x.strip().lower() for x in string.split())
		# Clean up the mess
		self.terms = tuple(sorted(set((x for x in terms if x))))
		if not self.terms:
			raise ValueError(string)
		Filter.__init__(self, 'path=%s' % ' '.join(self.terms))
	def __call__(self, path):
		path = path.lower()
		LOGGER.debug('PathFilter query=%s path=%s' % (self.terms, path))
		return any((term in path for term in self.terms))

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
			filter_func = lambda path: True
		root = self.server.cache.source_root
		find_files = [path for path in sorted(cache.Cache.find_files(root)) if filter_func(relpath(path, root))]

		# Cache [filter] = find_files, newest_mtime
		found = []
		for path in itertools.islice(find_files, start, end, 1):
			try:
				with filestuff.LockedFile(path) as f:
					found.append(self.Info(relpath(path, root), f.modified, f.size))
			except OSError:
				pass
		return found, (start > 0), (end < len(find_files) - 1)
