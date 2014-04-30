#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import os.path
from os.path import normpath
import fcntl
from datetime import datetime
from os import fstat
from pytz import utc

class _BaseFile(object):
	@property
	def name(self):
		raise NotImplementedError
	def fileno(self):
		raise NotImplementedError
	@property
	def modified(self):
		raise NotImplementedError
	@property
	def size(self):
		raise NotImplementedError
	def checksum(self, cksum_type):
		raise NotImplementedError
	@property
	def handle(self):
		raise NotImplementedError

class BaseFile(object):
	def __enter__(self):
		raise NotImplementedError
	def __exit__(self, type, value, tb):
		raise NotImplementedError

class _File(object):
	__slots__ = '__fd',
	BLOCKSIZE = 4096
	def __init__(self, fd):
		self.__fd = fd
	@property
	def name(self):
		return self.__fd.name
	def fileno(self):
		return self.__fd.fileno()
	@property
	def modified(self):
		info = fstat(self.__fd.fileno())
		return datetime.utcfromtimestamp(info.st_mtime).replace(tzinfo = utc)
	@property
	def size(self):
		info = fstat(self.__fd.fileno())
		return info.st_size
	def checksum(self, cksum_type):
		hasher = cksum_type()
		self.__fd.seek(0)
		try:
			data = self.__fd.read(self.BLOCKSIZE)
			while data:
				hasher.update(data)
				data = self.__fd.read(self.BLOCKSIZE)
			return hasher.digest()
		finally:
			self.__fd.seek(0)
	@property
	def handle(self):
		return self.__fd

class File(BaseFile):
	__slots__ = 'path','fd',
	def __init__(self, path):
		BaseFile.__init__(self)
		path = normpath(path)
		if any((part.startswith('.') for part in path.split(os.path.sep))):
			raise ValueError('Path entries cannot start with "."')
		self.path = path
		self.fd = None
	def __enter__(self):
		self.fd = open(self.path, 'rb')
		return _File(self.fd)
	def __exit__(self, type, value, tb):
		self.fd.close()
		self.fd = None

class LockedFile(File):
	def __enter__(self):
		self.fd = open(self.path, 'rb')
		fcntl.lockf(self.fd, fcntl.LOCK_SH)
		return _File(self.fd)
	def __exit__(self, type, value, tb):
		self.fd.flush()
		fcntl.lockf(self.fd, fcntl.LOCK_UN)
		self.fd.close()
		self.fd = None

class ExclusivelyLockedFile(File):
	def __enter__(self):
		self.fd = open(self.path, 'r+b')
		fcntl.lockf(self.fd, fcntl.LOCK_EX)
		return _File(self.fd)
	def __exit__(self, type, value, tb):
		self.fd.flush()
		fcntl.lockf(self.fd, fcntl.LOCK_UN)
		self.fd.close()
		self.fd = None

if __name__ == '__main__':
	import unittest
	from os import remove, stat
	from tempfile import NamedTemporaryFile
	from hashlib import md5
	from dateutil.tz import tzlocal

	localtz = tzlocal()

	def hashstring(s, cksum_type):
		hasher = cksum_type()
		hasher.update(s)
		return hasher.digest()

	class FileTests(unittest.TestCase):
		FILE_TEXT = 'TEST FILE\n'.encode('ascii')
		FILE_CHECKSUM = hashstring(FILE_TEXT, md5)
		def setUp(self):
			with NamedTemporaryFile(delete = False) as tmp:
				self.path = tmp.name
				tmp.write(self.FILE_TEXT)
			self.mtime = datetime.fromtimestamp(stat(self.path).st_mtime).replace(tzinfo = localtz).astimezone(utc)
		def tearDown(self):
			remove(self.path)
		def test_file_basic(self):
			f = File(self.path)
			self.assertEqual(f.path, self.path)
		def test_file_info(self):
			with File(self.path) as info:
				self.assertEqual(info.size, len(self.FILE_TEXT))
				self.assertEqual(info.modified, self.mtime)
				self.assertEqual(info.checksum(md5), self.FILE_CHECKSUM)
		def test_file_info_read(self):
			with File(self.path) as info:
				self.assertEqual(info.checksum(md5), self.FILE_CHECKSUM)
				self.assertEqual(info.handle.read(), self.FILE_TEXT)
				self.assertEqual(info.checksum(md5), self.FILE_CHECKSUM)
		def test_file_lock_info(self):
			with LockedFile(self.path) as info:
				self.assertEqual(info.size, len(self.FILE_TEXT))
				self.assertEqual(info.modified, self.mtime)
				self.assertEqual(info.checksum(md5), self.FILE_CHECKSUM)

			with ExclusivelyLockedFile(self.path) as info:
				self.assertEqual(info.size, len(self.FILE_TEXT))
				self.assertEqual(info.modified, self.mtime)
				self.assertEqual(info.checksum(md5), self.FILE_CHECKSUM)
	unittest.main()
