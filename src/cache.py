#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import struct
from datetime import datetime, timedelta
from pytz import utc
import filestuff



def timestamps_equivalent(t1, t2, tolerance = 0.001):
	if isinstance(tolerance, timedelta):
		tolerance = abs(tolerance.total_seconds())
	return abs((t1 - t2).total_seconds()) <= tolerance

class EntryHeader(object):
	__slots__ = 'size', 'timestamp', 'checksum'
	minsize = len(struct.pack('!LdH', 0, 0, 0))
	struct_fmt = '!LdH'
	def __init__(self, size, timestamp, checksum):
		self.size, self.timestamp, self.checksum = size, timestamp, checksum
	def write(self, stream):
		count = stream.write(struct.pack(self.struct_fmt, self.size, self.timestamp.timestamp(), len(self.checksum)))
		count += stream.write(self.checksum)
		return count
	@classmethod
	def read(cls, stream):
		buff = stream.read(cls.minsize)
		size, timestamp, cksum_len = struct.unpack(cls.struct_fmt, buff)
		timestamp = datetime.utcfromtimestamp(timestamp).replace(tzinfo = utc)
		checksum = None
		if cksum_len > 0:
			checksum = stream.read(cksum_len)
		if len(checksum) < cksum_len:
			raise ValueError('Invalid checksum length')
		return cls(size, timestamp, checksum)


if __name__ == '__main__':
	import unittest
	from os import remove, stat
	from tempfile import TemporaryDirectory, NamedTemporaryFile
	from hashlib import md5
	from codecs import getreader, getwriter

	def hashstring(s, cksum_type):
		hasher = cksum_type()
		hasher.update(s)
		return hasher.digest()
	

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
			self.assertTrue(timestamps_equivalent(self.timestamp, test2.timestamp))
			self.assertEqual(self.FILE_CHECKSUM, test2.checksum)

	unittest.main()
