#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')
import hashlib, functools
from zlib import adler32, crc32
import struct
import logging


LOGGER = logging.getLogger(__name__)

STEP = 4096

class Adler32(object):
	name = 'adler32'
	__slots__ = '__checksum',
	def __init__(self):
		self.__checksum = None
	def update(self, data):
		self.__checksum = adler32(data, self.__checksum) if self.__checksum is not None else adler32(data)
	def digest(self):
		return struct.pack('!I', self.__checksum)
		
class CRC32(object):
	name = 'crc32'
	__slots__ = '__checksum',
	def __init__(self):
		self.__checksum = None
	def update(self, data):
		self.__checksum = crc32(data, self.__checksum) if self.__checksum is not None else crc32(data)
	def digest(self):
		return struct.pack('!I', self.__checksum)



ALGORITHMS = {name.lower() : functools.partial(hashlib.new, name) for name in hashlib.algorithms_available}
ALGORITHMS[Adler32.name] = Adler32
ALGORITHMS[CRC32.name] = CRC32


def available_hashers():
	global ALGORITHMS
	LOGGER.debug('Getting available hashers')
	return frozenset(ALGORITHMS.keys())
def get_hasher(name):
	global ALGORITHMS
	try:
		LOGGER.debug('Attempting to get hasher %s' % name)
		return ALGORITHMS[name]
	except KeyError:
		raise ValueError('Invalid checksum algorithm: %s' % name)

if __name__ == '__main__':
	import unittest
	import timeit
	class HashTest(unittest.TestCase):
		TEST_DATA = b'TEST' * 4096
		def test_available(self):
			self.assertGreater(len(available_hashers()), 0)
		def test_invalid(self):
			self.assertRaises(ValueError, get_hasher, None)
		def test_get(self):
			for algorithm in available_hashers():
				hasher = get_hasher(algorithm)
				self.assertIsNotNone(hasher)
		def test_init(self):
			seen = set()
			for algorithm in available_hashers():
				hasher = get_hasher(algorithm)()
				self.assertNotIn(hasher.name, seen)
				seen.add(hasher.name)
		def test_process(self):
			for algorithm in available_hashers():
				digest = None
				hasher = get_hasher(algorithm)()
				hasher.update(self.TEST_DATA)
				digest = hasher.digest()
				self.assertGreater(len(digest), 0)
				print('%s => %s' % (algorithm, digest))


	unittest.main()
