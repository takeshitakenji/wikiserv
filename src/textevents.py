#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')
import logging


LOGGER = logging.getLogger(__name__)

class TextEventSource(object):
	__slots__ = '__writer_method', '__tee_output', '__finish_output', '__reader', '__reader_lock',
	# NOTE: writer_method(self) is the real executor
	def __init__(self, writer_method, tee_output = None):
		raise NotImplementedError
	def write(self, s):
		raise NotImplementedError
	def set_read(self, length, callback):
		raise NotImplementedError
	def set_finish(self, finish_output = None):
		raise NotImplementedError
	def execute(self):
		return self.__writer_method(self)

if __name__ == '__main__':
	import unittest
	unittest.main()
