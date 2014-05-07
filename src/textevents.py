#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')
import logging, functools
from collections import namedtuple
from threading import Lock


LOGGER = logging.getLogger(__name__)

class TextEventSource(object):
	STRING_TYPE = NotImplemented
	Callback = namedtuple('Callback', ['method', 'args', 'kwargs'])
	__slots__ = '__acumulator', '__accum_len', '__tee_output', '__finishing', '__finished', '__finish_output', '__lock', '__callback',
	@staticmethod
	def send_to_callback(value, callback):
		if isinstance(callback, self.Callback):
			return callback.method(value, *callback.args, **callback.kwargs)
		elif hasattr(callback, 'write'):
			return callback.write(value)
		else:
			return callback(value)
	def __init__(self, tee_output = None):
		self.check_string_type()
		self.__accumaltor, self.__accum_len = [], 0
		self.__tee_output = tee_output
		self.__finishing, self.__finish_output = None, None
		self.__finished = Event()
		self.__callback = None
		self.__lock = Lock()
	@classmethod
	def check_string_type(cls):
		if self.STRING_TYPE is NotImplemented:
			raise NotImplementedError
	@classmethod
	def blank_string_value(cls):
		cls.check_string_type()
		return self.STRING_TYPE()
	def write(self, s):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		if not isinstance(s, self.STRING_TYPE):
			raise IOError('%s doesn\'t support %s objects' % (self, type(s)))
		raise NotImplementedError
	def set_read(self, length, callback, *args, **kwargs):
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		raise NotImplementedError
	def set_finish(self, finish_output = None):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		raise NotImplementedError
	def execute_method(self, method, *method_args, **method_kwargs):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		return method(self, *method_args, **method_kwargs)

if __name__ == '__main__':
	import unittest
	unittest.main()
