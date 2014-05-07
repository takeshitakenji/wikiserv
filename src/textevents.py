#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')
import logging, functools
from collections import namedtuple
from threading import Lock, Event


LOGGER = logging.getLogger(__name__)

class BaseTextEventSource(object):
	STRING_TYPE = NotImplemented
	Callback = namedtuple('Callback', ['length', 'method', 'args', 'kwargs'])
	__slots__ = '__accumulator', '__accum_len', '__tee_output', '__finishing', '__finished', '__finish_output', '__lock', '__callback',
	def send_to_callback(self, value, callback):
		if isinstance(callback, self.Callback):
			return callback.method(self, value, *callback.args, **callback.kwargs)
		elif hasattr(callback, 'write'):
			return callback.write(value)
		else:
			return callback(value)
	def __init__(self, tee_output = None):
		self.check_string_type()
		self.__accumulator, self.__accum_len = [], 0
		self.__tee_output = tee_output
		self.__finishing, self.__finish_output = None, None
		self.__finished = Event()
		self.__callback = None
		self.__lock = Lock()
	def __del__(self):
		self.close()
	@classmethod
	def check_string_type(cls):
		if cls.STRING_TYPE is NotImplemented:
			raise NotImplementedError
	@classmethod
	def blank_string_value(cls):
		cls.check_string_type()
		return self.STRING_TYPE()
	@classmethod
	def string_join(cls, parts):
		cls.check_string_type()
		return cls.STRING_TYPE().join(parts)
	def __put_value(self, value, tee_output, finish_output, callback_output):
		if tee_output is not None:
			self.send_to_callback(value, tee_output)
		if finish_output is not None:
			self.send_to_callback(value, finish_output)
		elif callback_output is not None:
			self.send_to_callback(value, callback_output)
	def __not_accumulating(self, s):
		LOGGER.warning('%s: Not accumulating string of length %d due to there being no callback' % (self, len(s)))
		if self.__accumulator:
			del self.__accumulator[:]
			self.__accum_len = 0
	def write(self, s):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		if not isinstance(s, self.STRING_TYPE):
			raise IOError('%s doesn\'t support %s objects' % (self, type(s)))
		if not s:
			return 0
		do_release = True
		try:
			self.__lock.acquire()
			tee_output = self.__tee_output
			if self.__finishing:
				if self.__accumulator:
					del self.__accumulator[:]
					self.__accum_len = 0
				finish_output = self.__finish_output
				# We won't be entering back into locked code here
				self.__lock.release()
				do_release = False
				self.__put_value(s, tee_output, finish_output, None)
			elif self.__callback is None:
				self.__not_accumulating(s)
			else:
				self.__accumulator.append(s)
				self.__accum_len += len(s)
				try:
					while not self.__finishing and self.__callback is not None and self.__accum_len >= self.__callback.length:
						value = self.__accumulator[:]
						del self.__accumulator[:]
						if self.__accum_len > self.__callback.length:
							tosplit = value [-1]
							toremove = self.__accum_len - self.__callback.length
							value[-1] = tosplit[:-toremove]
							self.__accumulator.append(tosplit[-toremove:])
							self.__accum_len = len(self.__accumulator[-1])
						else:
							self.__accum_len = 0
						value = self.string_join(value)

						if len(value) != self.__callback.length:
							raise RuntimeError('Value %s is not of length %d' % (len(value), self.__callback.length))

						callback_output = self.__callback
						self.__callback = None

						try:
							self.__lock.release()
							self.__put_value(value, tee_output, None, callback_output)
						finally:
							self.__lock.acquire()
				finally:
					if self.__accumulator:
						finish_output = self.__finish_output if self.__finishing else None
						self.__lock.release()
						do_release = False
						self.__put_value(self.__accumulator[-1], tee_output, finish_output, None)

		finally:
			if do_release:
				self.__lock.release()
	def set_read(self, length, callback, *args, **kwargs):
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		if not callable(callback):
			raise ValueError('%s is not callable' % callback)
		old_callback = None
		with self.__lock:
			old_callback = self.__callback
			self.__callback = self.Callback(length, callback, args, kwargs)
		return old_callback
	def set_finish(self, finish_output = None):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		with self.__lock:
			if self.__finishing:
				raise RuntimeError('%s is already finishing' % self)
			if self.__callback is not None:
				LOGGER.warning('%s: Removing callback %s' % (self.__callback))
				self.__callback = None
			self.__finishing = True
			if self.__finish_output is not None:
				self.__finish_output = finish_output
			elif self.__tee_output:
				LOGGER.info('%s: Dumping remaining output to nowhere')
	def close(self):
		with self.__lock:
			if not self.__finished.is_set():
				self.__finished.set()
	def wait_for_finish(self, timeout = None):
		return self.__finished.wait(timeout)
	def execute_method(self, method, *method_args, **method_kwargs):
		self.check_string_type()
		if self.__finished.is_set():
			raise IOError('%s is closed' % self)
		return method(self, *method_args, **method_kwargs)


class TextEventSource(BaseTextEventSource):
	STRING_TYPE = str

class BinaryEventSource(BaseTextEventSource):
	STRING_TYPE = bytes

if __name__ == '__main__':
	import unittest, functools

	TLOGGER = logging.getLogger('test-' + __name__)

	logging.basicConfig(level = logging.DEBUG)

	class TextEventTest(unittest.TestCase):
		def set_value(self, source, value, next_callback = None):
			TLOGGER.debug('Got value %s from %s' % (value, source))
			self.value.append(value)
			if next_callback is not None:
				source.set_read(*next_callback)
		def setUp(self):
			self.te = TextEventSource()
			self.value = []
		def tearDown(self):
			self.te.close()
		def test_single(self):
			self.te.set_read(5, self.set_value)
			self.te.write('1' * 5)
			self.assertEqual(self.value, ['1' * 5])
		def test_split(self):
			self.te.set_read(5, self.set_value, (5, self.set_value))
			self.te.write('1' * 10)
			self.assertEqual(self.value, ['1' * 5] * 2)
	unittest.main()
