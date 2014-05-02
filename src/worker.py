#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')


from queue import Queue
from threading import Thread, Condition, Lock
from traceback import print_exception, extract_stack, format_list
import logging


LOGGER = logging.getLogger(__name__)

class Finished(Exception):
	@classmethod
	def finish(cls):
		raise cls


class Job(object):
	__slots__ = '__func', '__args', '__kwargs', '__completed', '__result', '__exception', '__lock', '__cond', '__creation_stack'
	def __init__(self, func, *args, **kwargs):
		if not callable(func):
			raise ValueError(func)
		self.__lock = Lock()
		self.__cond = Condition(self.__lock)
		self.__func, self.__args, self.__kwargs = func, args, kwargs
		self.__creation_stack = tuple(extract_stack())
		self.__completed, self.__result, self.__exception = False, None, None
	def __abbrev_info(self):
		return 'Job %x: %s(*%s, **%s)' % (id(self), self.__func, repr(self.__args), repr(self.__kwargs))
	def __full_info(self):
		out = [self.__abbrev_info(), '\n']
		out.extend(format_list(self.__creation_stack))
		return ''.join(out)
	def __str__(self):
		with self.__lock:
			return self.__full_info()
	def __repr__(self):
		with self.__lock:
			return self.__abbrev_info()
	@property
	def stack(self):
		with self.__lock:
			return self.__creation_stack
	def complete(self, result):
		with self.__lock:
			self.__completed = True
			self.__result = result
			self.__cond.notify_all()
			LOGGER.debug('Job %s is complete' % self.__abbrev_info())
	def complete_exception(self, exception):
		with self.__lock:
			self.__completed = True
			self.__exception = exception
			self.__cond.notify_all()
			LOGGER.debug('Job %s is complete' % self.__abbrev_info())
	def __call__(self):
		LOGGER.debug('Job %s is being executed' % self.__abbrev_info())
		return self.__func(*self.__args, **self.__kwargs)
	@property
	def result(self):
		with self.__lock:
			if not self.__completed:
				raise RuntimeError('Job %s is not complete' % self)
			elif self.__exception is not None:
				raise self.__exception
			else:
				return self.__result
	def wait(self, timeout = None):
		with self.__lock:
			if not self.__completed:
				self.__cond.wait(timeout)
			if not self.__completed:
				raise RuntimeError('Job %s is not complete' % self)
			elif self.__exception is not None:
				raise self.__exception
			else:
				return self.__result

class Worker(Thread):
	__slots__ = '__queue',
	def __init__(self, queue = None, autostart = False):
		Thread.__init__(self)
		self.__queue = queue if queue is not None else Queue()
		if autostart:
			self.start()
	def run(self):
		LOGGER.debug('Thread %s has started' % self)
		try:
			while True:
				job = self.__queue.get()
				try:
					job.complete(job())
				except Finished:
					job.complete(None)
					break
				except BaseException as e:
					LOGGER.exception('Job %s in thread %s:' % (job, self))
					print_exception(type(e), e, None, file = sys.stderr)
					job.complete_exception(e)
				finally:
					self.__queue.task_done()
		finally:
			LOGGER.debug('Thread %s has finished' % self)
	def __call__(self, func, *args, **kwargs):
		return self.schedule_sync(func, *args, **kwargs)
	def schedule(self, func, *args, **kwargs):
		job = Job(func, *args, **kwargs)
		self.__queue.put(job)
		return job
	def schedule_sync(self, func, *args, **kwargs):
		return self.schedule(func, *args, **kwargs).wait()
	def schedule_sync_timeout(self, timeout, func, *args, **kwargs):
		return self.schedule(func, *args, **kwargs).wait(timeout)
	def finish(self, wait = False, timeout = None):
		if wait:
			if timeout is not None:
				return self.schedule_sync(Finished.finish)
			else:
				return self.schedule_sync_timeout(timeout, Finished.finish)
		else:
			return self.schedule(Finished.finish)



if __name__ == '__main__':
	import unittest
	logging.basicConfig(level = logging.DEBUG)

	class InitTest(unittest.TestCase):
		def test_init(self):
			thread = Worker(autostart = True)
			thread.finish()
			thread.join()

			thread = Worker()
			thread.start()
			thread.finish(True)
			thread.join()
	class WorkerTest(unittest.TestCase):
		def process(self, incr = 1):
			self.count += incr
			return self.count
		def rexc(self, etype):
			raise etype()
		def setUp(self):
			self.thread = Worker(autostart = True)
			self.count = 0
		def tearDown(self):
			self.thread.finish(True)
			self.thread.join()
		def test_sync(self):
			self.assertEqual(self.thread(self.process, 2), 2)
			self.assertEqual(self.count, 2)
		def test_default(self):
			self.assertEqual(self.thread(self.process), 1)
			self.assertEqual(self.count, 1)
		def test_async(self):
			job = self.thread.schedule(self.process, 1)
			self.assertIsNotNone(job)
			self.assertEqual(job.wait(), 1)
			self.assertEqual(self.count, 1)
		def test_many(self):
			toadd = list(range(100))
			total = sum(toadd)
			jobs = []
			for i in toadd:
				jobs.append(self.thread.schedule(self.process, i))
			self.assertTrue(all(jobs))
			self.assertEqual(jobs[-1].wait(), total)
			self.assertEqual(self.count, total)
		def test_exc(self):
			job = self.thread.schedule(self.rexc, ValueError)
			self.assertRaises(ValueError, job.wait)
	unittest.main()
