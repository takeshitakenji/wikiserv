#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')


from queue import Queue
from threading import Thread, Condition, Lock, Event
import threading
from traceback import print_exception, extract_stack, format_list
import logging
import os, uuid


LOGGER = logging.getLogger(__name__)

class Finished(Exception):
	@classmethod
	def finish(cls):
		raise cls


def dump_threads():
	threads = ' '.join((str(t) for t in threading.enumerate()))
	LOGGER.debug('Current threads: %s' % threads)


class Job(object):
	__slots__ = '__func', '__args', '__kwargs', '__completed', '__result', '__exception', '__lock', '__cond', '__creation_stack', '__id'
	def __init__(self, func, *args, **kwargs):
		if not callable(func):
			raise ValueError(func)
		self.__id = str(uuid.uuid4())
		self.__lock = Lock()
		self.__cond = Condition(self.__lock)
		self.__func, self.__args, self.__kwargs = func, args, kwargs
		self.__creation_stack = tuple(extract_stack())
		self.__completed, self.__result, self.__exception = False, None, None
	def abbrev_info_unlocked(self):
		return 'Job %s: %s(*%s, **%s)' % (self.__id, self.__func, repr(self.__args), repr(self.__kwargs))
	def full_info_unlocked(self):
		out = [self.abbrev_info_unlocked(), '\n']
		out.extend(format_list(self.__creation_stack))
		return ''.join(out)
	def __str__(self):
		with self.__lock:
			return self.full_info_unlocked()
	def __repr__(self):
		with self.__lock:
			return self.abbrev_info_unlocked()
	@property
	def stack(self):
		with self.__lock:
			return self.__creation_stack
	def complete(self, result):
		with self.__lock:
			self.__completed = True
			self.__result = result
			self.__cond.notify_all()
			LOGGER.debug('Job %s is complete' % self.abbrev_info_unlocked())
	def complete_exception(self, exception):
		with self.__lock:
			self.__completed = True
			self.__exception = exception
			self.__cond.notify_all()
			LOGGER.debug('Job %s is complete' % self.abbrev_info_unlocked())
	def __call__(self):
		func, args, kwargs = None, None, None
		with self.__lock:
			LOGGER.debug('Job %s is being executed' % self.abbrev_info_unlocked())
			func, args, kwargs = self.__func, self.__args, self.__kwargs
		return func(*args, **kwargs)
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

class Queued(object):
	__slots__ = '__queue',
	def __init__(self, queue):
		self.__queue = queue if queue is not None else Queue()
	def schedule(self, func, *args, **kwargs):
		LOGGER.debug('Got %s to schedule in queue %s' % (repr(func), self.__queue))
		if not isinstance(func, Job):
			job = Job(func, *args, **kwargs)
		else:
			if args or kwargs:
				LOGGER.warning('Cannot pass args=%s or kwargs=%s to preconstructed job' % (args, kwargs))
			job = func
		LOGGER.debug('Scheduling job %s in queue %s' % (repr(job), self.__queue))
		self.__queue.put(job)
		return job
	def __call__(self, func, *args, **kwargs):
		return self.schedule_sync(func, *args, **kwargs)
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


class Worker(Thread, Queued):
	def __init__(self, queue = None, autostart = False):
		Thread.__init__(self)
		Queued.__init__(self, queue)
		if autostart:
			self.start()
	def run(self):
		LOGGER.debug('Thread %s has started' % self)
		try:
			while True:
				job = self._Queued__queue.get()
				try:
					LOGGER.debug('Running job %s in thread %s:' % (repr(job), self))
					job.complete(job())
				except Finished:
					job.complete(None)
					break
				except BaseException as e:
					LOGGER.exception('Job %s in thread %s:' % (job, self))
					print_exception(type(e), e, None, file = sys.stderr)
					job.complete_exception(e)
				finally:
					self._Queued__queue.task_done()
		finally:
			LOGGER.debug('Thread %s has finished' % self)


class WorkerPool(Queued):
	__slots__ = '__workers',
	def __init__(self, size, autostart = True):
		Queued.__init__(self, None)
		self.__workers = [Worker(self._Queued__queue, autostart) for i in range(size)]
	def start(self):
		for worker in self.__workers:
			worker.start()
	def finish(self):
		for worker in self.__workers:
			worker.finish()
	def join(self):
		for worker in self.__workers:
			worker.join()

class RWAdapter(Job):
	__slots__ = '__method', '__read', '__write',
	def __init__(self, method):
		self.__read, self.__write = os.pipe()
		self.__read = os.fdopen(self.__read, 'rb')
		Job.__init__(self, self.run, method)
	def run(self, method):
		try:
			with os.fdopen(self.__write, 'wb') as outf:
				method(outf)
		except IOError:
			pass
		finally:
			self.__write = None
	def read(self, length = None):
		if length is None:
			return self.__read.read(length)
		else:
			return self.__read.read()
	def close_read(self):
		self.__read.close()
		self.__read = None
		self.join()
	def __enter__(self):
		return self
	def __exit__(self, type, value, tb):
		self.close_read()
	def abbrev_info_unlocked(self):
		return 'Job %s: RWAdapter' % self._Job__id
		
				





if __name__ == '__main__':
	import unittest
	from tempfile import TemporaryFile
	from shutil import copyfileobj
	import functools
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
	class WorkerPoolTest(unittest.TestCase):
		def process(self, incr = 1):
			self.count += incr
			return self.count
		def rexc(self, etype):
			raise etype()
		def setUp(self):
			self.pool = WorkerPool(5, autostart = True)
			self.count = 0
		def tearDown(self):
			self.pool.finish()
			self.pool.join()
		def test_sync(self):
			self.assertEqual(self.pool(self.process, 2), 2)
			self.assertEqual(self.count, 2)
		def test_default(self):
			self.assertEqual(self.pool(self.process), 1)
			self.assertEqual(self.count, 1)
		def test_async(self):
			job = self.pool.schedule(self.process, 1)
			self.assertIsNotNone(job)
			self.assertEqual(job.wait(), 1)
			self.assertEqual(self.count, 1)
		def test_many(self):
			toadd = list(range(100))
			total = sum(toadd)
			jobs = []
			for i in toadd:
				jobs.append(self.pool.schedule(self.process, i))
			self.assertTrue(all(jobs))
			self.assertEqual(jobs[-1].wait(), total)
			self.assertEqual(self.count, total)
		def test_exc(self):
			job = self.pool.schedule(self.rexc, ValueError)
			self.assertRaises(ValueError, job.wait)
	class RWAdapterTest(unittest.TestCase):
		def process(self, inf, outf):
			copyfileobj(inf, outf)
		def setUp(self):
			self.thread = Worker(autostart = True)
		def tearDown(self):
			self.thread.finish(True)
			self.thread.join()
		def test_copy(self):
			TEXT = b'abcde'
			with TemporaryFile('w+b') as tmp:
				tmp.write(TEXT)
				tmp.flush()
				tmp.seek(0)
				job = RWAdapter(functools.partial(self.process, tmp))
				self.thread.schedule(job)
				try:
					text = job.read()
					self.assertEqual(TEXT, text)
				finally:
					job.wait()

	unittest.main()
