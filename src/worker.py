#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')


from queue import Queue
from threading import Thread, Condition, Lock


class Finished(Exception):
	@classmethod
	def finish(cls):
		raise cls


class Job(object):
	__slots__ = '__func', '__args', '__kwargs', '__completed', '__result', '__exception', '__lock', '__cond'
	def __init__(self, func, *args, **kwargs):
		if not callable(func):
			raise ValueError(func)
		self.__lock = Lock()
		self.__cond = Condition(self.__lock)
		self.__func, self.__args, self.__kwargs = func, args, kwargs
		self.__completed, self.__result, self.__exception = False, None, None
	def complete(self, result):
		with self.__lock:
			self.__completed = True
			self.__result = result
			self.__cond.notify_all()
	def complete_exception(self, exception):
		with self.__lock:
			self.__completed = True
			self.__exception = exception
			self.__cond.notify_all()
	def __call__(self):
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
	def __init__(self, queue = None):
		Thread.__init__(self)
		self.__queue = queue if queue is not None else Queue()
	def run(self):
		while True:
			job = self.__queue.get()
			try:
				job.complete(job())
			except Finished:
				job.complete(None)
				break
			except BaseException as e:
				print_exception(type(e), value, 'Job %s in thread %s' % (job, self))
				job.complete_exception(e)
			finally:
				self.__queue.task_done()
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

	class InitTest(unittest.TestCase):
		def test_init(self):
			thread = Worker()
			thread.start()
			thread.finish()
			thread.join()

			thread = Worker()
			thread.start()
			thread.finish(True)
			thread.join()
	class WorkerTest(unittest.TestCase):
		pass
	unittest.main()
