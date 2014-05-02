#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import tornado.ioloop
import tornado.web
import logging, binascii
import config, cache, processors
from dateutil.parser import parse as date_parse
from threading import Semaphore
from pytz import utc
from email.utils import format_datetime
from shutil import copyfileobj


LOGGER = logging.getLogger('wikiserv')



class Server(object):
	__slots__ = 'configuration', 'cache', 'processors'
	instance = None
	ilock = Semaphore()
	@classmethod
	def get_instance(cls):
		with cls.ilock:
			if cls.instance is None:
				raise RuntimeError
			return cls.instance
	@classmethod
	def set_instance(cls, configuration):
		with cls.ilock:
			cls.instance = cls(configuration)
	@classmethod
	def close_instance(cls):
		with cls.ilock:
			cls.instance.close()
			cls.instance = None
	def __init__(self, configuration):
		self.processors = configuration.processors
		ctype = cache.DispatcherCache if configuration.dispatcher_thread else cache.Cache
		self.cache = ctype(
			configuration.cache_dir,
			configuration.source_dir,
			configuration.checksum_function,
			self.process,
			configuration.max_age,
			configuration.max_entries,
			configuration.auto_scrub
		)
	def __getitem__(self, key):
		return self.cache[key]
	@property
	def default_processor(self):
		return self.processors[None]
	def process(self, inf, outf):
		fname = inf.name
		for extension, processor in self.processors.items():
			if extension is None:
				continue
			elif fname.endswith(extension):
				return processor(inf, outf)
		else:
			return self.default_processor(inf, outf)
	def close(self):
		self.cache.close()

class WikiHandler(tornado.web.RequestHandler):
	def check_fill_headers(self, entry):
		LOGGER.debug('Getting headers for request')
		prev_mtime = None
		try:
			prev_time = date_parse(self.request.headers['If-Modified-Since'])
			if prev_time.tzinfo is None:
				prev_time = prev_time.replace(tzinfo = utc)
			LOGGER.debug('Found If-Modified-Since=%s' % prev_time)
		except KeyError:
			pass
		self.set_header('Etag', binascii.hexlify(entry.header.checksum))
		self.set_header('Last-Modified', format_datetime(entry.header.timestamp))
		self.set_header('Cache-Control', 'Public')
		content_header = processors.Processor.read_header(entry)
		self.set_header('Content-Type', '%s; charset=%s' % (content_header.mime, content_header.encoding))
		if (prev_mtime is not None and entry.timestamp <= prev_mtime) \
				or self.check_etag_header():
			LOGGER.debug('Returning 304')
			self.set_status(304)
			return False
		return True
	def head(self, path):
		LOGGER.debug('HEAD %s' % path)
		try:
			wrap = Server.get_instance().cache[path]
		except KeyError:
			raise tornado.web.HTTPError(404)
		with wrap as entry:
			self.check_fill_headers(entry)
	def get(self, path):
		LOGGER.debug('GET %s' % path)
		try:
			wrap = Server.get_instance().cache[path]
		except KeyError:
			raise tornado.web.HTTPError(404)
		with wrap as entry:
			if not self.check_fill_headers(entry):
				return
			LOGGER.debug('Returning data')
			copyfileobj(entry, self)


class SkipHandler(tornado.web.RequestHandler):
	def head(self):
		raise tornado.web.HTTPError(404)
	def get(self):
		raise tornado.web.HTTPError(404)

application = tornado.web.Application([
	(r'^/.*\brobots\.txt$', SkipHandler),
	(r'^/.*\bfavicon\.ico$', SkipHandler),
	(r'^/(.+)$', WikiHandler),
])

if __name__ == '__main__':
	from argparse import ArgumentParser

	parser = ArgumentParser('%(proc)s [ options ] -c config.xml ')
	parser.add_argument('--config', '-c', required = True, dest = 'configuration', help = 'XML configuration file')

	args = parser.parse_args()

	cfg = None
	with open(args.configuration, 'rb') as f:
		cfg = config.Configuration(f)

	logging.basicConfig(level = cfg.log_level)

	Server.set_instance(cfg)
	try:
		application.listen(8888)
		tornado.ioloop.IOLoop.instance().start()
	finally:
		Server.close_instance()
