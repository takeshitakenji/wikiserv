#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import tornado.ioloop
import tornado.web
import logging, binascii, cgi
import config, cache, processors, filestuff
from dateutil.parser import parse as date_parse
from threading import Semaphore
from pytz import utc
from email.utils import format_datetime
from shutil import copyfileobj
from collections import namedtuple
import itertools
from os.path import relpath

LOGGER = logging.getLogger('wikiserv')



class Server(object):
	__slots__ = 'configuration', 'cache', 'processors', 'send_etags',
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
		self.send_etags = configuration.send_etags
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



Element = namedtuple('Element', ['tag', 'attrib', 'text'])

def xhtml_head(stream, title, *head):
	print('<?xml version="1.0" encoding="UTF-8" ?>', file = stream)
	print('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">', file = stream)
	print('<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">', file = stream)
	print('<head>', file = stream)
	print('	<title>%s</title>' % cgi.escape(title), file = stream)
	for element in head:
		selement = ['<%s' % element.tag]
		if element.attrib:
			selement += [
				' ',
				' '.join(('%s="s%"' % (key, cgi.escape(value, True)) \
						for key, value in element.attrib.items())),
			]
		if element.text:
			selement.append(' >%s</%s>' % (element.text, element.tag))
		else:
			selement.append(' />')
		print(''.join(selement), file = stream)
	print('</head>\n<body>', file = stream)

def xhtml_foot(stream):
	print('</body>\n</html>', file = stream)
		


class IndexHandler(tornado.web.RequestHandler):
	COUNT = 100
	Info = namedtuple('Info', ['name', 'modified', 'size'])
	@classmethod
	def get_info(cls, path, root):
		try:
			with filestuff.LockedFile(path) as f:
				return cls.Info(relpath(path, root), f.modified, f.size)
		except OSError:
			return None
	def check_fill_headers(self, start, filter_string):
		LOGGER.debug('Getting headers for request')
		prev_mtime = None
		server = Server.get_instance()
		try:
			prev_mtime = date_parse(self.request.headers['If-Modified-Since'])
			if prev_mtime.tzinfo is None:
				prev_mtime = prev_mtime.replace(tzinfo = utc)
			LOGGER.debug('Found If-Modified-Since=%s' % prev_mtime)
		except KeyError:
			pass
		
		path_filter = lambda path: True
		if filter_string is not None:
			path_filter = lambda path: filter_string in path.lower()

		self.set_header('Content-Type', 'application/xhtml+xml; charset=UTF-8')
		server = Server.get_instance()
		find_files = (path for path in sorted(cache.Cache.find_files(server.cache.source_root)) if path_filter(path))
		files = (self.get_info(x, server.cache.source_root) for x in itertools.islice(find_files, start, start + self.COUNT, 1))
		files = [x for x in files if x is not None]

		more = False
		try:
			find_files.__next__()
			more = True
		except StopIteration:
			pass
		if not files:
			return [], (start > 0), more

		newest = max(files, key = lambda x: x.modified)
		self.set_header('Last-Modified', format_datetime(newest.modified))
		self.set_header('Cache-Control', 'Public')
		if prev_mtime is not None and newest.modified <= prev_mtime:
			LOGGER.debug('Returning 304 from modification time')
			self.set_status(304)
			return False, (start > 0), more
		return files, (start > 0), more
	def head(self):
		try:
			start = int(self.get_argument('start', 0))
			if start < 0:
				start = 0
		except ValueError:
			start = 0
		filter_string = self.get_argument('filter', None)
		LOGGER.debug('HEAD INDEX start=%d' % start)
		self.check_fill_headers(start, filter_string)
	def get(self):
		try:
			start = int(self.get_argument('start', 0))
			if start < 0:
				start = 0
		except ValueError:
			start = 0
		filter_string = self.get_argument('filter', None)
		LOGGER.debug('GET INDEX start=%d' % start)
		files, less, more = self.check_fill_headers(start, filter_string)
		if files is False:
			return False
		LOGGER.debug('Yielding %d files (more=%s, less=%s)' % (len(files), less, more))
		xhtml_head(self, 'Index')
		print('<h1>Wiki Index</h1>', file = self)
		print('<ul>', file = self)
		for f in files:
			print('\t<li><a href="/%s">%s</a> @ %s (%f kB)</li>' % (cgi.escape(f.name, True), cgi.escape(f.name), f.modified, (float(f.size) / 1024)), file = self)
		print('</ul>', file = self)
		print('<p>', file = self)
		if less:
			print('\t<a href="/?start=%d">Previous Page</a>' % max(start - self.COUNT, 0), file = self)
		if less and more:
			print('\t&nbsp;|&nbsp;', file = self)
		if more:
			print('\t<a href="/?start=%d>Next Page</a>' % (start + self.COUNT), file = self)
		print('</p>', file = self)
		

			
		xhtml_foot(self)


class WikiHandler(tornado.web.RequestHandler):
	def compute_etag(self):
		return None
	def check_fill_headers(self, entry):
		LOGGER.debug('Getting headers for request')
		prev_mtime = None
		server = Server.get_instance()
		try:
			prev_mtime = date_parse(self.request.headers['If-Modified-Since'])
			if prev_mtime.tzinfo is None:
				prev_mtime = prev_mtime.replace(tzinfo = utc)
			LOGGER.debug('Found If-Modified-Since=%s' % prev_mtime)
		except KeyError:
			pass
		if server.send_etags:
			self.set_header('Etag', '"%s"' % binascii.hexlify(entry.header.checksum).decode('ascii'))
		self.set_header('Last-Modified', format_datetime(entry.header.timestamp))
		self.set_header('Cache-Control', 'Public')
		content_header = processors.Processor.read_header(entry)
		self.set_header('Content-Type', '%s; charset=%s' % (content_header.mime, content_header.encoding))
		if prev_mtime is not None and entry.modified <= prev_mtime:
			LOGGER.debug('Returning 304 from modification time')
			self.set_status(304)
			return False
		elif server.send_etags and self.check_etag_header():
			LOGGER.debug('Returning 304 from etags time')
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
	(r'^/$', IndexHandler),
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