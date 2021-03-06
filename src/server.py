#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import tornado.ioloop
import tornado.web
import logging, binascii, cgi, shelve, pickle, shutil
import config, cache, processors, filestuff, search, worker, common
from dateutil.parser import parse as date_parse
from threading import Semaphore
from pytz import utc
from dateutil.tz import tzlocal
from email.utils import format_datetime
from shutil import copyfileobj
from collections import namedtuple
import itertools, functools
from os.path import relpath, join as path_join, isdir
from os import mkdir
from codecs import getreader, getwriter

LOGGER = logging.getLogger('wikiserv')



class VarHost(object):
	__slots__ = 'runtime_vars',
	def __init__(self, path):
		self.runtime_vars = shelve.open(path, 'c', protocol = pickle.HIGHEST_PROTOCOL)
		common.fix_perms(path)
	def __del__(self):
		self.close()
	def close(self):
		if self.runtime_vars is not None:
			self.runtime_vars.close()
			self.runtime_vars = None
	def getvar(self, key):
		try:
			return self.runtime_vars[key]
		except KeyError:
			return None
	def setvar(self, key, value):
		self.runtime_vars[key] = value


class Server(VarHost):
	__slots__ = 'configuration', 'caches', 'processors', 'send_etags', 'search', 'preview_lines', 'workers', 'runtime_vars',
	instance = None
	ilock = Semaphore()
	localzone = tzlocal()
	CACHE_TYPES = {
		'document' : 'process',
		'preview' : 'doc_head',
	}
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
	@staticmethod
	def get_cache(configuration, process, subdir = None):
		ctype = cache.DispatcherCache if configuration.dispatcher_thread else cache.Cache
		return ctype(
			(path_join(configuration.cache_dir, subdir) if subdir is not None else configuration.cache_dir),
			configuration.source_dir,
			configuration.checksum_function,
			process,
			configuration.max_age,
			configuration.max_entries,
			configuration.auto_scrub
		)
	@classmethod
	def process_funcs(cls, obj):
		return {ctype : getattr(obj, method) for ctype, method in cls.CACHE_TYPES.items()}
	@classmethod
	def get_caches(cls, configuration, process_funcs, skip = frozenset()):
		if not isdir(configuration.cache_dir):
			mkdir(configuration.cache_dir)
		common.fix_dir_perms(configuration.cache_dir)

		pfsrc = None
		if hasattr(process_funcs, '__getitem__'):
			pfsrc = lambda ctype: process_funcs[ctype]
		else:
			pfsrc = lambda ctype: process_funcs
		return {ctype : cls.get_cache(configuration, pfsrc(ctype), ctype) for ctype in cls.CACHE_TYPES.keys() if not ctype in skip}
	def __init__(self, configuration):
		self.caches = {}
		self.workers = None
		self.search = None
		self.preview_lines = configuration.preview_lines
		self.processors = configuration.processors
		self.send_etags = configuration.send_etags
		VarHost.__init__(self, configuration.runtime_vars)
		skip = []
		if not self.preview_lines:
			skip.append('preview')
		else:
			preview_root = path_join(configuration.cache_dir, 'preview')
			if self.preview_lines != self.getvar('PREVIEW_LINES'):
				try:
					shutil.rmtree(preview_root)
				except OSError:
					pass
				self.setvar('PREVIEW_LINES', self.preview_lines)

		self.caches.update(self.get_caches(configuration, self.process_funcs(self), skip))
		if configuration.use_search_cache:
			self.search = search.Search(self, path_join(configuration.cache_dir, 'search'), \
					configuration.search_max_age, configuration.search_max_entries, configuration.search_auto_scrub)
		else:
			self.search = search.Search(self)
		self.workers = worker.WorkerPool(configuration.worker_threads, autostart = True)
	def __del__(self):
		self.close()
	def __getitem__(self, key):
		return self.cache[key]
	@property
	def root(self):
		return self.cache.source_root
	@property
	def cache(self):
		return self.caches['document']
	@property
	def preview(self):
		return self.caches.get('preview', None)
	def get_preview(self, path):
		LOGGER.debug('get_preview path=%s' % path)
		if not self.preview_lines:
			return None
		with self.preview[path] as preview:
			try:
				header = processors.Processor.read_header(preview)
				reader = getreader(header.encoding)(preview)
				return reader.read()
			except IOError:
				return None
	@property
	def default_processor(self):
		return self.processors[None]
	def process(self, inf, outf, cached):
		fname = inf.name
		for extension, processor in self.processors.items():
			if extension is None:
				continue
			elif fname.endswith(extension):
				return processor(inf, outf, cached)
		else:
			return self.default_processor(inf, outf, cached)
	def doc_head(self, inf, outf, cached):
		LOGGER.debug('doc_head inf=%s outf=%s' % (inf, outf))
		buff = inf.read(2048)
		header = processors.AutoBaseProcessor.auto_header(buff)
		if header.encoding is None:
			raise NotImplementedError
		inf.seek(0)
		reader = getreader(header.encoding)(inf)

		processors.Processor.write_header(outf, header)
		writer = getwriter(header.encoding)(outf)
		for line in itertools.islice(reader, self.preview_lines):
			writer.write(line)
	def close(self):
		for name, cache in self.caches.items():
			try:
				cache.close()
			except:
				LOGGER.exception('Closing cache [%s]=%s' % (name, cache))
		self.caches.clear()
		if self.workers is not None:
			self.workers.finish()
			self.workers.join()
			self.workers = None
		if self.search is not None:
			self.search.close()
			self.search = None
		VarHost.close(self)




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
	print('<p id="foot"><a href="/">Index</a>&nbsp;<a href=".search">Search</a></p>', file = stream)
	print('</body>\n</html>', file = stream)



class IndexHandler(tornado.web.RequestHandler):
	COUNT = 100
	def check_fill_headers(self, start, filter_func = None):
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
		
		self.set_header('Content-Type', 'application/xhtml+xml; charset=UTF-8')
		server = Server.get_instance()
		files, less, more = server.search.find_by_path(start, start + self.COUNT, filter_func)

		if not files:
			return [], (start > 0), more

		newest = max(files, key = lambda x: x.modified)
		self.set_header('Last-Modified', format_datetime(newest.modified))
		self.set_header('Cache-Control', ('no-cache' if filter_func else 'Public'))
		if prev_mtime is not None and newest.modified.replace(microsecond = 0) <= prev_mtime:
			LOGGER.debug('Returning 304 from modification time')
			self.set_status(304)
			return False, less, more
		return files, less, more

	FILTERS = [
		('filter', search.PathFilter),
		('search', search.ContentFilter),
	]
	def get_filter_func(self):
		filters = []
		for arg, func in self.FILTERS:
			try:
				filters.append(func(self.get_argument(arg, None)))
			except ValueError:
				continue
		LOGGER.debug('get_filter_func => %s' % filters)
		if not filters:
			return None
		elif len(filters) == 1:
			return filters[0]
		else:
			return search.CompoundFilter(filters)
	def head(self):
		try:
			start = int(self.get_argument('start', 0))
			if start < 0:
				start = 0
		except ValueError:
			start = 0
		filter_func = self.get_filter_func()
		LOGGER.debug('HEAD INDEX start=%d filter_func=%s' % (start, filter_func))
		self.check_fill_headers(start, filter_func)
	def get(self):
		try:
			start = int(self.get_argument('start', 0))
			if start < 0:
				start = 0
		except ValueError:
			start = 0
		filter_func = self.get_filter_func()
		LOGGER.debug('HEAD INDEX start=%d filter_func=%s' % (start, filter_func))
		files, less, more = self.check_fill_headers(start, filter_func)
		if files is False:
			return
		LOGGER.debug('Yielding %d files (more=%s, less=%s)' % (len(files), less, more))
		xhtml_head(self, 'Search' if filter_func else 'Index')
		if filter_func:
			print('<h1>Search</h1>', file = self)
			print('<p>Terms: %s</p>' % cgi.escape(str(filter_func)), file = self)
		else:
			print('<h1>Wiki Index</h1>', file = self)

		print('<ul>', file = self)
		server = Server.get_instance()
		for f in files:
			self.write('\t<li><a href="/%s">%s</a> @ %s (%f kB)' % (cgi.escape(f.name, True), cgi.escape(f.name), f.modified.astimezone(server.localzone).strftime('%c (%Z)'), (float(f.size) / 1024)))
			if filter_func and server:
				preview = server.get_preview(f.name)
				if preview:
					print('&nbsp;<pre style="display: block;">%s</pre>' % cgi.escape(preview), file = self)
			print('\t</li>', file = self)
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
	def check_fill_headers(self, entry, header = None):
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
		if header is None:
			header = entry.header
		if server.send_etags:
			checksum = header.checksum
			if checksum:
				self.set_header('Etag', '"%s"' % binascii.hexlify(checksum).decode('ascii'))
		self.set_header('Last-Modified', format_datetime(header.timestamp))
		self.set_header('Cache-Control', 'Public')
		content_header = processors.Processor.read_header(entry)
		if content_header.encoding:
			self.set_header('Content-Type', '%s; charset=%s' % (content_header.mime, content_header.encoding))
		else:
			self.set_header('Content-Type', content_header.mime)
		if prev_mtime is not None and header.timestamp.replace(microsecond = 0) <= prev_mtime:
			LOGGER.debug('Returning 304 from modification time')
			self.set_status(304)
			return False
		elif server.send_etags and self.check_etag_header():
			LOGGER.debug('Returning 304 from etags')
			self.set_status(304)
			return False
		return True
	def head(self, path):
		LOGGER.debug('HEAD %s' % path)
		try:
			server = Server.get_instance()
			wrap = server.cache[path]
			with wrap as entry:
				if isinstance(entry, cache.AutoProcess):
					# NoCache
					reader = worker.RWAdapter(entry)
					server.workers.schedule(reader)
					try:
						with reader:
							self.check_fill_headers(reader, entry.header)
					finally:
						reader.wait()

				else:
					self.check_fill_headers(entry)
		except KeyError:
			raise tornado.web.HTTPError(404)
	def get(self, path):
		LOGGER.debug('GET %s' % path)
		try:
			server = Server.get_instance()
			wrap = server.cache[path]
			with wrap as entry:
				if isinstance(entry, cache.AutoProcess):
					# NoCache
					reader = worker.RWAdapter(entry)
					server.workers.schedule(reader)
					try:
						with reader:
							if not self.check_fill_headers(reader, entry.header):
								return
							copyfileobj(reader, self)
					finally:
						reader.wait()
				else:
					if not self.check_fill_headers(entry):
						return
					LOGGER.debug('Returning data')
					copyfileobj(entry, self)
		except KeyError:
			raise tornado.web.HTTPError(404)

class SearchHandler(tornado.web.RequestHandler):
	CONTENT = \
"""<form action="/" method="GET">
	<fieldset>
		<legend>Search Terms</legend>
		<div>
			<label for="filter">Title</label>
			<input name="filter" id="filter" />
		</div>
		<div>
			<label for="search">Terms</label>
			<input name="search" id="search" />
		</div>
	</fieldset>
	<input type="submit" />
</form>"""
	def check_fill_headers(self):
		self.set_header('Cache-Control', 'Public')
		self.set_header('Content-Type', 'application/xhtml+xml; charset=UTF-8')
		prev_mtime = None
		try:
			prev_mtime = date_parse(self.request.headers['If-Modified-Since'])
			if prev_mtime.tzinfo is None:
				prev_mtime = prev_mtime.replace(tzinfo = utc)
			LOGGER.debug('Found If-Modified-Since=%s' % prev_mtime)
		except KeyError:
			pass
		with filestuff.File(__file__) as info:
			mtime = info.modified
		self.set_header('Last-Modified', format_datetime(mtime))
		if prev_mtime is not None and mtime.replace(microsecond = 0) <= prev_mtime:
			LOGGER.debug('Returning 304 from modification time')
			self.set_status(304)
			return False
		elif self.check_etag_header():
			LOGGER.debug('Returning 304 from etags')
			self.set_status(304)
			return False
		return True
	def head(self):
		self.check_fill_headers()
	def get(self):
		if not self.check_fill_headers():
			return
		xhtml_head(self, 'Search')
		print(self.CONTENT, file = self)
		xhtml_foot(self)

class SkipHandler(tornado.web.RequestHandler):
	def head(self):
		raise tornado.web.HTTPError(404)
	def get(self):
		raise tornado.web.HTTPError(404)

application = tornado.web.Application([
	(r'^/$', IndexHandler),
	(r'^/\.search$', SearchHandler),
	(r'^/.*\brobots\.txt$', SkipHandler),
	(r'^/.*\bfavicon\.ico$', SkipHandler),
	(r'^/(.+)$', WikiHandler),
])

if __name__ == '__main__':
	from argparse import ArgumentParser
	def positive_int(s):
		s = int(s)
		if s < 1:
			raise ValueError(s)
		return s

	parser = ArgumentParser(usage = '%(prog)s [ options ] -c config.xml ')
	parser.add_argument('--config', '-c', required = True, metavar = 'CONFIG.XML', dest = 'configuration', help = 'XML configuration file')
	parser.add_argument('--scrub', dest = 'scrub_only', action = 'store_true', default = False, help = 'Instead of running the server, just do a cache scrub')
	parser.add_argument('--bind-address', dest = 'bind_address', metavar = 'ADDRESS', help = 'Bind to ADDRESS instead of the address specified in configuration')
	parser.add_argument('--bind-port', dest = 'bind_port', metavar = 'ADDRESS', type = positive_int, help = 'Bind to ADDRESS instead of the port specified in configuration')

	args = parser.parse_args()

	cfg = None
	with open(args.configuration, 'rb') as f:
		cfg = config.Configuration(f, setlog = True)
	if args.bind_address is not None:
		cfg.bind_address = args.bind_address
	if args.bind_port is not None:
		cfg.bind_port = args.bind_port

	if not args.scrub_only:
		Server.set_instance(cfg)
		try:
			application.listen(cfg.bind_port, cfg.bind_address)
			tornado.ioloop.IOLoop.instance().start()
		finally:
			Server.close_instance()
	else:
		def fake_process(inf, outf, cached):
			raise RuntimeError('Cannot serve pages in scrub mode')
		class FakeServer(VarHost):
			__slots__ = 'root',
			def __init__(self, configuration):
				LOGGER.debug('Using fake server with runtime_vars=%s and root=%s' % (configuration.runtime_vars, configuration.source_dir))
				VarHost.__init__(self, configuration.runtime_vars)
				self.root = configuration.source_dir
			def __del__(self):
				self.close()
		cfg.auto_scrub = False
		for cache in Server.get_caches(cfg, fake_process).values():
			cache.close()
		if cfg.use_search_cache:
			server = FakeServer(cfg)
			try:
				search.Search(server, path_join(cfg.cache_dir, 'search'), \
						cfg.search_max_age, cfg.search_max_entries, cfg.search_auto_scrub).close()
			finally:
				server.close()
