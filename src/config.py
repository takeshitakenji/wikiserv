#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

from lxml import etree
from datetime import timedelta
import hashers, processors
from os.path import join as path_join, dirname, normpath, isabs, abspath
import logging

LOGGER = logging.getLogger(__name__)

def positive_int(value):
	value = int(value)
	if value < 1:
		raise ValueError('Not a positive integer: %s' % value)
	return value

class Configuration(object):
	@staticmethod
	def xpath_single(document, xpath, nsmap = None):
		matches = document.xpath(xpath, namespaces = nsmap)
		if not matches:
			raise KeyError('Missing element: %s' % xpath)
		return matches[0]
	@staticmethod
	def get_path(current_dir, path):
		path = normpath(path)
		if isabs(path):
			return path
		else:
			return abspath(path_join(current_dir, path))
	def include_processors(self, root, source_path):
		# TODO: Iterate over /configuration/processors/include to include external XML files, noting absolute paths and paths relative to stream.name.
		included_processors = {}
		procs = {}
		for child in root.xpath('processor'):
			name = ''.join(child.xpath('text()'))
			extensions = None
			try:
				extensions = (x.strip() for x in child.attrib['extensions'].split())
				extensions = [x for x in extensions if x]
			except KeyError:
				pass

			mime = None
			try:
				mime = child.attrib['mime-type'].strip()
			except KeyError:
				pass

			proc = None
			if (name, mime) in procs:
				proc = procs[name, mime]
			else:
				if mime is not None:
					try:
						proc = processors.get_processor(name)(mime, self.encoding)
					except TypeError:
						LOGGER.warning('Processor %s does not support MIME assignment' % name)
						mime = none
				if proc is None:
					proc = processors.get_processor(name)(self.encoding)

				procs[name, mime] = proc
			if proc is None:
				raise RuntimeError
			if extensions:
				for extension in extensions:
					included_processors[extension] = proc
			else:
				included_processors[None] = proc
		LOGGER.debug('Resulting procs from %s: %s' % (source_path, procs))
		return included_processors
	def __init__(self, stream, setlog = False):
		document = etree.parse(stream)
		try:
			log_level = self.xpath_single(document, '/configuration/log-level/text()').strip().upper()
			self.log_level = getattr(logging, log_level)
		except (KeyError, AttributeError):
			self.log_level = logging.ERROR
		if setlog:
			logging.basicConfig(level = self.log_level)
		self.source_dir = self.get_path(dirname(stream.name), self.xpath_single(document, '/configuration/document-root/text()').strip())
		self.runtime_vars = self.get_path(dirname(stream.name), self.xpath_single(document, '/configuration/runtime-vars/text()').strip())
		try:
			self.preview_lines = int(self.xpath_single(document, '/configuration/preview-lines/text()').strip())
		except KeyError:
			self.preview_lines = None
		try:
			self.worker_threads = int(self.xpath_single(document, '/configuration/worker-threads/text()').strip())
			if self.worker_threads < 1:
				raise ValueError(self.worker_threads)
		except KeyError:
			self.worker_threads = 1

		self.cache_dir = self.get_path(dirname(stream.name), self.xpath_single(document, '/configuration/cache/@dir').strip())
		self.checksum_function = hashers.get_hasher( \
			self.xpath_single(document, '/configuration/cache/checksum-function/text()').strip())

		try:
			self.bind_address = self.xpath_single(document, '/configuration/bind-address/text()').strip()
		except KeyError:
			self.bind_address = ''

		self.bind_port = int(self.xpath_single(document, '/configuration/bind-port/text()').strip())

		# Main cache
		try:
			self.max_age = timedelta(seconds = positive_int(self.xpath_single(document, '/configuration/cache/max-age/text()')))
		except KeyError:
			self.max_age = None

		try:
			self.max_entries = positive_int(self.xpath_single(document, '/configuration/cache/max-entries/text()'))
		except KeyError:
			self.max_entries = None

		self.auto_scrub = bool(document.xpath('/configuration/cache/auto-scrub'))
		self.send_etags = bool(document.xpath('/configuration/cache/send-etags'))


		# Search cache
		self.use_search_cache = bool(document.xpath('/configuration/search-cache'))
		try:
			self.search_max_age = timedelta(seconds = positive_int(self.xpath_single(document, '/configuration/search-cache/max-age/text()')))
		except KeyError:
			self.search_max_age = None

		try:
			self.search_max_entries = positive_int(self.xpath_single(document, '/configuration/search-cache/max-entries/text()'))
		except KeyError:
			self.search_max_entries = None

		self.search_auto_scrub = bool(document.xpath('/configuration/search-cache/auto-scrub'))



		self.dispatcher_thread = bool(document.xpath('/configuration/cache/dispatcher-thread'))

		self.encoding = self.xpath_single(document, '/configuration/processors/encoding/text()')

		self.processors = {}
		self.processors.update(self.include_processors(self.xpath_single(document, '/configuration/processors'), stream.name))

		LOGGER.debug('Resulting processors: %s' % self.processors)

		if None not in self.processors:
			LOGGER.warning('There is no processor defined for unspecified file extensions; setting default to autoraw-nocache.')
			self.processors[None] = processors.get_processor('autoraw-nocache')(self.encoding)
	@property
	def default_processor(self):
		return self.processors[None]




if __name__ == '__main__':
	import unittest, logging
	from hashlib import sha1
	from os.path import join as path_join, dirname
	logging.basicConfig(level = logging.DEBUG)
	
	class TestConfig(unittest.TestCase):
		CONFIG_PATH = path_join(dirname(__file__), 'testdata', 'example_config_test.xml')
		def test_read_config(self):
			with open(self.CONFIG_PATH, 'r', encoding = 'utf8') as f:
				config = Configuration(f)
			self.assertIsNotNone(config)
			self.assertEqual(config.cache_dir, abspath(path_join('testdata', 'example-cache')))
			self.assertEqual(config.source_dir, abspath(path_join('testdata', 'example-source')))
			self.assertIsNotNone(config.checksum_function)
			self.assertEqual(config.max_age, timedelta(seconds = 86400))
			self.assertEqual(config.max_entries, 2048)
			self.assertTrue(config.auto_scrub)
			self.assertEqual(config.encoding, 'utf8')
			self.assertTrue(config.processors)
			self.assertIn(None, config.processors)
			self.assertIsInstance(config.default_processor, processors.Processor)
			print(config.processors)
			for extension, processor in config.processors.items():
				if extension is None:
					continue
				self.assertIsInstance(extension, str)
				self.assertIsInstance(processor, processors.Processor)



	unittest.main()
