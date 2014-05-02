#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

from lxml import etree
from datetime import timedelta
import hashers, processors
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
	def __init__(self, stream):
		document = etree.parse(stream)
		try:
			log_level = self.xpath_single(document, '/configuration/log-level/text()').strip().upper()
			self.log_level = getattr(logging, log_level)
		except (KeyError, AttributeError):
			self.log_level = logging.ERROR
		self.cache_dir = self.xpath_single(document, '/configuration/cache/cache-dir/text()').strip()
		self.source_dir = self.xpath_single(document, '/configuration/cache/source-dir/text()').strip()
		self.checksum_function = hashers.get_hasher( \
			self.xpath_single(document, '/configuration/cache/checksum-function/text()').strip())

		try:
			self.max_age = timedelta(seconds = positive_int(self.xpath_single(document, '/configuration/cache/max-age/text()')))
		except KeyError:
			self.max_age = None

		try:
			self.max_entries = positive_int(self.xpath_single(document, '/configuration/cache/max-entries/text()'))
		except KeyError:
			self.max_entries = None

		self.auto_scrub = bool(document.xpath('/configuration/cache/auto-scrub'))
		self.dispatcher_thread = bool(document.xpath('/configuration/cache/dispatcher-thread'))

		self.encoding = self.xpath_single(document, '/configuration/processors/encoding/text()')

		self.processors = {}
		for child in document.xpath('/configuration/processors/processor'):
			name = ''.join(child.xpath('text()'))
			extensions = None
			try:
				extensions = (x.strip() for x in child.attrib['extensions'].split())
				extensions = [x for x in extensions if x]
			except KeyError:
				pass

			proctype = processors.get_processor(name)
			if extensions:
				proc = proctype(self.encoding)
				for extension in extensions:
					self.processors[extension] = proc
			else:
				self.processors[None] = proctype(self.encoding)
		if None not in self.processors:
			LOGGER.warning('There is no processor defined for unspecified file extensions.')
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
			self.assertEqual(config.cache_dir, 'example-cache')
			self.assertEqual(config.source_dir, 'example-source')
			self.assertIsNotNone(config.checksum_function)
			self.assertEqual(config.max_age, timedelta(seconds = 86400))
			self.assertEqual(config.max_entries, 2048)
			self.assertTrue(config.auto_scrub)
			self.assertEqual(config.encoding, 'utf8')
			self.assertTrue(config.processors)
			self.assertIn(None, config.processors)
			self.assertIsInstance(config.default_processor, processors.Processor)
			for extension, processor in config.processors.items():
				if extension is None:
					continue
				self.assertIsInstance(extension, str)
				self.assertIsInstance(processor, processors.Processor)



	unittest.main()
