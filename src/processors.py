#!/usr/bin/env python2
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

from os import environ, getuid
import struct, platform, os, stat
from collections import namedtuple
from os.path import pathsep, join as path_join, normpath, isfile, basename
import logging
from subprocess import Popen, CalledProcessError, PIPE
from shutil import copyfileobj
import magic, chardet
from tempfile import TemporaryFile
from codecs import getreader, getwriter
from time import sleep
import cache

LOGGER = logging.getLogger(__name__)



if platform.system() == 'Windows':
	def find_executable(executable):
		if basename(executable) != executable:
			raise ValueError(executable)
		PATH = [normpath(p) for p in environ['PATH'].split(pathsep)]
		PATHEXT = environ['PATHEXT'].split(pathsep)

		executable_ext = frozenset(((executable + ext) for ext in PATHEXT))

		for path in PATH:
			path = path_join(path, executable_ext)
			if isfile(path):
				return path
		raise ValueError(executable)
else:
	import grp, pwd
	def get_user_groups(current_uid):
		current_user = pwd.getpwuid(current_uid)
		groups = {g.gr_gid for g in grp.getgrall() if current_user.pw_name in g.gr_mem}
		gid = current_user.pw_gid
		groups.add(gid)
		return frozenset(groups)
	def executable_by_user(current_uid, current_groups, info):
		if stat.S_ISDIR(info.st_mode):
			return False
		elif info.st_uid == current_uid:
			required = stat.S_IRUSR | stat.S_IXUSR
			return (required & info.st_mode) == required
		elif info.st_gid in current_groups:
			required = stat.S_IRGRP | stat.S_IXGRP
			return (required & info.st_mode) == required
		else:
			required = stat.S_IROTH | stat.S_IXOTH
			return (required & info.st_mode) == required

	def find_executable(executable):
		current_uid = getuid()
		current_groups = get_user_groups(current_uid)
		if basename(executable) != executable:
			raise ValueError(executable)
		PATH = [normpath(p) for p in environ['PATH'].split(pathsep)]
		for path in PATH:
			path = path_join(path, executable)
			info = None
			try:
				info = os.stat(path)
			except OSError:
				continue
			if executable_by_user(current_uid, current_groups, info):
				return path
			LOGGER.debug('Skipping path: %s' % path)
		raise ValueError(executable)


class BaseProcessor(object):
	Header = namedtuple('Header', ['encoding', 'mime'])
	length_format = '!B'
	length_length = 1

	NAME = NotImplemented
	MIME = NotImplemented

	processors = {}
	@classmethod
	def register(cls):
		if any((x is NotImplemented for x in [cls.NAME, cls.MIME])):
			raise RuntimeError('Class %s is not set up properly' % cls)
		LOGGER.debug('Registered processor [%s] = %s' % (cls.NAME, cls))
		cls.processors[cls.NAME] = cls
	@classmethod
	def available_processors(cls):
		return frozenset(cls.processors.keys())
	@classmethod
	def get_processor(cls, name):
		return cls.processors[name]
	@classmethod
	def write_header(self, stream, header):
		LOGGER.debug('Writing header to %s' % stream)
		count = 0
		if header.encoding is not None:
			encoding = header.encoding.encode('ascii')
			count += stream.write(struct.pack(self.length_format, len(encoding)))
			count += stream.write(encoding)
		else:
			count += stream.write(struct.pack(self.length_format, 0))

		mime = header.mime.encode('ascii')
		count += stream.write(struct.pack(self.length_format, len(mime)))
		count += stream.write(mime)
		return count
	@classmethod
	def read_header(cls, stream):
		LOGGER.debug('Reading header from %s' % stream)
		#print(repr(stream.read(cls.length_length)))
		try:
			length, = struct.unpack(cls.length_format, stream.read(cls.length_length))
		except struct.error:
			LOGGER.exception('When reading %s' % stream)
			raise IOError
		encoding = None
		if length > 0:
			encoding = stream.read(length).decode('ascii')

		try:
			length, = struct.unpack(cls.length_format, stream.read(cls.length_length))
		except struct.error:
			LOGGER.exception('When reading %s' % stream)
			raise IOError
		mime = stream.read(length).decode('ascii')

		return cls.Header(encoding, mime)
	def process(self, inf, outf):
		raise NotImplementedError
	def __call__(self, inf, outf, cached):
		return self.process(inf, outf)


class Processor(BaseProcessor):
	@classmethod
	def call_process(cls, args, inf, outf, copy_in = False):
		p = None
		if copy_in:
			p = Popen(args, stdin = PIPE, stdout = PIPE)
		else:
			p = Popen(args, stdin = inf, stdout = PIPE)
		try:
			if copy_in:
				copyfileobj(inf, p.stdin)
				p.stdin.close()
			# This is needed because the header will be overwritten otherwise.
			copyfileobj(p.stdout, outf)
			p.stdout.close()
			p.wait()
		except:
			p.terminate()
			p.wait()
			raise
		finally:
			if p.returncode != 0:
				raise CalledProcessError('%s exited with %s' % (args[0], p.returncode))
	
	__slots__ = 'header',
	def __init__(self, encoding):
		BaseProcessor.__init__(self)
		if len(self.mime_type) > 0xFF:
			raise ValueError('MIME type is too long: %s' % self.mime_type)
		if encoding is not None and len(encoding) > 0xFF:
			raise ValueError('Character encoding is too long: %s' % self.encoding)
		# Verify they are ASCII
		self.mime_type.encode('ascii')
		if encoding is not None:
			encoding.encode('ascii')
			b''.decode(encoding)
		
		self.header = self.Header(encoding, self.mime_type)
	@property
	def mime_type(self):
		return self.MIME
	def __call__(self, inf, outf, cached):
		self.write_header(outf, self.header)
		return self.process(inf, outf)


class RawProcessor(Processor):
	NAME = 'raw'
	MIME = None
	__slots__ = 'mime',
	def __init__(self, mime, encoding):
		self.mime = mime
		Processor.__init__(self, None)
	def process(self, inf, outf):
		copyfileobj(inf, outf)
	@property
	def mime_type(self):
		return self.mime
RawProcessor.register()


class AutoBaseProcessor(BaseProcessor):
	def __init__(self, encoding):
		BaseProcessor.__init__(self)
	def process(self, inf, outf):
		copyfileobj(inf, outf)
	@classmethod
	def auto_header(cls, buff):
		mime_type = magic.from_buffer(buff, mime = True).decode('ascii')
		cinfo = chardet.detect(buff)

		encoding = cinfo['encoding'] if cinfo['confidence'] > 0.75 else None

		LOGGER.debug('Detected encoding=%s mime_type=%s' % (encoding, mime_type))
		return cls.Header(encoding, mime_type)
	def __call__(self, inf, outf, cached):
		try:
			header = self.auto_header(inf.read(2048))
			self.write_header(outf, header)
		finally:
			inf.seek(0)
		return self.process(inf, outf)

class AutoRawProcessor(AutoBaseProcessor):
	NAME = 'autoraw'
	MIME = None
	def process(self, inf, outf):
		copyfileobj(inf, outf)
AutoRawProcessor.register()

class AutoRawNoCacheProcessor(AutoBaseProcessor):
	NAME = 'autoraw-nocache'
	MIME = None
	def process(self, inf, outf):
		copyfileobj(inf, outf)
	def __call__(self, inf, outf, cached):
		if cached:
			raise cache.NoCache
		LOGGER.debug('autoraw-nocache: %s -> %s' % (inf, outf))
		try:
			header = self.auto_header(inf.read(2048))
			self.write_header(outf, header)
		finally:
			inf.seek(0)

		return self.process(inf, outf)
AutoRawNoCacheProcessor.register()


try:
	asciidoc = find_executable('asciidoc')
	class AsciidocProcessor(Processor):
		BACKEND = NotImplemented
		ATTRIBUTES = []
		FOOTER_LINK = '\n\'\'\'\'\nlink:/[Index]\n'
		insert_link = True
		__slots__ = 'footer_link',
		def __init__(self, encoding):
			Processor.__init__(self, encoding)
			self.footer_link = self.FOOTER_LINK.encode(encoding)
		def process(self, inf, outf):
			if self.BACKEND is NotImplemented:
				raise NotImplementedError
			args = ['asciidoc', '-b', self.BACKEND, '-a', 'encoding=%s' % self.header.encoding]
			for attr in self.ATTRIBUTES:
				args += ['-a', attr]
			args.append('-')
			if self.insert_link:
				with TemporaryFile('r+b') as tmp:
					copyfileobj(inf, tmp)
					tmp.write(self.footer_link)
					tmp.flush()
					tmp.seek(0)
					self.call_process(args, tmp, outf)
			else:
				self.call_process(args, inf, outf)
	class AsciidocXHTMLProcessor(AsciidocProcessor):
		BACKEND = 'xhtml11'
		NAME = 'asciidoc-xhtml11'
		MIME = 'application/xhtml+xml'
		ATTRIBUTES = ['toc2']
	AsciidocXHTMLProcessor.register()

	class AsciidocHTML5Processor(AsciidocProcessor):
		BACKEND = 'html5'
		NAME = 'asciidoc-html5'
		ATTRIBUTES = ['toc2']
		MIME = 'text/html'
	AsciidocHTML5Processor.register()

	class AsciidocHTML4Processor(AsciidocProcessor):
		BACKEND = 'html4'
		NAME = 'asciidoc-html4'
		MIME = 'text/html'
	AsciidocHTML4Processor.register()
except ValueError:
	pass

try:
	import markdown
	class MarkdownProcessor(Processor):
		BACKEND = NotImplemented
		EXTENSIONS = []
		DOCUMENT_START = NotImplemented
		DOCUMENT_END = '\n</body>\n</html>\n'
		FOOTER_LINK = '\n<p><a href="/">Index</a></p>\n'
		insert_link = True
		def process(self, inf, outf):
			if self.DOCUMENT_START is NotImplemented or self.BACKEND is NotImplemented:
				raise NotImplementedError
			reader = getreader(self.header.encoding)(inf)
			writer = getwriter(self.header.encoding)(outf)

			writer.write(self.DOCUMENT_START.format(title = basename(inf.name)))
			html = markdown.markdown(reader.read(), extensions = self.EXTENSIONS, output_format = self.BACKEND, safe_mode = 'escape')
			writer.write(html)
			if self.insert_link:
				writer.write(self.FOOTER_LINK)
			writer.write(self.DOCUMENT_END)
	class MarkdownXHTMLProcessor(MarkdownProcessor):
		BACKEND = 'xhtml1'
		NAME = 'markdown-xhtml1'
		MIME = 'application/xhtml+xml'

		DOCUMENT_START = \
"""<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
	<title>{title}</title>
</head>
<body>
"""
	MarkdownXHTMLProcessor.register()

	class MarkdownHTML5Processor(MarkdownProcessor):
		BACKEND = 'html5'
		NAME = 'markdown-html5'
		MIME = 'text/html'

		DOCUMENT_START = \
"""<!DOCTYPE html PUBLIC>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
	<title>{title}</title>
</head>
<body>
"""
	MarkdownHTML5Processor.register()

	class MarkdownHTML4Processor(MarkdownProcessor):
		BACKEND = 'html4'
		NAME = 'markdown-html4'
		MIME = 'text/html'

		DOCUMENT_START = \
"""<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
	<title>{title}</title>
</head>
<body>
"""
	MarkdownHTML4Processor.register()


except ImportError:
	pass

def available_processors():
	LOGGER.debug('Getting available hashers')
	return Processor.available_processors()

def get_processor(name):
	LOGGER.debug('Getting processor %s' % name)
	try:
		return Processor.get_processor(name)
	except:
		LOGGER.exception('On attempting to get processor %s from %s' % (name, Processor.available_processors()))
		raise


if __name__ == '__main__':
	import unittest
	from tempfile import NamedTemporaryFile
	from os import remove
	from lxml import etree

	logging.basicConfig(level = logging.DEBUG)
	class TestPath(unittest.TestCase):
		def test_executable(self):
			if platform.system() == 'Windows':
				path = find_executable('cmd')
				self.assertIsNotNone(path)
			else:
				path = find_executable('sh')
				self.assertIsNotNone(path)
	class TestListProcessors(unittest.TestCase):
		def test_correct_type(self):
			available = available_processors()
			self.assertIsInstance(available, frozenset)
		def test_init(self):
			available = available_processors()
			for name in available:
				proctype = get_processor(name)
				self.assertIn(BaseProcessor, proctype.__mro__)

				try:
					proc = proctype('application/octet-string', None)
					self.assertIsNotNone(proc)
				except TypeError:
					proc = proctype('utf8')
					self.assertIsNotNone(proc)
					if hasattr(proc, 'header'):
						self.assertEqual(proc.header.encoding, 'utf8')
	class TestHeader(unittest.TestCase):
		class FakeProcessor(Processor):
			NAME = 'Fake'
			MIME = 'text/plain'
		def test_header(self):
			name = None
			processor = self.FakeProcessor('utf8')
			text = 'BLAH'
			with NamedTemporaryFile('wb', delete = False) as f:
				name = f.name
				processor.write_header(f, processor.header)
				f.write(text.encode('utf8'))
			try:
				with open(name, 'rb') as f:
					header = self.FakeProcessor.read_header(f)
					ftext = f.read().decode('utf8')
				self.assertEqual(header, processor.header)
				self.assertEqual(text, ftext)
			finally:
				remove(name)
	if 'AsciidocXHTMLProcessor' in vars():
		class TestAsciidoc(unittest.TestCase):
			DOCUMENT = \
"""Main Header
===========
Optional Author Name <optional@author.email>
Optional version, optional date
:Author:    AlternativeWayToSetOptional Author Name
:Email:     <AlternativeWayToSetOptional@author.email>
:Date:      AlternativeWayToSetOptional date
:Revision:  AlternativeWayToSetOptional version"""
			def setUp(self):
				with NamedTemporaryFile('wb', delete = False) as f:
					self.inf = f.name
					document = self.DOCUMENT.encode('utf8')
					f.write(document)
				with NamedTemporaryFile(delete = False) as f:
					self.outf = f.name
			def tearDown(self):
				remove(self.inf)
				remove(self.outf)
			def test_asciidoc(self):
				proctype = get_processor('asciidoc-xhtml11')
				proc = proctype('utf8')
				with open(self.outf, 'w+b') as outf:
					with open(self.inf, 'rb') as inf:
						proc(inf, outf)
					outf.seek(0)
					header = proctype.read_header(outf)
					self.assertEqual(header, proc.header)
					# XHTML is XML, so this should work
					document = etree.parse(outf)
					info = document.docinfo
					self.assertEqual(info.public_id.strip(), '-//W3C//DTD XHTML 1.1//EN')
					self.assertEqual(info.system_url, 'http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd')
					self.assertEqual(info.encoding, 'UTF-8')
					self.assertEqual(info.root_name, 'html')

					root = document.getroot()
					self.assertEqual(root.nsmap[None], 'http://www.w3.org/1999/xhtml')
	if 'MarkdownXHTMLProcessor' in vars():
		class TestMarkdown(unittest.TestCase):
			DOCUMENT = \
"""wikiserv
========

Wiki server with manual editing of text files as backend, with a selectable filter
converting them to HTML and a caching mechanism.
"""
			def setUp(self):
				with NamedTemporaryFile('wb', delete = False) as f:
					self.inf = f.name
					document = self.DOCUMENT.encode('utf8')
					f.write(document)
				with NamedTemporaryFile(delete = False) as f:
					self.outf = f.name
			def tearDown(self):
				remove(self.inf)
				remove(self.outf)
			def test_markdown(self):
				proctype = get_processor('markdown-xhtml1')
				proc = proctype('utf8')
				with open(self.outf, 'w+b') as outf:
					with open(self.inf, 'rb') as inf:
						proc(inf, outf)
					outf.seek(0)
					header = proctype.read_header(outf)
					self.assertEqual(header, proc.header)
					# XHTML is XML, so this should work
					document = etree.parse(outf)
					info = document.docinfo
					self.assertEqual(info.public_id.strip(), '-//W3C//DTD XHTML 1.1//EN')
					self.assertEqual(info.system_url, 'http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd')
					self.assertEqual(info.encoding, 'UTF-8')
					self.assertEqual(info.root_name, 'html')

					root = document.getroot()
					self.assertEqual(root.nsmap[None], 'http://www.w3.org/1999/xhtml')
	unittest.main()
