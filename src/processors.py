#!/usr/bin/env python2
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

from os import environ, getuid
import struct, platform, os, stat
from collections import namedtuple
from os.path import pathsep, join as path_join, normpath, isfile, basename
import logging


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
		groups = set((g.gr_gid for g in grp.getgrall() if current_user.pw_name in g.gr_mem))
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




class Processor(object):
	processors = {}
	NAME = NotImplemented
	MIME = NotImplemented

	Header = namedtuple('Header', ['encoding', 'mime'])
	length_format = '!B'
	length_length = 1

	@classmethod
	def register(cls):
		if any((x is NotImplemented for x in [cls.NAME, cls.MIME])):
			raise RuntimeError('Class %s is not set up properly' % cls)
		self.processors[cls.NAME] = cls
	
	def __init__(self, encoding):
		if len(self.MIME) > 0xFF:
			raise ValueError('MIME type is too long: %s' % self.MIME)
		if len(encoding) > 0xFF:
			raise ValueError('Character encoding is too long: %s' % self.encoding)
		# Verify they are ASCII
		self.MIME.encode('ascii')
		encoding.encode('ascii')
		b''.decode(encoding)
		
		self.header = self.Header(self.MIME, encoding)
	def write_header(self, stream):
		encoding = self.header.encoding.encode('ascii')
		count = stream.write(struct.pack(self.length_format, len(encoding)))
		count += stream.write(encoding)

		mime = self.header.mime.encode('ascii')
		count += stream.write(struct.pack(self.length_format, len(mime)))
		count += stream.write(mime)
		return count
	@classmethod
	def read_header(cls, stream):
		length, = struct.unpack(cls.length_format, stream.read(cls.length_length))
		encoding = stream.read(length).decode('ascii')

		length, = struct.unpack(cls.length_format, stream.read(cls.length_length))
		mime = stream.read(length).decode('ascii')

		return cls.Header(encoding, mime)
	def process(self, inf, outf):
		raise NotImplementedError
	def __call__(self, inf, outf):
		self.write_header(outf)
		return self.process(inf, outf)


if __name__ == '__main__':
	import unittest
	from tempfile import NamedTemporaryFile
	from os import remove

	logging.basicConfig(level = logging.DEBUG)
	class TestPath(unittest.TestCase):
		def test_executable(self):
			if platform.system() == 'Windows':
				path = find_executable('cmd')
				self.assertIsNotNone(path)
			else:
				path = find_executable('sh')
				self.assertIsNotNone(path)
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
				processor.write_header(f)
				f.write(text.encode('utf8'))
			try:
				with open(name, 'rb') as f:
					header = self.FakeProcessor.read_header(f)
					ftext = f.read().decode('utf8')
				self.assertEqual(header, processor.header)
				self.assertEqual(text, ftext)
			finally:
				remove(name)
	unittest.main()
