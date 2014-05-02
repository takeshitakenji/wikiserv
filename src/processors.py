#!/usr/bin/env python2
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

from os import environ, getuid
import struct, platform, os, stat
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



if __name__ == '__main__':
	import unittest

	logging.basicConfig(level = logging.DEBUG)
	class TestPath(unittest.TestCase):
		def test_executable(self):
			if platform.system() == 'Windows':
				path = find_executable('cmd')
				self.assertIsNotNone(path)
			else:
				path = find_executable('sh')
				self.assertIsNotNone(path)
	unittest.main()
