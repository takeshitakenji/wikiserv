#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')
import os, stat


allbits = stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO
dir_perms = stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR
file_perms = stat.S_IRUSR|stat.S_IWUSR


def fix_dir_perms(path):
	info = os.stat(path)
	if (info.st_mode & allbits) != dir_perms:
		os.chmod(path, dir_perms)
def fix_perms(handle):
	try:
		info = os.fstat(handle.fileno())
		if (info.st_mode & allbits) != file_perms:
			os.fchmod(handle.fileno(), file_perms)
	except AttributeError:
		# handle is a path
		info = os.stat(handle)
		if (info.st_mode & allbits) != file_perms:
			os.chmod(handle, file_perms)
