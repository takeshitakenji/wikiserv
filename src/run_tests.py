#!/usr/bin/env python3
import sys
if sys.version_info < (3, 0):
	raise RuntimeError('At least Python 3.0 is required')

from glob import glob
from os.path import abspath
from subprocess import check_call

abs_this = abspath(__file__)


def tail(it, n = 10):
	buff = []
	for line in it:
		if len(buff) >= n:
			buff.pop(0)
		buff.append(line)
	return buff

for pyscript in glob('*.py'):
	pyscript = abspath(pyscript)
	if pyscript == abs_this:
		continue
	with open(pyscript, 'r') as f:
		check = [line for line in (line.strip() for line in tail(f)) if line]
	print(check)
	if check and any(('unittest.main()' in line for line in check)):
		print('=' * 72)
		print('Running %s' % pyscript)
		print('=' * 72)
		check_call(('python3', pyscript))
