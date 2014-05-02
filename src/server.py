#!/usr/bin/env python3
import sys
if sys.version_info < (3, 3):
	raise RuntimeError('At least Python 3.3 is required')

import tornado.ioloop
import tornado.web
import logging


LOGGER = logging.getLogger(__name__)

class WikiHandler(tornado.web.RequestHandler):
	pass

application = tornado.web.Application([
	(r'/', WikiHandler),
])

if __name__ == '__main__':
	pass
	#application.listen(8888)
	#tornado.ioloop.IOLoop.instance().start()
