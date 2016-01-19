#!/usr/bin/python

import sys, os
reload(sys)
sys.setdefaultencoding('utf8')
import tornado.ioloop
import tornado.web
from tornado.mqttserver import MqttServer

if __name__ == '__main__':
	server = MqttServer()
	#server.bind(8888)
	#server.start(0)
	server.listen(8888)
	tornado.ioloop.IOLoop.current().start()
