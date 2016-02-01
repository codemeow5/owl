#!/usr/bin/python

from tornado.ioloop import IOLoop
from tornado.mqttserver import MqttServer

if __name__ == '__main__':
	server = MqttServer()
	#server.bind(8888)
	#server.start(0)
	server.listen(8888)
	IOLoop.current().start()
