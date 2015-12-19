#!/usr/bin/python

import pdb

from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection

class MqttServer(TCPServer):

	def handle_stream(self, stream, address):
		pdb.set_trace()
		if not hasattr(MqttServer, 'connections'):
			MqttServer.connections = []
		connection = MqttConnection(self, stream, address)
		MqttServer.connections.append(connection)
		connection.wait_message()

