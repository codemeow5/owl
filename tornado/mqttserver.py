#!/usr/bin/python

import socket
from functools import partial
import pdb

from tornado.tcpserver import TCPServer

class MqttServer(TCPServer):

	def handle_read(self, buff, stream):
		pdb.set_trace()
		print buff
		callback = partial(self.handle_read, stream=stream)
		stream.read_until("0", callback)

	def handle_stream(self, stream, address):
		pdb.set_trace()
		if not hasattr(MqttServer, 'streams'):
			MqttServer.streams = []
		MqttServer.streams.append(stream)
		callback = partial(self.handle_read, stream=stream)
		stream.read_until("0", callback)
		
