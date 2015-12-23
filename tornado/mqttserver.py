#!/usr/bin/python

import pdb

from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection

class MqttServer(TCPServer):

	def __init__(self):
		self.__topic_dict = {}
		# The structure of self.__topic_dict is shown below 
		# { topic: { client_id: MqttConnection } }
		# Value of client_id is None or state of MqttConnection is
		# CLOSED meant that the client not logged in

	def subscribe(self, connection, topic, qos):
		# TODO handle qos
		client_id = connection.client_id
		subs_dict = None
		if not topic in self.__topic_dict:
			subs_dict = {}
			self.__topic_dict[topic] = subs_dict
		else:
			subs_dict = self.__topic_dict.get(topic)
		subs_dict[client_id] = connection
		# TODO persistence

	def unsubscribe(self, connection, topic):
		client_id = connection.client_id
		if topic in self.__topic_dict:
			subs_dict = self.__topic_dict.get(topic)
			subs_dict.pop(client_id)
			if len(subs_dict) == 0:
				self.__topic_dict.pop(topic)
		# TODO persistence

	def publish(self, topic, message):
		pass

	def handle_stream(self, stream, address):
		pdb.set_trace()
		if not hasattr(MqttServer, 'connections'): # TODO cstree
			MqttServer.connections = []
		connection = MqttConnection(self, stream, address)
		MqttServer.connections.append(connection)
		connection.wait_message()

