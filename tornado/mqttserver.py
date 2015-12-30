#!/usr/bin/python

import pdb

from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection

class MqttServer(TCPServer):

	def __init__(self):
		TCPServer.__init__(self)
		self.__TOPICS__ = {}
		self.__CONNECTIONS__ = {}
		# The structure of self.__TOPICS__ is shown below 
		# { topic: { client_id: MqttConnection } }
		# Value of client_id is None or state of MqttConnection is
		# CLOSED meant that the client not logged in

	def register(self, connection):
		self.__CONNECTIONS__[connection.client_id] = connection

	def subscribe(self, connection, topic, qos):
		# TODO handle qos
		client_id = connection.client_id
		subs_dict = None
		if not topic in self.__TOPICS__:
			subs_dict = {}
			self.__TOPICS__[topic] = subs_dict
		else:
			subs_dict = self.__TOPICS__.get(topic)
		subs_dict[client_id] = connection
		# TODO persistence
		return qos # possible downgrade

	def unsubscribe(self, connection, topic):
		client_id = connection.client_id
		if topic in self.__TOPICS__:
			subs_dict = self.__TOPICS__.get(topic)
			subs_dict.pop(client_id)
			if len(subs_dict) == 0:
				self.__TOPICS__.pop(topic)
		# TODO persistence

	def publish(self, topic, message):
		pass

	def handle_stream(self, stream, address):
		pdb.set_trace()
		connection = MqttConnection(self, stream, address)
		connection.wait_message()

