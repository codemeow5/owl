#!/usr/bin/python

import pdb

from tornado import gen
from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection
from tornado.mariadb import MariaDB
from tornado.trie import Trie

class MqttServer(TCPServer):

	def __init__(self):
		TCPServer.__init__(self)
		self.__SUBSCRIBES__ = Trie()
		self.__CONNECTIONS__ = {}
		self.__MESSAGE_ID__ = 0
		MariaDB.current().import_to_memory(self.__SUBSCRIBES__)

	def login(self, connection):
		if connection.client_id is None:
			return False
		original = self.__CONNECTIONS__.get(connection.client_id, None)
		if original is not None:
			if connection.protocol_version == 0x3:
				original.close()
			elif connection.protocol_version == 0x4:
				return False
			else:
				return False
		self.__CONNECTIONS__[connection.client_id] = connection
		result = MariaDB.current().fetch_subscribes(connection.client_id)
		for (topic, qos) in result:
			self.__SUBSCRIBES__.add_subscribe(
				topic, connection.client_id, qos, connection)
		return True

	def clean_session(self, connection):
		self.__CONNECTIONS__.pop(connection.client_id, None)
		if hasattr(connection, 'clean_session'):
			print 'connection.clean_session is %s' % connection.clean_session
		if hasattr(connection, 'clean_session') and not connection.clean_session:
			return
		for topic in connection.subscribes:
			self.__SUBSCRIBES__.remove_subscribe(topic, connection.client_id)

	def get_retain_messages(self, topic):
		nodes = self.__SUBSCRIBES__.matches_sub(topic)
		retain_messages = []
		for node in nodes:
			if node.has_retain_message:
				retain_messages.append(node.get_retain_message())
		return retain_messages

	def subscribe(self, connection, topic, qos):
		if not connection.clean_session:
			execute_result = MariaDB.current().add_subscribe({
				'topic': topic,
				'client_id': connection.client_id,
				'qos': qos})
			if not execute_result:
				return None
		self.__SUBSCRIBES__.add_subscribe(
			topic, connection.client_id, qos, connection)

	def unsubscribe(self, connection, topic):
		if not connection.clean_session:
			execute_result = MariaDB.current().remove_subscribe({
				'topic': topic,
				'client_id': connection.client_id})
			if not execute_result:
				return None
		self.__SUBSCRIBES__.remove_subscribe(topic, connection.client_id)

	@gen.coroutine
	def publish_message(self, message):
		if message is None:
			raise gen.Return(None)
		if message.retain:
			self.__SUBSCRIBES__.set_retain_message(message)
			execute_result = MariaDB.current().add_retain_message(message)
			if not execute_result:
				raise gen.Return(None)
		nodes = self.__SUBSCRIBES__.matches_pub(message.topic)
		for node in nodes:
			clients = node.get_clients()
			for (client_id, client_info) in clients.items():
				connection = client_info.get('connection', None)
				qos_ = client_info.get('qos', 0)
				if qos_ > message.qos:
					qos_ = message.qos
				if connection is None or connection.state <> 'CONNECTED':
					# Offline message
					message_id = self.fetch_message_id()
					MqttConnection.save_offline_message(
						client_id, message_id, qos_, message.topic, message.payload, 0x0)
					continue
				yield connection.send_publish(
					qos_, message.topic, message.payload, 0x0) # TODO
				
	def handle_stream(self, stream, address):
		connection = MqttConnection(self, stream, address)
		connection.wait()

	def fetch_message_id(self):
		# TODO Persistence
		self.__MESSAGE_ID__ = self.__MESSAGE_ID__ + 1
		return self.__MESSAGE_ID__







