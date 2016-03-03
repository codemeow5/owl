#!/usr/bin/python

import pdb

from tornado import gen
from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection
from tornado.redis_ import RedisStorage
from tornado.mqttsession import MqttSessionStorage

class MqttServer(TCPServer):

	def __init__(self):
		TCPServer.__init__(self)
		#self.__SUBSCRIBES__ = Trie()
		#self.__CONNECTIONS__ = {}
		self.__SESSIONS__ = MqttSessionStorage()
		self.__MESSAGE_ID__ = 0
		self.__REDIS__ = RedisStorage()
		#MariaDB.current().import_to_memory(self.__SUBSCRIBES__)

	def redis(self):
		return self.__REDIS__

	def closeConnection(self, session_id):
		# TODO Close remote connection
		# Temp
		connection = self.__SESSIONS__.get(session_id=session_id)
		if connection is not None:
			connection.close()
		pass

	def fetchSession(self, client_id):
		return self.__REDIS__.fetchSession(client_id)

	def saveSession(self, connection):
		client_id = connection.client_id
		if client_id is None or len(client_id) == 0:
			raise Exception('Missing argument \'client_id\'')
		self.__SESSIONS__.save(connection)
		
		# TODO Check whether the client has been log in the cluster
		#original = self.__CONNECTIONS__.get(connection.client_id, None)
		#if original is not None:
		#	if connection.protocol_version == 0x3:
		#		original.close()
		#	elif connection.protocol_version == 0x4:
		#		return False
		#	else:
		#		return False
		#self.__CONNECTIONS__[connection.client_id] = connection
		# TODO Nothing to do
		#result = MariaDB.current().fetch_subscribes(connection.client_id)
		#for (topic, qos) in result:
		#	connection.subscribes[topic] = True
		#	self.__SUBSCRIBES__.add_subscribe(
		#		topic, connection.client_id, qos, connection)
		#return True

	def cleanSession(self, client_id):
		self.__SESSIONS__.remove(client_id=client_id)

	def cleanSessionState(self, client_id):
		self.__REDIS__.clearSubscription(client_id)
		self.__REDIS__.clearUnreleasedMessages(client_id)
		self.__REDIS__.clearOutgoingMessages(client_id)

	#def clean_raw_session(self, connection):
	#	result = MariaDB.current().fetch_subscribes(connection.client_id)
	#	for (topic, qos) in result:
	#		self.__SUBSCRIBES__.remove_subscribe(topic, connection.client_id)
	#	MariaDB.current().remove_subscribes(connection.client_id)
	#	MariaDB.current().remove_unreleased_messages(connection.client_id)
	#	MariaDB.current().remove_outgoing_messages(connection.client_id)

	#def clean_session(self, connection):
	#	self.__CONNECTIONS__.pop(connection.client_id, None)
	#	if hasattr(connection, 'clean_session') and not connection.clean_session:
	#		return
	#	for topic in connection.subscribes:
	#		self.__SUBSCRIBES__.remove_subscribe(topic, connection.client_id)

	#def get_retain_messages(self, topic):
	#	nodes = self.__SUBSCRIBES__.matches_sub(topic)
	#	retain_messages = []
	#	for node in nodes:
	#		if node.has_retain_message:
	#			retain_messages.append(node.get_retain_message())
	#	return retain_messages

	def subscribe(self, connection, topic, qos):
		# remove clean_session
		self.__REDIS__.addSubscription(
			topic, connection.client_id, qos)
			#execute_result = MariaDB.current().add_subscribe({
			#	'topic': topic,
			#	'client_id': connection.client_id,
			#	'qos': qos})
			#if not execute_result:
			#	return None
		#self.__SUBSCRIBES__.add_subscribe(
		#	topic, connection.client_id, qos, connection)

	def unsubscribe(self, connection, topic):
		# remove clean_session
		self.__REDIS__.removeSubscription(topic, connection.client_id)
			#execute_result = MariaDB.current().remove_subscribe({
			#	'topic': topic,
			#	'client_id': connection.client_id})
			#if not execute_result:
			#	return None
		#self.__SUBSCRIBES__.remove_subscribe(topic, connection.client_id)

	@gen.coroutine
	def publish_message(self, message):
		if message is None:
			raise gen.Return(None)
		if message.retain:
			self.__REDIS__.setRetainMessage(message)
			#self.__SUBSCRIBES__.set_retain_message(message)
			#execute_result = MariaDB.current().add_retain_message(message)
			#if not execute_result:
			#	raise gen.Return(None)
		#nodes = self.__SUBSCRIBES__.matches_pub(message.topic)
		topics = self.__REDIS__.matchPublish(message.topic)
		for topic in topics:
			#clients = node.get_clients()
			clients = self.__REDIS__.fetchSubClients(topic)
			for (client_id, qos_) in clients.items(): 
				if qos_ > message.qos:
					qos_ = message.qos
				session_id = self.__REDIS__.fetchSession(client_id)
				if session_id is None:
					# Offline message
					message_id = self.fetch_message_id()
					self.save_offline_message(
						client_id, message_id, qos_, message.topic, message.payload, 0x0)
				else:
					# TODO Send message from remote Broker
					# Temp
					connection = self.__SESSIONS__.get(session_id=session_id)
					if connection is not None:
						connection.send_publish(
							qos_, message.topic, message.payload, 0x0)
					
			#for (client_id, client_info) in clients.items():
			#	connection = client_info.get('connection', None)
			#	qos_ = client_info.get('qos', 0)
			#	if qos_ > message.qos:
			#		qos_ = message.qos
			#	if connection is None or connection.state <> 'CONNECTED':
			#		# Offline message
			#		message_id = self.fetch_message_id()
			#		MqttConnection.save_offline_message(
			#			client_id, message_id, qos_, message.topic, message.payload, 0x0)
			#		continue
			#	print 'Send Publish: QoS is %s, Topic is %s, Payload is %s' % \
			#		(qos_, message.topic, message.payload)
			#	yield connection.send_publish(
			#		qos_, message.topic, message.payload, 0x0) # TODO

        def save_offline_message(self, client_id, message_id, qos, topic, payload, retain):
                message = MqttConnection.build_publish_message(message_id, qos, topic, payload, retain)
                #MariaDB.current().add_outgoing_message(client_id, message)
                self.__REDIS__.addOutgoingMessage(client_id, message)
				
	def handle_stream(self, stream, address):
		connection = MqttConnection(self, stream, address)
		connection.wait()

	def fetch_message_id(self):
		# TODO Persistence
		self.__MESSAGE_ID__ = self.__MESSAGE_ID__ + 1
		return self.__MESSAGE_ID__







