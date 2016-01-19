#!/usr/bin/python

import pdb

from tornado import gen
from tornado.tcpserver import TCPServer
from tornado.mqttconnection import MqttConnection

class MqttServer(TCPServer):

	def __init__(self):
		TCPServer.__init__(self)
		self.__SUBSCRIBES__ = {}
		self.__CONNECTIONS__ = {}
		self.__MESSAGE_ID__ = 0
		# The structure of self.__SUBSCRIBES__ is shown below 
		# { 
		# 	topic: {
		# 		clients: {
		# 			client_id: {
		# 				connection: MqttConnection, 
		# 				qos: QoS Level
		# 			}
		# 		}, 
		# 		retain_message: {
		# 			qos: QoS Level,
		# 			payload: Payload
		# 		}
		# 	} 
		# }
		# Value of client_id is None or state of MqttConnection is
		# CLOSED meant that the client not logged in

	def register(self, connection):
		self.__CONNECTIONS__[connection.client_id] = connection

	def unregister(self, connection):
		self.__CONNECTIONS__.pop(connection.client_id, None)

	def clean_session(self, connection):
		pdb.set_trace()
		self.unregister(connection)
		for topic in connection.subscribes:
			topic_context = self.__SUBSCRIBES__.get(topic, None)
			if topic_context is None:
				continue
			clients = topic_context.get('clients', None)
			if clients is None:
				continue
			clients.pop(connection.client_id, None)

	def subscribe(self, connection, topic, qos):
		# TODO handle qos
		client_id = connection.client_id
		topic_context = self.__SUBSCRIBES__.get(topic, None)
		if topic_context is None:
			topic_context = self.__SUBSCRIBES__[topic] = {}
		clients = topic_context.get('clients', None)
		if clients is None:
			clients = topic_context['clients'] = {}
		clients[client_id] = {
			'connection': connection,
			'qos': qos
			}
		retain_message = topic_context.get('retain_message', None)
		pdb.set_trace()
		# TODO Persistence
		# QoS level possible downgrade
		return (topic, qos, retain_message)

	def unsubscribe(self, connection, topic):
		if not topic in self.__SUBSCRIBES__:
			return
		topic_context = self.__SUBSCRIBES__.get(topic, None)
		if topic_context is None:
			return
		clients = topic_context.get('clients', None)
		if clients is not None:
			clients.pop(connection.client_id, None)
		# TODO persistence

	@gen.coroutine
	def deliver(self, delivery):
		if delivery is None:
			gen.Return(None)
		topic = delivery.get('topic', None)
		if topic is None:
			gen.Return(None)
		qos = delivery.get('qos', None)
		if qos is None:
			gen.Return(None)
		retain = delivery.get('retain', 0)
		payload = delivery.get('payload', None)
		# TODO calculate topic wildcards
		topics = self.wildcards(topic)
		if retain:
			topic_context = self.__SUBSCRIBES__.get(topic, None)
			if topic_context is None:
				topic_context = self.__SUBSCRIBES__[topic] = {}
			topic_context['retain_message'] = delivery
			pdb.set_trace()
		for topic_ in topics:
			topic_context = self.__SUBSCRIBES__.get(topic_, None)
			if topic_context is None:
				continue
			clients = topic_context.get('clients', None)
			if clients is None:
				continue
			for (client_id, client_info) in clients.items():
				connection = client_info.get('connection', None)
				qos_ = client_info.get('qos', 0)
				if connection is None or connection.state <> 'CONNECTED':
					continue
				if qos_ < qos:
					qos = qos_
				yield connection.send_publish(qos, topic, payload, 0x0) # TODO

	def wildcards(self, topic):
		"""Calculate topic wildcards
		"""
		sections = topic.split('/')
		length = len(sections)
		topics = []
		pre_level = ['']
		for level in range(length + 1):
			section = None
			if level < length:
				section = sections[level]
			is_last_section = False
			if level + 1 == length:
				is_last_section = True
			pre_level_ = []
			for s in pre_level:
				if section is not None:
					new_topic = s + '/' + section if len(s) > 0 else section
					if is_last_section:
						topics.append(new_topic)
						pre_level_.append(new_topic)
					else:
						pre_level_.append(new_topic)
				new_topic = s + '/' + '#' if len(s) > 0 else '#'
				topics.append(new_topic)
				new_topic = s + '/' + '+' if len(s) > 0 else '+'
				if is_last_section:
					topics.append(new_topic)
					pre_level_.append(new_topic)
				else:
					pre_level_.append(new_topic)
				pre_level = pre_level_
		return topics

	def handle_stream(self, stream, address):
		connection = MqttConnection(self, stream, address)
		connection.wait()

	def fetch_message_id(self):
		self.__MESSAGE_ID__ = self.__MESSAGE_ID__ + 1
		return self.__MESSAGE_ID__







