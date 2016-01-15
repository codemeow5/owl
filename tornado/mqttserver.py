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
		#	topic: { 
		#		client_id: (MqttConnection, qos) 
		#	} 
		# }
		# Value of client_id is None or state of MqttConnection is
		# CLOSED meant that the client not logged in

	def register(self, connection):
		self.__CONNECTIONS__[connection.client_id] = connection

	def subscribe(self, connection, topic, qos):
		# TODO handle qos
		client_id = connection.client_id
		subscribers = None
		if not topic in self.__SUBSCRIBES__:
			subscribers = {}
			self.__SUBSCRIBES__[topic] = subscribers # TODO match topics
		else:
			subscribers = self.__SUBSCRIBES__.get(topic)
		subscribers[client_id] = (connection, qos)
		# TODO persistence
		return qos # possible downgrade

	def unsubscribe(self, connection, topic):
		client_id = connection.client_id
		if topic in self.__SUBSCRIBES__:
			subscribers = self.__SUBSCRIBES__.get(topic)
			subscribers.pop(client_id, None)
			if len(subscribers) == 0:
				self.__SUBSCRIBES__.pop(topic, None)
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
		payload = delivery.get('payload', None)
		# TODO calculate topic wildcards
		topics = self.wildcards(topic)
		for topic_ in topics:
			subscribers = self.__SUBSCRIBES__.get(topic_, None)
			if subscribers is None:
				continue
			for (client_id, (connection, qos_)) in subscribers.items():
				if connection is None or connection.state <> 'CONNECTED':
					continue
				message_id = self.fetch_message_id()
				if qos_ < qos:
					qos = qos_
				yield connection.send_publish(qos, 0, topic, message_id, payload) # TODO

	def wildcards(topic):
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
		pdb.set_trace()
		connection = MqttConnection(self, stream, address)
		connection.wait()

	def fetch_message_id(self):
		self.__MESSAGE_ID__ = self.__MESSAGE_ID__ + 1







