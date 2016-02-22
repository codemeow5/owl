#!/usr/bin/python

import pdb

class MqttMessage():

	def __init__(self, topic, payload, qos=0, retain=0x0):
		if topic is None or len(topic) == 0:
			raise Exception('Topic can not be empty')
		self.topic = topic
		if qos is None or qos < 0:
			raise Exception('Invalid QoS flag format')
		self.qos = qos
		if retain is None:
			raise Exception('Invalid Retain flag format')
		self.retain = retain
		self.payload = payload

class BinaryMessage():

	def __init__(self, buffer=None, message_type=None, qos=0, retry=0, message_id=None):
		if buffer is None:
			buffer = bytearray()
		self.buffer = buffer
		self.message_type = message_type
		self.qos = qos
		self.retry = retry
		self.message_id = message_id

