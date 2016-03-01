#!/usr/bin/python

import struct
import time
from tornado import gen
from tornado import mqttutil
from tornado.ioloop import IOLoop
from functools import partial
from tornado.mariadb import MariaDB
from tornado.mqttmessage import MqttMessage, BinaryMessage
import pdb

# Constant
RETRY_TIMEOUT = 60 # seconds
RETRY_INTERVAL = 5 # seconds
RETRY_LIMIT = 20
WAIT_CONNECT_TIMEOUT = 60 # seconds

# Message types
CONNECT = 0x10
CONNACK = 0x20
PUBLISH = 0x30
PUBACK = 0x40
PUBREC = 0x50
PUBREL = 0x60
PUBCOMP = 0x70
SUBSCRIBE = 0x80
SUBACK = 0x90
UNSUBSCRIBE = 0xA0
UNSUBACK = 0xB0
PINGREQ = 0xC0
PINGRESP = 0xD0
DISCONNECT = 0xE0

# QoS Level
QoS0 = 0x0
QoS1 = 0x2
QoS2 = 0x4
QoS3 = 0x6

class MqttConnection():

	@gen.coroutine
	def __read_fix_header_byte_1(self, buff):
		pack = {}
		(buff,) = struct.unpack('!B', buff)
		pack['cmd'] = buff
		yield self.__read_remaining_length(pack)

	def __handle_pack(self, pack):
		self.last_alive = self.loop.time()
		message_type = pack['message_type'] = pack.get('cmd') & 0xF0
		if message_type == CONNECT:
			self.__handle_connect(pack)
		elif message_type == PUBLISH:
			self.__handle_publish(pack)
		elif message_type == PUBACK:
			self.__handle_puback(pack)
		elif message_type == PUBREC:
			self.__handle_pubrec(pack)
		elif message_type == PUBREL:
			self.__handle_pubrel(pack)
		elif message_type == PUBCOMP:
			self.__handle_pubcomp(pack)
		elif message_type == SUBSCRIBE:
			self.__handle_subscribe(pack)
		elif message_type == UNSUBSCRIBE:
			self.__handle_unsubscribe(pack)
		elif message_type == PINGREQ:
			self.__handle_pingreq(pack)
		elif message_type == DISCONNECT:
			self.__handle_disconnect(pack)

	def __read_next_string_length(self, buff, offset):
		if len(buff) <= offset:
			return None
		(length,) = struct.unpack('!h', buff[offset:offset + 2])
		return length

	def __read_next_string(self, buff, offset):
		if len(buff) <= offset:
			return (None, offset)
		(buffer_length,) = struct.unpack('!h', buff[offset:offset + 2])
		(content,) = struct.unpack('!%ss' % buffer_length, 
			buff[offset + 2:offset + 2 + buffer_length])
		return (content, offset + 2 + buffer_length)

	def __read_next_buffer(self, buff, offset, length):
		if len(buff) <= offset:
			return (None, offset)
		buff_tuple = struct.unpack('!%sB' % length, buff[offset: offset + length])
		return (buff_tuple, offset + length)

	@gen.coroutine
	def __handle_pingreq(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PINGREQ message, disconnect the client')
			raise gen.Return(None)
		yield self.__send_pingresp()

	@gen.coroutine
	def __send_pingresp(self):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2B', PINGRESP, 0))
		message.message_type = PINGRESP
		yield self.write(message)

	@gen.coroutine
	def __handle_subscribe(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBLISH message, disconnect the client')
			return
		payload_length = pack.get('remaining_length') - 2
		remaining_buffer_format = '!H%ss' % payload_length
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, pack.get('remaining_buffer'))
		message_id = pack['message_id'] = remaining_buffer_tuple[0]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		qoss = []
		retain_messages = []
		offset = 0
		while True:
			(topic, offset) = self.__read_next_string(payload, offset)
			if topic is None:
				break
			((qos,), offset) = self.__read_next_buffer(payload, offset, 1)
			qos = qos << 1
			self.subscribe(topic, qos)
			self.subscribes[topic] = True
			qoss.append(qos)
			retain_messages_ = self.get_retain_messages(topic)
			for retain_message in retain_messages_:
				qos = retain_message.qos if qos > retain_message.qos else qos
				retain_messages.append(
					MqttMessage(retain_message.topic, retain_message.payload, qos))
		yield self.__send_suback(message_id, qoss)
		for retain_message in retain_messages:
			yield self.send_publish(
				retain_message.qos, 
				retain_message.topic, 
				retain_message.payload,
				0x1)

	@gen.coroutine
	def __send_suback(self, message_id, qoss):
		payload = bytearray()
		for qos in qoss:
			payload.extend(struct.pack('!B', qos >> 1))
		remaining_length = 2 + len(payload)
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!B', SUBACK))
		message.buffer.extend(self.__write_remaining_length(remaining_length))
		message.buffer.extend(struct.pack('!H', message_id))
		message.buffer.extend(payload)
		message.message_type = SUBACK
		message.message_id = message_id
		yield self.write(message)

	@gen.coroutine
	def __handle_unsubscribe(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received UNSUBSCRIBE message, disconnect the client')
			raise gen.Return(None)
		payload_length = pack.get('remaining_length') - 2
		remaining_buffer_format = '!H%ss' % payload_length
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, pack.get('remaining_buffer'))
		message_id = pack['message_id'] = remaining_buffer_tuple[0]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		offset = 0
		while True:
			(topic, offset) = self.__read_next_string(payload, offset)
			if topic is None:
				break
			self.unsubscribe(topic)
			self.subscribes.pop(topic, None)
		# TODO response UNSUBACK
		yield self.__send_unsuback(message_id)

	def __clean_session(self):
		self.server.clean_session(self)

	def __clean_raw_session(self):
		self.server.clean_raw_session(self)

	@gen.coroutine
	def __handle_publish(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBLISH message, disconnect the client')
			raise gen.Return(None)
		qos = pack['qos'] = pack.get('cmd') & 0x6
		remaining_buffer = pack.get('remaining_buffer')
		topic_length = self.__read_next_string_length(remaining_buffer, 0)
		variable_header_length = 0
		if qos == QoS0:
			variable_header_length = topic_length + 2
		else:
			variable_header_length = topic_length + 2 + 2
		payload_length = pack.get('remaining_length') - variable_header_length
		remaining_buffer_format = None
		if qos == QoS0:
			remaining_buffer_format = '!H%ss%ss' % (topic_length, payload_length)
		else:
			remaining_buffer_format = '!H%ssH%ss' % (topic_length, payload_length)
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, remaining_buffer)
		topic = pack['topic'] = remaining_buffer_tuple[1]
		message_id = None
		if qos <> QoS0:
			message_id = pack['message_id'] = remaining_buffer_tuple[2]
			message_id = int(message_id)
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		retain = pack['retain'] = pack.get('cmd') & 0x1
		message = MqttMessage(topic, payload, qos, retain)
		if qos == QoS0:
			self.publish_message(message)
			raise gen.Return(None)
		if qos == QoS1:
			self.publish_message(message)
			yield self.__send_puback(message_id)
			raise gen.Return(None)
		if qos == QoS2:
			self.unreleased_messages[message_id] = message
			if not self.clean_session:
				MariaDB.current().add_unreleased_message(
					self.client_id, message_id, message)
			yield self.__send_pubrec(message_id)
			raise gen.Return(None)

	@gen.coroutine
	def __handle_puback(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBACK message, disconnect the client')
			raise gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		message_id = int(message_id)
		handle = self.retry_callbacks.pop(message_id, None)
		if handle is not None:
			self.loop.remove_timeout(handle)
		if not self.clean_session:
			MariaDB.current().remove_outgoing_message(self.client_id, message_id)

	@gen.coroutine
	def __handle_pubrec(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBREC message, disconnect the client')
			raise gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		message_id = int(message_id)
		handle = self.retry_callbacks.pop(message_id, None)
		if handle is not None:
			self.loop.remove_timeout(handle)
		if not self.clean_session:
			MariaDB.current().remove_outgoing_message(self.client_id, message_id)
		yield self.__send_pubrel(message_id)

	@gen.coroutine
	def __handle_pubcomp(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBCOMP message, disconnect the client')
			raise gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		message_id = int(message_id)
		handle = self.retry_callbacks.pop(message_id, None)
		if handle is not None:
			self.loop.remove_timeout(handle)
		if not self.clean_session:
			MariaDB.current().remove_outgoing_message(self.client_id, message_id)

	@gen.coroutine
	def __handle_pubrel(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBREL message, disconnect the client')
			raise gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		message_id = int(message_id)
		message = self.unreleased_messages.pop(message_id, None)
		if not self.clean_session:
			MariaDB.current().remove_unreleased_message(self.client_id, message_id)
		self.publish_message(message)
		print 'PUBCOMP Id is %s' % message_id
		yield self.__send_pubcomp(message_id)

	@gen.coroutine
	def __send_pubrec(self, message_id):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2B', PUBREC, 2))
		message.buffer.extend(struct.pack('!H', message_id))
		message.message_type = PUBREC
		message.message_id = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_pubrel(self, message_id):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2B', PUBREL | QoS1, 2))
		message.buffer.extend(struct.pack('!H', message_id))
		message.qos = QoS1
		message.message_type = PUBREL
		message.message_id = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_puback(self, message_id):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2B', PUBACK, 2))
		message.buffer.extend(struct.pack('!H', message_id))
		message.message_type = PUBACK
		message.message_id = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_pubcomp(self, message_id):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2B', PUBCOMP, 2))
		message.buffer.extend(struct.pack('!H', message_id))
		message.message_type = PUBCOMP
		message.message_id = message_id
		yield self.write(message)

	@classmethod
	def build_publish_message(cls, message_id, qos, topic, payload, retain):
		payload_ = bytearray()
		payload_.extend(struct.pack('!%ss' % len(payload), str(payload)))
		topic_ = bytearray()
		bare_topic_ = struct.pack('!%ss' % len(topic), topic)
		topic_.extend(struct.pack('!H', len(bare_topic_)))
		topic_.extend(bare_topic_)
		variable_header_length = 0
		if qos == QoS0:
			variable_header_length = len(topic_)
		else:
			variable_header_length = len(topic_) + 2
		remaining_buffer_length = variable_header_length + len(payload_)
		message = BinaryMessage()
		byte1 = PUBLISH | qos | retain
		print 'Byte1 is %s' % byte1
		message.buffer.extend(struct.pack('!B', byte1))
		remaining_buffer_length_ = \
			MqttConnection.__write_remaining_length(remaining_buffer_length)
		message.buffer.extend(remaining_buffer_length_)
		message.buffer.extend(topic_)
		if qos <> QoS0:
			message.message_id = message_id
			message.buffer.extend(struct.pack('!H', message.message_id))
		message.buffer.extend(payload_)
		message.qos = qos
		message.message_type = PUBLISH
		return message

	@gen.coroutine
	def send_publish(self, qos, topic, payload, retain):
		message_id = self.server.fetch_message_id()
		message = MqttConnection.build_publish_message(message_id, qos, topic, payload, retain)
		print 'Send Publish(Binary): Buffer is "%s", QoS is %s, Message Type is %s' % \
			(message.buffer, message.qos, message.message_type)
		yield self.write(message)

	@classmethod
	def save_offline_message(cls, client_id, message_id, qos, topic, payload, retain):
		message = MqttConnection.build_publish_message(message_id, qos, topic, payload, retain)
		MariaDB.current().add_outgoing_message(client_id, message)

	@gen.coroutine
	def publish_message(self, message):
		yield self.server.publish_message(message)

	def redis(self):
		return self.server.redis()

	def closeConnection(self, session_id):
		return self.server.closeConnection(session_id)

	def fetchSession(self, client_id):
		return self.server.fetchSession(client_id)

	def login(self):
		return self.server.login(self)

	def subscribe(self, topic, qos):
		return self.server.subscribe(self, topic, qos)

	def unsubscribe(self, topic):
		self.server.unsubscribe(self, topic)

	def get_retain_messages(self, topic):
		return self.server.get_retain_messages(topic)

	@gen.coroutine
	def __handle_disconnect(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received DISCONNECT message, disconnect the client')
			raise gen.Return(None)
		self.received_disconnect = True
		self.close()

	@gen.coroutine
	def __send_unsuback(self, message_id):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!2BH', UNSUBACK, 2, message_id))
		message.message_type = UNSUBACK
		message.message_id = message_id
		yield self.write(message)

	@gen.coroutine
	def __handle_connect(self, pack):
		if self.state == 'CONNECTED':
			self.close('Process a second CONNECT Packet sent from '
				'a Client as a protocol violation and disconnect the Client')
			raise gen.Return(None)
		self.state = 'CONNECTING'
		remaining_buffer = pack.get('remaining_buffer')
		(protocol_name, protocol_name_utf_length) = \
			self.__read_next_string(remaining_buffer, 0)
		protocol_name_length = protocol_name_utf_length - 2
		payload_length = pack.get('remaining_length') - 6 - protocol_name_length
		remaining_buffer_format = '!H%ss2BH%ss' % (protocol_name_length, payload_length)
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, 
			remaining_buffer)
		self.protocol_version = pack['protocol_version'] = remaining_buffer_tuple[2]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		if self.protocol_version <> 0x3 and self.protocol_version <> 0x4:
			# 3.1.1 version require close the Network Connection without
			# sending a CONNACK
			#yield self.__send_connack(0x1)
			self.close('Connection Refused: unacceptable protocol version')
			raise gen.Return(None)
		elif self.protocol_version == 0x3 and protocol_name <> 'MQIsdp':
			yield self.__send_connack(0x1)
			self.close('Connection Refused: unacceptable protocol version')
			raise gen.Return(None)
		elif self.protocol_version == 0x4 and protocol_name <> 'MQTT':
			self.close('Connection Refused: unacceptable protocol version')
			raise gen.Return(None)
		(client_id, offset) = self.__read_next_string(payload, 0)
		if len(client_id) > 23:
			if self.protocol_version == 0x3:
				yield self.__send_connack(0x2)
			self.close('Connection Refused: identifier rejected')
			raise gen.Return(None)
		self.client_id = client_id
		connect_flags = pack['connect_flags'] = remaining_buffer_tuple[3]
		self.will_flag = connect_flags & 0x4 == 0x4
		if self.will_flag:
		# If the Will Flag is set to 1
			self.will_qos = connect_flags & 0x18 >> 3
			self.will_retain = connect_flags & 0x20 >> 5
			(self.will_topic, offset) = self.__read_next_string(payload, offset)
			(self.will_message, offset) = self.__read_next_string(payload, offset)
		if connect_flags & 0x80 == 0x80:
		# If the User name flag is set to 1
			(username, offset) = self.__read_next_string(payload, offset)
			if connect_flags & 0x40 == 0x40:
			# If the Password flag is set to 1
				(password, offset) = self.__read_next_string(payload, offset)
		self.keep_alive = pack['keep_alive'] = remaining_buffer_tuple[4]
		self.clean_session = connect_flags & 0x2 == 0x2
		originalSession = self.fetchSession(client_id)
		if originalSession is not None:
			if self.protocol_version == 0x3:
				self.closeConnection(originalSession)
			elif self.protocol_version == 0x4:
				self.close()
				raise gen.Return(None)
			else:
				self.close()
				raise gen.Return(None)
		#if not self.login():
		#	self.close()
		#	raise gen.Return(None)
		if self.clean_session:
			self.clearSessionState(client_id)
		else:
			self.unreleased_messages = \
				self.redis().fetchUnreleasedMessages(client_id)
		yield self.__send_connack(0x0)
		self.state = 'CONNECTED'
		self.login()
		if self.keep_alive > 0:
			self.keep_alive_callback()
		if not self.clean_session:
			self.__send_offline_message()

	def __send_offline_message(self):
		# Resend offline message
		messages = MariaDB.current().fetch_outgoing_messages(self.client_id)
		for message in messages:
			self.loop.add_callback(self.write, message, False)

	def keep_alive_callback(self):
		now = self.loop.time()
		deadline = self.last_alive + self.keep_alive * 1.5
		if now > deadline:
			self.close()
		else:
			self.keep_alive_handle = self.loop.call_at(deadline, 
				self.keep_alive_callback)

	@gen.coroutine
	def __send_connack(self, code):
		message = BinaryMessage()
		message.buffer.extend(struct.pack('!4B', CONNACK, 2, 0x0, code))
		message.message_type = CONNACK
		yield self.write(message)

	def close(self, error=None):
		self.state = 'CLOSING'
		if error is not None:
			self.error = error
		self.stream.close()

	@gen.coroutine
	def __read_remaining_buffer(self, pack):
		buff = yield self.stream.read_bytes(pack.get('remaining_length'))
		pack['remaining_buffer'] = buff
		self.wait_message() # Waits for the next message
		self.__handle_pack(pack)

	@gen.coroutine
	def __read_remaining_length(self, pack):
		multiplier = 1
		value = 0
		while True:
			digit = yield self.stream.read_bytes(1)
			(digit,) = struct.unpack('!B', digit)
			value += (digit & 127) * multiplier
			multiplier *= 128
			if ((digit & 128) == 0):
				break
		pack['remaining_length'] = value
		yield self.__read_remaining_buffer(pack)

	@classmethod	
	def __write_remaining_length(cls, length):
		packet = bytearray()
		while True:
			digit = length % 128
			length = length / 128
			if length > 0:
				digit = digit | 0x80
			packet.extend(struct.pack('!B', digit))
			if length == 0:
				break
		return packet

	def __close_callback(self):
		# Debug
		print '__close_callback() is called'
		self.state = 'CLOSING'
		self.__clean_session()
		if hasattr(self, 'wait_connect_handle'):
			self.loop.remove_timeout(self.wait_connect_handle)
		if hasattr(self, 'keep_alive_handle'):
			self.loop.remove_timeout(self.keep_alive_handle)
		if hasattr(self, 'retry_callbacks'):
			for (message_id, handle) in self.retry_callbacks.items():
				if handle is not None:
					self.loop.remove_timeout(handle)
		if not hasattr(self, 'received_disconnect') or not self.received_disconnect:
			if hasattr(self, 'will_flag') and self.will_flag:
				message = MqttMessage(
					self.will_topic, 
					self.will_message, 
					self.will_qos, 
					self.will_retain)
				self.publish_message(message)
		self.state = 'CLOSED'

	def __init__(self, server, stream, address):

		self.session_id = mqttutil.gen_session_id()
		self.server = server
		self.stream = stream
		self.address = address
		self.loop = IOLoop.current()
		self.protocol_version = 0x4

		self.unreleased_messages = {}
		self.retry_callbacks = {}
		self.subscribes = {}

		self.state = 'INITIALIZE'
		self.error = None
		self.stream.set_close_callback(self.__close_callback)
		self.client_id = None

	def wait_message(self):
		self.stream.read_bytes(1, self.__read_fix_header_byte_1)


	def wait(self):
		def callback():
			# If the server does not receive a CONNECT message
			# within a reasonable amount of time after the
			# TCP/IP connection is established, the server should
			# close the connection.
			if self.state == 'INITIALIZE':
				self.close('Does not receive a CONNECT message within a '
					'reasonable amount of time after the TCP/IP '
					'connection is established')
		self.wait_connect_handle = self.loop.call_later(
			WAIT_CONNECT_TIMEOUT, callback)
		# TODO Keep Alive timer not implemented
		self.wait_message()

	@gen.coroutine
	def __retry(self, message):
		if message.message_id is None:
			raise gen.Return(None)
		if message.buffer is None:
			self.retry_callbacks.pop(message.message_id, None)
			raise gen.Return(None)
		message.buffer[0] = message.buffer[0] | 0x8 # DUP flag set 1
		buffstr = str(message.buffer)
		try:
			yield self.stream.write(buffstr)
		finally:
			message.retry = message.retry + 1
			if message.retry > RETRY_LIMIT:
				self.retry_callbacks.pop(message.message_id, None)
				raise gen.Return(None)
			delay = RETRY_TIMEOUT + message.retry * RETRY_INTERVAL
			handle = self.loop.call_later(delay, self.__retry, message)
			self.retry_callbacks[message.message_id] = handle

	@gen.coroutine
	def write(self, message, persistence=True):
		"""Message format
		{
			'buffer': 'Binary packet',
			'qos': 'QoS level(Optional)',
			'message_type': 'Message Type',
			'message_id': 'Message Id(Optional)',
			'retry': 'Deliver retry time(default is 0)'
		}
		"""
		if persistence and not self.clean_session \
			and message.qos > 0 and message.message_id is not None:
			MariaDB.current().add_outgoing_message(self.client_id, message)
		buffstr = str(message.buffer)
		try:
			yield self.stream.write(buffstr)
		finally:
			if message.qos == 0:
				raise gen.Return(None)
			if message.message_id is None:
				raise gen.Return(None)
			handle = self.loop.call_later(RETRY_TIMEOUT, self.__retry, message)
			self.retry_callbacks[message.message_id] = handle






