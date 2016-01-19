#!/usr/bin/python

import struct
import time
from tornado import gen
from tornado.ioloop import IOLoop
from functools import partial
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
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2B', PINGRESP, 0))
		message['message_type'] = PINGRESP
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
		offset = 0
		while True:
			(topic, offset) = self.__read_next_string(payload, offset)
			if topic is None:
				break
			((qos,), offset) = self.__read_next_buffer(payload, offset, 1)
			qoss.append(self.subscribe(topic, qos))
			self.subscribes[topic] = True
		# TODO response SUBACK
		yield self.__send_suback(message_id, qoss)

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
		# TODO reply
		if qos == QoS0:
			self.deliver(pack)
			raise gen.Return(None)
		if qos == QoS1:
			self.deliver(pack)
			yield self.__send_puback(message_id)
			raise gen.Return(None)
		if qos == QoS2:
			# TODO response PUBREC
			self.unreleased_deliveries[message_id] = pack
			yield self.__send_pubrec(message_id)
			raise gen.Return(None)

	@gen.coroutine
	def __handle_puback(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBACK message, disconnect the client')
			raise gen.Return(None)
		# TODO what to do?
		pass

	@gen.coroutine
	def __handle_pubrec(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBREC message, disconnect the client')
			raise gen.Return(None)
		yield self.__send_pubrel(message_id)

	@gen.coroutine
	def __handle_pubcomp(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBCOMP message, disconnect the client')
			raise gen.Return(None)
		# TODO what to do?
		pass

	@gen.coroutine
	def __handle_pubrel(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received PUBREL message, disconnect the client')
			raise gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		message_id = int(message_id)
		pack = self.unreleased_deliveries.get(message_id, None)
		delivery = self.unreleased_deliveries.pop(message_id, None)
		self.deliver(delivery)
		yield self.__send_pubcomp(message_id)

	@gen.coroutine
	def __send_pubrec(self, message_id):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2B', PUBREC, 2))
		b.extend(struct.pack('!H', message_id))
		message['message_type'] = PUBREC
		message['message_id'] = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_pubrel(self, message_id):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2B', PUBREL | QoS1, 2))
		b.extend(struct.pack('!H', message_id))
		message['qos'] = QoS1
		message['message_type'] = PUBREL
		message['message_id'] = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_puback(self, message_id):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2B', PUBACK, 2))
		b.extend(struct.pack('!H', message_id))
		message['message_type'] = PUBACK
		message['message_id'] = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_pubcomp(self, message_id):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2B', PUBCOMP, 2))
		b.extend(struct.pack('!H', message_id))
		message['message_type'] = PUBCOMP
		message['message_id'] = message_id
		yield self.write(message)

	@gen.coroutine
	def send_publish(self, qos, topic, payload, retain):
		payload_ = bytearray()
		payload_.extend(struct.pack('!%ss' % len(payload), payload))
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
		message = {}
		b = message['b'] = bytearray()
		byte1 = PUBLISH | qos | retain
		b.extend(struct.pack('!B', byte1))
		remaining_buffer_length_ = self.__write_remaining_length(remaining_buffer_length)
		b.extend(remaining_buffer_length_)
		b.extend(topic_)
		if qos <> QoS0:
			message['message_id'] = message_id = self.server.fetch_message_id()
			b.extend(struct.pack('!H', message_id))
		b.extend(payload_)
		message['qos'] = qos
		message['message_type'] = PUBLISH
		yield self.write(message)

	@gen.coroutine
	def deliver(self, delivery):
		yield self.server.deliver(delivery)

	def subscribe(self, topic, qos):
		return self.server.subscribe(self, topic, qos)

	def unsubscribe(self, topic):
		self.server.unsubscribe(self, topic)

	@gen.coroutine
	def __handle_disconnect(self, pack):
		if self.state <> 'CONNECTED':
			self.close('Client must first sent a CONNECT message, '
				'now received DISCONNECT message, disconnect the client')
			raise gen.Return(None)
		self.close()

	@gen.coroutine
	def __send_unsuback(self, message_id):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!2BH', UNSUBACK, 2, message_id))
		message['message_type'] = UNSUBACK
		message['message_id'] = message_id
		yield self.write(message)

	@gen.coroutine
	def __send_suback(self, message_id, qoss):
		payload = bytearray()
		for (topic, qos, retain_message) in qoss:
			payload.extend(struct.pack('!B', qos))
		remaining_length = 2 + len(payload)
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!B', SUBACK))
		b.extend(self.__write_remaining_length(remaining_length))
		b.extend(struct.pack('!H', message_id))
		b.extend(payload)
		message['message_type'] = SUBACK
		message['message_id'] = message_id
		self.write(message, partial(self.__suback_callback, qoss))

	def __suback_callback(self, qoss):
		for (topic, qos, retain_message) in qoss:
			if retain_message is None:
				continue
			qos_ = retain_message.get('qos', 0)
			if qos < qos_:
				qos_ = qos
			payload = retain_message.get('payload', None)
			yield self.send_publish(qos_, topic, payload, 0x1)

	@gen.coroutine
	def __handle_connect(self, pack):
		self.state = 'CONNECTING'
		payload_length = pack.get('remaining_length') - 12
		remaining_buffer_format = '!H6s2BH%ss' % payload_length
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, pack.get('remaining_buffer'))
		protocol_version = pack['protocol_version'] = remaining_buffer_tuple[2]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		if protocol_version <> 0x3:
			yield self.__send_connack(0x1)
			self.close('Connection Refused: unacceptable protocol version')
			raise gen.Return(None)
		(client_id, offset) = self.__read_next_string(payload, 0)
		if len(client_id) > 23:
			yield self.__send_connack(0x2)
			self.close('Connection Refused: identifier rejected')
			raise gen.Return(None)
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
		self.clean_session = connect_flags & 0x1 == 0x1
		self.keep_alive = pack['keep_alive'] = remaining_buffer_tuple[4]
		yield self.__send_connack(0x0)
		self.state = 'CONNECTED'
		self.client_id = client_id
		if self.keep_alive > 0:
			self.keep_alive_callback()
		self.server.register(self)

	def keep_alive_callback(self):
		now = self.loop.time()
		deadline = self.last_alive + self.keep_alive * 1.5
		if now > deadline:
			self.close()
		else:
			self.keep_alive_handle = self.loop.call_at(deadline, self.keep_alive_callback)

	@gen.coroutine
	def __send_connack(self, code):
		message = {}
		b = message['b'] = bytearray()
		b.extend(struct.pack('!4B', CONNACK, 2, 0x0, code))
		message['message_type'] = CONNACK
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
	
	def __write_remaining_length(self, length):
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
		self.state = 'CLOSING'
		if hasattr(self, 'wait_connect_handle'):
			self.loop.remove_timeout(self.wait_connect_handle)
		if hasattr(self, 'keep_alive_handle'):
			self.loop.remove_timeout(self.keep_alive_handle)
		if hasattr(self, 'clean_session') and self.clean_session:
			self.server.clean_session(self)
		if self.stream.error is not None or self.error is not None:
			if hasattr(self, 'will_flag') and self.will_flag:
				self.deliver({
					'topic': self.will_topic,
					'qos': self.will_qos,
					'payload': self.will_message,
					'retain': self.will_retain
					})
		self.state = 'CLOSED'

	def __init__(self, server, stream, address):

		self.server = server
		self.stream = stream
		self.address = address
		self.loop = IOLoop.current()

		# Unreleased Deliveries 
		# Key: Message Id
		# Value: Delivery(Include topic, qos, payload)
		self.unreleased_deliveries = {}
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
		message_id = message.get('message_id', None)
		if message_id is None:
			raise gen.Return(None)
		pack = message.get('b', None)
		if pack is None:
			del self.retry_callbacks[message_id]
			raise gen.Return(None)
		pack[0] = pack[0] | 0x8 # DUP flag set 1
		pack = str(pack)
		try:
			yield self.stream.write(pack)
		finally:
			retry = message.get('retry', 0) + 1
			if retry > RETRY_LIMIT:
				del self.retry_callbacks[message_id]
				raise gen.Return(None)
			message['retry'] = retry
			delay = RETRY_TIMEOUT + retry * RETRY_INTERVAL
			handle = self.loop.call_later(delay, self.__retry, message)
			self.retry_callbacks[message_id] = handle

	def write(self, message, callback=None):
		"""Message format
		{
			'b': 'Binary packet',
			'qos': 'QoS level(Optional)',
			'message_type': 'Message Type',
			'message_id': 'Message Id(Optional)',
			'retry': 'Deliver retry time(default is 0)'
		}
		"""
		future = None
		pack = message.get('b', None)
		if pack is None:
			raise gen.Return(None)
		pack = str(pack)
		try:
			future = self.stream.write(pack, callback)
		finally:
			qos = message.get('qos', 0)
			if qos == 0:
				raise gen.Return(None)
			message_id = message.get('message_id', None)
			if message_id is None:
				raise gen.Return(None)
			handle = self.loop.call_later(RETRY_TIMEOUT, self.__retry, message)
			self.retry_callbacks[message_id] = handle
		return future






