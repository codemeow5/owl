#!/usr/bin/python

import struct
from tornado import gen
from tornado.ioloop import IOLoop
from functools import partial
import pdb

# Constant
WAIT_TIME = 60

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
		message_type = pack['message_type'] = pack.get('cmd') & 0xF0
		if message_type == CONNECT:
			self.__handle_connect(pack)
			return
		if message_type == PUBLISH:
			self.__handle_publish(pack)
			return
		if message_type == PUBACK:
			self.__handle_puback(pack)
			return
		if message_type == PUBREC:
			self.__handle_pubrec(pack)
			return
		if message_type == PUBREL:
			self.__handle_pubrel(pack)
			return
		if message_type == PUBCOMP:
			self.__handle_pubcomp(pack)
			return
		if message_type == SUBSCRIBE:
			self.__handle_subscribe(pack)
			return
		if message_type == UNSUBSCRIBE:
			self.__handle_unsubscribe(pack)
			return
		if message_type == PINGREQ:
			self.__handle_pingreq(pack)
			return
		if message_type == DISCONNECT:
			self.__handle_disconnect(pack)
			return

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
		yield self.__send_pingresp()

	@gen.coroutine
	def __send_pingresp(self):
		packet = bytearray()
		packet.extend(struct.pack('!2B', PINGRESP, 0))
		packet = str(packet)
		yield self.stream.write(packet)

	@gen.coroutine
	def __handle_subscribe(self, pack):
		pdb.set_trace()
		if self.state <> 'CONNECTED':
			# TODO disconnect the client
			self.close()
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
			qoss.append(self.server.subscribe(self, topic, qos))
		# TODO response SUBACK
		yield self.__send_suback(message_id, qoss)

	@gen.coroutine
	def __handle_unsubscribe(self, pack):
		pdb.set_trace()
		if self.state <> 'CONNECTED':
			# TODO disconnect the client
			self.close()
			return
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
			self.server.unsubscribe(self, topic)
		# TODO response UNSUBACK
		yield self.__send_unsuback(message_id)

	@gen.coroutine
	def __handle_publish(self, pack):
		pdb.set_trace()
		if self.state <> 'CONNECTED':
			# TODO disconnect the client
			self.close()
			return gen.Return(None)
		remaining_buffer = pack.get('remaining_buffer')
		topic_length = self.__read_next_string_length(remaining_buffer, 0)
		payload_length = pack.get('remaining_length') - 2 - topic_length - 2
		remaining_buffer_format = '!H%ssH%ss' % (topic_length, payload_length)
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, remaining_buffer)
		topic = pack['topic'] = remaining_buffer_tuple[1]
		message_id = pack['message_id'] = remaining_buffer_tuple[2]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		qos = pack['qos'] = pack.get('cmd') & 0x6
		self.incoming_messages[message_id] = pack
		# TODO reply
		if qos == QoS0:
			self.server.publish(topic, payload, qos)
			return gen.Return(None)
		if qos == QoS1:
			self.server.publish(topic, payload, qos)
			yield self.__send_puback(message_id)
			return gen.Return(None)
		if qos == QoS2:
			# TODO response PUBREC
			yield self.__send_pubrec(message_id)
			return gen.Return(None)

	@gen.coroutine
	def __handle_puback(self, pack):
		pdb.set_trace()
		# TODO what to do?
		pass

	@gen.coroutine
	def __handle_pubrec(self, pack):
		pdb.set_trace()
		yield self.__send_pubrel(message_id)

	@gen.coroutine
	def __handle_pubcomp(self, pack):
		pdb.set_trace()
		# TODO what to do?
		pass

	@gen.coroutine
	def __handle_pubrel(self, pack):
		pdb.set_trace()
		remaining_buffer = pack.get('remaining_buffer')
		(message_id,) = pack['message_id'] = struct.unpack('!H', remaining_buffer)
		pack = self.incoming_messages.get(message_id, None)
		if pack is None:
			# TODO pack is missing
			return
		topic = pack.get('topic')
		payload = pack.get('payload')
		qos = pack.get('qos')
		self.server.publish(topic, payload, qos)
		yield self.__send_pubcomp(message_id)

	@gen.coroutine
	def __send_pubrec(self, message_id):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!2B', PUBREC, 2))
		packet.extend(struct.pack('!H', message_id))
		yield self.stream.write(packet)

	@gen.coroutine
	def __send_pubrel(self, message_id):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!2B', PUBREL, 2))
		packet.extend(struct.pack('!H', message_id))
		yield self.stream.write(packet)

	@gen.coroutine
	def __send_puback(self, message_id):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!2B', PUBACK, 2))
		packet.extend(struct.pack('!H', message_id))
		yield self.stream.write(packet)

	@gen.coroutine
	def __send_pubcomp(self, message_id):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!2B', PUBCOMP, 2))
		packet.extend(struct.pack('!H', message_id))
		yield self.stream.write(packet)

	@gen.coroutine
	def send_publish(self, dup, qos, retain, topic, message_id, payload):
		pdb.set_trace()
		payload_ = bytearray()
		payload_.extend(struct.pack('!%ss' % len(payload), payload))
		topic_ = bytearray()
		bare_topic_ = struct.pack('!%ss' % len(topic), topic)
		topic_.extend(struct.pack('!H', len(bare_topic_)))
		topic_.extend(bare_topic_)
		variable_header_length = len(topic_) + 2
		remaining_buffer_length = variable_header_length + len(payload_)
		packet = bytearray()
		byte1 = PUBLISH | dup | qos | retain
		packet.extend(struct.pack('!B', byte1))
		remaining_buffer_length_ = self.__write_remaining_length(remaining_buffer_length)
		packet.extend(remaining_buffer_length_)
		packet.extend(topic_)
		packet.extend(struct.pack('!H', message_id))
		packet.extend(payload_)
		yield self.stream.write(packet)

	@gen.coroutine
	def __handle_disconnect(self, pack):
		self.close()

	@gen.coroutine
	def __send_unsuback(self, message_id):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!2BH', UNSUBACK, 2, message_id))
		packet = str(packet)
		yield self.stream.write(packet)
		pdb.set_trace()

	@gen.coroutine
	def __send_suback(self, message_id, qoss):
		pdb.set_trace()
		payload = bytearray()
		for qos in qoss:
			payload.extend(struct.pack('!B', qos))
		remaining_length = 2 + len(payload)
		packet = bytearray()
		packet.extend(struct.pack('!B', SUBACK))
		packet.extend(self.__write_remaining_length(remaining_length))
		packet.extend(struct.pack('!H', message_id))
		packet.extend(payload)
		packet = str(packet)
		yield self.stream.write(packet)

	@gen.coroutine
	def __handle_connect(self, pack):
		pdb.set_trace()
		self.state = 'CONNECTING'
		payload_length = pack.get('remaining_length') - 12
		remaining_buffer_format = '!H6s2BH%ss' % payload_length
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, pack.get('remaining_buffer'))
		protocol_version = pack['protocol_version'] = remaining_buffer_tuple[2]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		if protocol_version <> 0x3:
			yield self.__send_connack(0x1)
			self.close()
			gen.Return(None)
		(client_id, offset) = self.__read_next_string(payload, 0)
		if len(client_id) > 23:
			yield self.__send_connack(0x2)
			self.close()
			gen.Return(None)
		connect_flags = pack['connect_flags'] = remaining_buffer_tuple[3]
		will_flag = connect_flags & 0x4 == 0x4
		if will_flag:
		# If the Will Flag is set to 1
			will_qos = connect_flags & 0x18 >> 3
			will_retain = connect_flags & 0x20 >> 5
			(will_topic, offset) = self.__read_next_string(payload, offset)
			(will_message, offset) = self.__read_next_string(payload, offset)
		if connect_flags & 0x80 == 0x80:
		# If the User name flag is set to 1
			(username, offset) = self.__read_next_string(payload, offset)
			if connect_flags & 0x40 == 0x40:
			# If the Password flag is set to 1
				(password, offset) = self.__read_next_string(payload, offset)
		yield self.__send_connack(0x0)
		self.state = 'CONNECTED'
		self.client_id = client_id
		self.will_flag = will_flag
		self.will_qos = will_qos
		self.will_retain = will_retain
		self.will_topic = will_topic
		self.will_message = will_message
		self.server.register(self)

	@gen.coroutine
	def __send_connack(self, code):
		packet = bytearray()
		packet.extend(struct.pack('!4B', CONNACK, 2, 0x0, code))
		packet = str(packet)
		yield self.stream.write(packet)

	def close(self):
		self.state = 'CLOSING'
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
		self.state = 'CLOSED'
		if self.stream.error is not None:
			if self.will_flag:
				# TODO Will Retain not implemented
				self.server.publish(self.will_topic, self.will_message, self.will_qos)

	def __init__(self, server, stream, address):
		self.server = server
		self.stream = stream
		self.address = address
		self.incoming_messages = {}
		self.outgoing_messages = {}

		self.state = 'INITIALIZE'
		self.set_close_callback(self.__close_callback)
		self.client_id = None

	def wait_message(self):
		def callback():
			if self.state == 'INITIALIZE':
				self.close()
		IOLoop.current().call_later(WAIT_TIME, callback)
		self.stream.read_bytes(1, self.__read_fix_header_byte_1)









