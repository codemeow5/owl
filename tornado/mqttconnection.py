#!/usr/bin/python

import struct
from tornado import gen
from functools import partial
import pdb

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

class MqttConnection():

	@gen.coroutine
	def __read_fix_header_byte_1(self, buff):
		pack = {}
		(buff,) = struct.unpack('!B', buff)
		pack['cmd'] = buff
		yield self.__read_remaining_length(pack)

	def __handle_pack(self, pack):
		message_type = pack.get('cmd') & 0xF0
		if message_type == CONNECT:
			self.__handle_connect(pack)
			return
		if message_type == CONNACK:
			pass
		if message_type == PUBLISH:
			pass
		if message_type == PUBACK:
			pass
		if message_type == PUBREC:
			pass
		if message_type == PUBREL:
			pass
		if message_type == PUBCOMP:
			pass
		if message_type == SUBSCRIBE:
			self.__handle_subscribe(pack)
			return
		if message_type == SUBACK:
			pass
		if message_type == UNSUBSCRIBE:
			pass
		if message_type == UNSUBACK:
			pass
		if message_type == PINGREQ:
			pass
		if message_type == PINGRESP:
			pass
		if message_type == DISCONNECT:
			pass

	def __read_next_string(self, buff, offset):
		if len(buff) <= offset:
			return None
		(buffer_length,) = struct.unpack('!h', buff[offset:offset + 2])
		(content,) = struct.unpack('!%ss' % buffer_length, 
			buff[offset + 2:offset + 2 + buffer_length])
		return (content, offset + 2 + buffer_length)

	def __read_next_buffer(self, buff, offset, length):
		if len(buff) <= offset:
			return None
		buff_tuple = struct.unpack('!%sB' % length, buff[offset: offset + length])
		return (buff_tuple, offset + length)

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
		while True:
			(topic, offset) = self.__read_next_string(payload, 0)
			if topic is None:
				break
			((qos,), offset) = self.__read_next_buffer(payload, offset, 1)
			qoss.append(self.server.subscribe(self, topic, qos))
		# TODO response SUBACK
		yield self.__send_suback(message_id, qoss)

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
		payload_length = pack.get('remaining_length') - 12
		remaining_buffer_format = '!H6s2BH%ss' % payload_length
		remaining_buffer_tuple = struct.unpack(remaining_buffer_format, pack.get('remaining_buffer'))
		protocol_version = pack['protocol_version'] = remaining_buffer_tuple[2]
		payload = pack['payload'] = remaining_buffer_tuple[-1]
		if protocol_version <> 0x3:
			yield self.__send_connack(0x1)
			self.close()
		(client_id, offset) = self.__read_next_string(payload, 0)
		if len(client_id) > 23:
			yield self.__send_connack(0x2)
			self.close()
		connect_flags = pack['connect_flags'] = remaining_buffer_tuple[3]
		if connect_flags & 0x4 == 0x4:
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

	@gen.coroutine
	def __send_connack(self, code):
		packet = bytearray()
		packet.extend(struct.pack('!4B', CONNACK, 2, 0x0, code))
		packet = str(packet)
		yield self.stream.write(packet)

	def close(self):
		self.state = 'CLOSING'
		# TODO remove from cstree
		pass
		self.state = 'CLOSED'

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

	def __init__(self, server, stream, address):
		self.server = server
		self.stream = stream
		self.address = address

		self.state = 'CONNECTING'
		self.client_id = None
		pass

	def wait_message(self):
		self.stream.read_bytes(1, self.__read_fix_header_byte_1)



