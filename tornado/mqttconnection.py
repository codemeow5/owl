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
		pdb.set_trace()
		pack = {}
		(buff,) = struct.unpack('!B', buff)
		pack['cmd'] = buff
		yield self.__read_remaining_length(pack)

	def __handle_pack(self, pack):
		pdb.set_trace()
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
			pass
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
		pdb.set_trace()
		if len(buff) <= offset:
			return None
		(buffer_length,) = struct.unpack('!h', buff[offset:offset + 2])
		(content,) = struct.unpack('!%ss' % buffer_length, 
			buff[offset + 2:offset + 2 + buffer_length])
		return (content, offset + 2 + buffer_length)

	@gen.coroutine
	def __handle_connect(self, pack):
		pdb.set_trace()
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

	@gen.coroutine
	def __send_connack(self, code):
		pdb.set_trace()
		packet = bytearray()
		packet.extend(struct.pack('!4B', CONNACK, 2, 0x0, code))
		packet = str(packet)
		yield self.stream.write(packet)
		pdb.set_trace()

	def close(self):
		# TODO remove from cstree
		pass

	@gen.coroutine
	def __read_remaining_buffer(self, pack):
		pdb.set_trace()
		buff = yield self.stream.read_bytes(pack.get('remaining_length'))
		pdb.set_trace()
		pack['remaining_buffer'] = buff
		self.wait_message() # Waits for the next message
		self.__handle_pack(pack)

	@gen.coroutine
	def __read_remaining_length(self, pack):
		pdb.set_trace()
		multiplier = 1
		value = 0
		while True:
			digit = yield self.stream.read_bytes(1)
			pdb.set_trace()
			(digit,) = struct.unpack('!B', digit)
			value += (digit & 127) * multiplier
			multiplier *= 128
			if ((digit & 128) == 0):
				break
		pack['remaining_length'] = value
		yield self.__read_remaining_buffer(pack)

	def __init__(self, server, stream, address):
		self.server = server
		self.stream = stream
		self.address = address
		pass

	def wait_message(self):
		pdb.set_trace()
		self.stream.read_bytes(1, self.__read_fix_header_byte_1)



