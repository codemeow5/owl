#!/usr/bin/python

import struct
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

	def __read_fix_header_byte_1(self, buff):
		pdb.set_trace()
		pack = {}
		buff = struct.unpack('!B', buff)
		pack.cmd = buff
		self.__read_remaining_length(pack)

	def __handle_pack(self, pack):
		pass
		message_type = pack.cmd & 0xF0
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
		if len(buff) <= offset:
			return None
		buffer_length = struct.unpack('!h', buff[offset:offset + 2])
		content = struct.unpack('!%ss' % buffer_length, 
			buff[offset + 2:offset + 2 + buffer_length])
		return (content, offset + 2 + buffer_length)

	def __handle_connect(self, pack):
		remaining_buffer = pack.remaining_buffer
		protocol_version = remaining_buffer[8]
		if protocol_version <> 0x3:
			# TODO reply connack message and disconnect
			self.close()
		(client_id, offset) = self.__read_next_string(remaining_buffer, 12)
		if len(client_id) > 23:
			# TODO reply connack message and disconnect
			self.close()
		connect_flags = remaining_buffer[9]
		if connect_flags & 0x4 == 0x4:
		# If the Will Flag is set to 1
			will_qos = connect_flags & 0x18 >> 3
			will_retain = connect_flags & 0x20 >> 5
			(will_topic, offset) = self.__read_next_string(remaining_buffer, offset)
			(will_message, offset) = self.__read_next_string(remaining_buffer, offset)
			#TODO here
		# TODO

	def close(self):
		# TODO remove from cstree
		pass

	def __read_remaining_buffer(self, pack):
		buff = self.stream.read_bytes(pack.remaining_length)
		buff = struct.unpack('!%sB' % pack.remaining_length, buff)
		pack.remaining_buffer = buff
		self.wait_message() # Waits for the next message
		self.__handle_pack(pack)

	def __read_remaining_length(self, pack):
		multiplier = 1
		value = 0
		while True:
			digit = yield self.stream.read_bytes(1)
			value += (digit & 127) * multiplier
			multiplier *= 128
			if ((digit & 128) == 0)
				break
		pack.remaining_length = value
		self.__read_remaining_buffer(value, pack)

	def __init__(self, server, stream, address):
		self.server = server
		self.stream = stream
		self.address = address
		pass

	def wait_message(self):
		self.stream.read_bytes(1, self.__read_fix_header_byte_1)



