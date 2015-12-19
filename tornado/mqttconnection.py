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
		# TODO

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
