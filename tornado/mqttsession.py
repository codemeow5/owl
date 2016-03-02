#!/usr/bin/python

import pdb

class MqttSessionStorage():

	def __init__(self):
		self.__SESSIONS__ = {}
		self.__CLIENTS__ = {}

	def clear(self):
		self.__SESSIONS__.clear()
		self.__CLIENTS__.clear()

	def save(self, connection):
		client_id = connection.client_id
		if client_id is None or len(client_id) == 0:
			raise Exception('Missing argument \'client_id\'')
		session_id = connection.session_id
		if session_id is None or len(session_id) == 0:
			raise Exception('Missing argument \'session_id\'')
		self.__SESSIONS__[session_id] = connection
		self.__CLIENTS__[client_id] = connection

	def get(self, session_id=None, client_id=None):
		if session_id is not None:
			return self.__SESSIONS__.get(session_id, None)
		elif client_id is not None:
			return self.__CLIENTS__.get(client_id, None)
		else:
			raise Exception('Missing arguments \'client_id\' and \'session_id\'')

	def remove(self, session_id=None, client_id=None):
		if session_id is not None:
			connection = self.__SESSIONS__.pop(session_id, None)
			if connection is not None:
				client_id = connection.client_id
				if client_id is not None and len(client_id) > 0:
					self.__CLIENTS__.pop(client_id, None)
		elif client_id is not None:
			connection = self.__CLIENTS__.pop(client_id, None)
			if connection is not None:
				session_id = connection.session_id
				if session_id is not None and len(session_id) > 0:
					self.__SESSIONS__.pop(session_id, None)
			return self.__CLIENTS__.has_key(client_id)
		else:
			raise Exception('Missing arguments \'client_id\' and \'session_id\'')

	def checkExistSession(self, session_id=None, client_id=None):
		if session_id is not None and len(session_id) > 0:
			return self.__SESSIONS__.has_key(session_id)
		elif client_id is not None and len(client_id) > 0:
			return self.__CLIENTS__.has_key(client_id)
		else:
			raise Exception('Missing arguments \'client_id\' and \'session_id\'')
