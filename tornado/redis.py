#!/usr/bin/python

import redis

__REDIS__ = None

class R():

	@classmethod
	def r(cls):
		if cls.__REDIS__ is None:
			try:
				cls.__REDIS__ = redis.Redis(
					unix_socket_path='/var/run/redis/redis-6400.sock',
					db=0)
			except Exception, e:
				cls.__REDIS__ = None
		return cls.__REDIS__

	
