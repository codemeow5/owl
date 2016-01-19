#!/usr/bin/python

import redis

class R():

	@classmethod
	def r(cls):
		if cls.__REDIS__ is None:
			try:
				r = cls.__REDIS__ = redis.StrictRedis(
					unix_socket_path='/var/run/redis/redis-6400.sock',
					db=0)
			except Exception, e:
				cls.__REDIS__ = None
		return cls.__REDIS__

