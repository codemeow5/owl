#!/usr/bin/python

import redis

__REDIS__ = None

class R():

	@classmethod
	def r(cls):
		if cls.__REDIS__ is None:
			try:
				r = cls.__REDIS__ = redis.StrictRedis(
					unix_socket_path='/var/run/redis/redis-6400.sock',
					db=0)
				p = cls.__PUBSUB__ = r.pubsub()
				p.subscribe(**{'__keyevent@0__:expired': cls.__handle_expire_keep_alive})
			except Exception, e:
				cls.__REDIS__ = None
		return cls.__REDIS__

	@classmethod
	def __handle_expire_keep_alive(cls, message):
		type_ = message.get('type')
		if type_ is None or type_ == 'subscribe':
			return
		if type_ == 'message':
			data = message.get('data')
			pass
