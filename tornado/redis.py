#!/usr/bin/python

import redis

__REDIS__ = None

def redis():
	global __REDIS__
	if __REDIS__ is None:
		try:
			__REDIS__ = redis.Redis(
				unix_socket_path='/var/run/redis/redis-6400.sock',
				db=0)
		except Exception, e:
			__REDIS__ = None
	return __REDIS__


