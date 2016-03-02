#!/usr/bin/python

import pdb

from tornado.redis_ import RedisStorage

if __name__ == '__main__':
	r = RedisStorage()
	pdb.set_trace()
	r.matchPublish('TopicA')
