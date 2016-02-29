#!/usr/bin/python

import redis
from tornado import mqttutil
from tornado.proto.mqttmessage_pb2 import MqttMessage

class RedisStorage():

	def r(self):
		if self.__REDIS__ is None:
			try:
				r = self.__REDIS__ = redis.StrictRedis(
					unix_socket_path='/var/run/redis/redis-6400.sock',
					db=0)
			except Exception, e:
				self.__REDIS__ = None
		return self.__REDIS__

	# Keys format
	# TOPIC:$ROOT is the root node of the Subscribe Tree
	# TOPIC:[topic name] is ordinary node of the Subscribe Tree
	# SUBSCRIPTION:[topic name] is a list of subscription info associated with topic
	# RETAINMSG:[topic name] is the retain message associated with topic
	# SESSION:[client id] is the session associated with client

	# Run only once at the time of installation
	def seed(self):
		pass

	def __init__(self):
		pass

	def checkExistTopic(self, topic):
		r = self.r()
		if r.exists(
			mqttutil.gen_redis_topic_key(topic), 
			mqttutil.gen_redis_sub_key(topic), 
			mqttutil.gen_redis_retain_msg_key(topic)) == 0:
			return False
		return True

	def addSubscription(self, topic, client_id, qos, session_id):
		if not mqttutil.sub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.r()
		pipe = r.pipeline()
		index = 0
		parentTopic = mqttutil.gen_redis_topic_key('$ROOT')
		while True:
			index = topic.find('/', index + 1)
			currentTopic = topic[:index]
			pipe.sadd(parentTopic, topic)
			if index == -1:
				break
			parentTopic = mqttutil.gen_redis_topic_key(currentTopic)
		pipe.hset(mqttutil.gen_redis_sub_key(topic), client_id, qos)
		pipe.setbit(mqttutil.gen_redis_topic_metadata_key(topic), 8, 1) 
		pipe.set(mqttutil.gen_redis_session_key(client_id), session_id)
		pipe.execute()

	def removeSubscription(self, topic, client_id):
		if not mqttutil.sub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.r()
		if r.hdel(mqttutil.gen_redis_sub_key(topic), client_id) > 0:
			if not self.checkExistTopic(topic):
				parentTopic = mqttutil.fetch_parent_topic(topic)
				r.srem(mqttutil.gen_redis_topic_key(parentTopic))

	def setRetainMessage(self, message):
		pass






