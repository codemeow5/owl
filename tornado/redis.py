#!/usr/bin/python

import redis
from tornado import mqttutil
from tornado.proto.mqttmessage_pb2 import MqttMessage

# TODO Sharing session state between multiple broker process
class BrokerRedisStorage():

	def __r__(self):
		if self.__REDIS__ is None:
			try:
				r = self.__REDIS__ = redis.StrictRedis(
					unix_socket_path='/var/run/redis/redis-6400.sock',
					db=0)
			except Exception, e:
				self.__REDIS__ = None
		return self.__REDIS__

# TODO Async
class RedisStorage():

	def __r__(self):
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
	# SUBSCRIPTION:TOPIC:[topic name] is a list of subscription info associated with topic
	# SUBSCRIPTION:CLIENT:[client id] is a list of topic associated with client
	# RETAINMSG:[topic name] is the retain message associated with topic
	# SESSION:[client id] is the session associated with client
	# SessionId format is [Broker IpAddress:Port]@[Client GUID]

	# Run only once at the time of installation
	def seed(self):
		pass

	def __init__(self):
		pass

	def checkEmptyTopic(self, topic):
		r = self.__r__()
		if r.exists(
			mqttutil.gen_redis_topic_key(topic), 
			mqttutil.gen_redis_sub_key(topic), 
			mqttutil.gen_redis_retain_msg_key(topic)) == 0:
			return True
		return False

	def setSession(self, client_id, session_id):
		r = self.__r__()
		r.set(mqttutil.gen_redis_session_key(client_id), session_id)

	def fetchSession(self, client_id):
		r = self.__r__()
		return r.get(mqttutil.gen_redis_session_key(client_id))

	def removeSession(self, client_id):
		r = self.__r__()
		r.del(mqttutil.gen_redis_session_key(client_id))

	def addSubscription(self, topic, client_id, qos, session_id):
		if not mqttutil.sub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.__r__()
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
		pipe.sadd(mqttutil.gen_redis_client_sub_key(client_id), topic)
		pipe.setbit(mqttutil.gen_redis_topic_metadata_key(topic), 8, 1) 
		pipe.execute()

	def removeSubscription(self, topic, client_id):
		if not mqttutil.sub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.__r__()
		if r.hdel(mqttutil.gen_redis_sub_key(topic), client_id) > 0:
			topic_ = topic
			while True:
				if not self.checkEmptyTopic(topic_):
					break
				parentTopic = mqttutil.fetch_parent_topic(topic_)
				r.srem(mqttutil.gen_redis_topic_key(parentTopic), topic_)
				topic_ = parentTopic
		r.srem(mqttutil.gen_redis_client_sub_key(client_id), topic)

	def setRetainMessage(self, message):
		topic = message.topic
		if not mqttutil.pub_topic_check(topic):
			raise Exception('Invalid topic format')
		messageStream = message.SerializeToString()
		r = self.__r__()
		r.set(mqttutil.gen_redis_retain_msg_key(topic), messageStream)

	def matchSubscription(self, topic):
		if not mqttutil.sub_topic_check(topic):
			raise Exception('Invalid topic format')
		matches = []
		parentTopics = []
		parentTopics.append('$ROOT')
		index = 0
		while True:
			preIndex = index
			index = topic.find('/', index + 1)
			currentWord = topic[preIndex:index]
			parentTopics_ = parentTopics
			parentTopics = []
			r = self.__r__()
			for parentTopic in parentTopics_:
				if currentWord == '#':
					parentTopics.extend(self.flatSubscription(parentTopic))
				elif currentWord == '+':
					childrenTopics = r.smembers(
						mqttutil.gen_redis_topic_key(parentTopic))
					parentTopics.extend(childrenTopics)
				else:
					childrenTopic = parentTopic + '/' + currentWord
					if r.sismember(
						mqttutil.gen_redis_topic_key(parentTopic), 
						childrenTopic):
						parentTopics.append(childrenTopic)
			if index == -1:
				matches.extend(parentTopics)
		return matches

	def matchPublish(self, topic):
		if not mqttutil.pub_topic_check(topic):
			raise Exception('Invalid topic format')
		matches = []
		parentTopics = []
		parentTopics.append('$ROOT')
		index = 0
		while True:
			preIndex = index
			index = topic.find('/', index + 1)
			currentWord = topic[preIndex:index]
			parentTopics_ = parentTopics
			parentTopics = []
			r = self.__r__()
			for parentTopic in parentTopics_:
				childrenTopic = parentTopic + '/' + currentWord
				if childrenTopic is not None:
					parentTopics.append(childrenTopic)
				childrenTopic = parentTopic + '/' + '+'
				if childrenTopic is not None:
					parentTopics.append(childrenTopic)
				childrenTopic = parentTopic + '/' + '#'
				if childrenTopic is not None:
					matches.append(chilrenTopic)
			if index == -1:
				matches.extend(parentTopics)
		return matches

	def flatSubscription(self, topic, resultTopics=None):
		r = self.__r__()
		if resultTopics is None:
			resultTopics = []
		childrenTopics = r.smembers(mqttutil.gen_redis_topic_key(topic))
		resultTopics.extend(childrenTopics)
		for childrenTopic in childrenTopics:
			resultTopics.extend(self.flatSubscription(childrenTopic, resultTopics))
		return resultTopics






