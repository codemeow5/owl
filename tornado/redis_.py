#!/usr/bin/python

import pdb
import os
import redis
from tornado import mqttutil
from tornado.options import OptionParser
from tornado.proto.mqttmessage_pb2 import MqttMessage, NetworkMessage

options = OptionParser()
options.define("redis_address", default='127.0.0.1')
options.define("redis_port", default='6379')
options.parse_config_file(os.path.join(os.getcwd(), "owl.cfg"))

# TODO Sharing session state between multiple broker process
class BrokerRedisStorage():

	def __r__(self):
		if not hasattr(self, '__REDIS__') or self.__REDIS__ is None:
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
		if not hasattr(self, '__REDIS__') or self.__REDIS__ is None:
			try:
				r = self.__REDIS__ = redis.StrictRedis(
					host=options.redis_address,
					port=int(options.redis_port),
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
	# UNREL:[client id] is a list of unrelease message associated with client
	# OUTGOING:[client id] is a list of outgoing message associated with client
	# SessionId format is [Broker IpAddress:Port]@[Client GUID]

	# Run only once at the time of installation
	def seed(self):
		pass

	def __init__(self):
		pass

	def addUnreleasedMessage(self, client_id, message_id, message):
		if client_id is None or message is None:
			return
		r = self.__r__()
		messageStream = message.SerializeToString()
		r.hset(mqttutil.gen_redis_unrel_key(client_id), message_id, messageStream)

	def removeUnreleasedMessage(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		r = self.__r__()
		r.hdel(mqttutil.gen_redis_unrel_key(client_id), message_id)

	def clearUnreleasedMessages(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		r.delete(mqttutil.gen_redis_unrel_key(client_id))

	def fetchUnreleasedMessage(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		r = self.__r__()
		messageStream = r.hget(mqttutil.gen_redis_unrel_key(client_id), message_id)
		message = NetworkMessage()
		message.ParseFromString(messageStream)
		return message

	def fetchUnreleasedMessageIds(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		return r.hkeys(mqttutil.gen_redis_unrel_key(client_id))

	def fetchUnreleasedMessages(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		messageStreams = r.hvals(mqttutil.gen_redis_unrel_key(client_id))
		messages = []
		for messageStream in messageStreams:
			message = NetworkMessage()
			message.ParseFromString(messageStream)
			messages.append(message)
		return messages

	def addOutgoingMessage(self, client_id, message):
		if client_id is None or message is None:
			return
		r = self.__r__()
		message_id = message.message_id
		messageStream = message.SerializeToString()
		r.hset(mqttutil.gen_redis_outgoing_key(client_id), message_id, messageStream)

	def removeOutgoingMessage(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		r = self.__r__()
		r.hdel(mqttutil.gen_redis_outgoing_key(client_id), message_id)

	def clearOutgoingMessages(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		r.delete(mqttutil.gen_redis_outgoing_key(client_id))

	def fetchOutgoingMessage(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		r = self.__r__()
		messageStream = r.hget(mqttutil.gen_redis_outgoing_key(client_id), message_id)
		message = NetworkMessage()
		message.ParseFromString(messageStream)
		return message

	def fetchOutgoingMessageIds(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		return r.hkeys(mqttutil.gen_redis_outgoing_key(client_id))

	def fetchOutgoingMessages(self, client_id):
		if client_id is None:
			return
		r = self.__r__()
		messageStreams = r.hvals(mqttutil.gen_redis_outgoing_key(client_id))
		messages = []
		for messageStream in messageStreams:
			message = NetworkMessage()
			message.ParseFromString(messageStream)
			messages.append(message)
		return messages

	def checkEmptyTopic(self, topic):
		r = self.__r__()
		if r.exists(mqttutil.gen_redis_topic_key(topic)) == 0 and \
			r.exists(mqttutil.gen_redis_sub_key(topic)) == 0 and \
			r.exists(mqttutil.gen_redis_retain_msg_key(topic)) == 0:
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
		r.delete(mqttutil.gen_redis_session_key(client_id))

	def addSubscription(self, topic, client_id, qos):
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

	def fetchSubClients(self, topic):
		r = self.__r__()
		clients = r.hgetall(mqttutil.gen_redis_sub_key(topic))
		return clients

	def clearSubscription(self, client_id):
		r = self.__r__()
		topics = r.smembers(mqttutil.gen_redis_client_sub_key(client_id))
		r.delete(mqttutil.gen_redis_client_sub_key(client_id))
		if topics is None:
			return
		for topic in topics:
			self.removeSubscription(topic, client_id)

	def setRetainMessage(self, message):
		topic = message.topic
		if not mqttutil.pub_topic_check(topic):
			raise Exception('Invalid topic format')
		messageStream = message.SerializeToString()
		r = self.__r__()
		r.set(mqttutil.gen_redis_retain_msg_key(topic), messageStream)

	def removeRetainMessage(self, topic):
		if not mqttutil.pub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.__r__()
		r.delete(mqttutil.gen_redis_retain_msg_key(topic))

	def fetchRetainMessage(self, topic):
		if not mqttutil.pub_topic_check(topic):
			raise Exception('Invalid topic format')
		r = self.__r__()
		messageStream = r.get(mqttutil.gen_redis_retain_msg_key(topic))
		if messageStream is None:
			return None
		message = MqttMessage()
		message.ParseFromString(messageStream)
		return message

	def fetchRetainMessages(self, topic):
		r = self.__r__()
		matchTopics = self.matchSubscription(topic)
		messages = []
		for matchTopic in matchTopics:
			message = self.fetchRetainMessage(matchTopic)
			if message is not None:
				messages.append(message)
		return messages

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
			currentWord = topic[preIndex:index] if index <> -1 else topic[preIndex:]
			parentTopics_ = parentTopics
			parentTopics = []
			r = self.__r__()
			for parentTopic in parentTopics_:
				if currentWord == '#':
					matches.extend(self.flatSubscription(parentTopic))
				elif currentWord == '+':
					childrenTopics = r.smembers(
						mqttutil.gen_redis_topic_key(parentTopic))
					for childrenTopic in childrenTopics:
						if mqttutil.pub_topic_check(childrenTopic):
							parentTopics.append(childrenTopic)
				else:
					childrenTopic = parentTopic + '/' + currentWord \
						if preIndex <> 0 else currentWord
					if r.sismember(
						mqttutil.gen_redis_topic_key(parentTopic), 
						childrenTopic):
						parentTopics.append(childrenTopic)
			if index == -1:
				matches.extend(parentTopics)
				break
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
			currentWord = topic[preIndex:index] if index <> -1 else topic[preIndex:]
			parentTopics_ = parentTopics
			parentTopics = []
			r = self.__r__()
			for parentTopic in parentTopics_:
				childrenTopic = parentTopic + '/' + currentWord \
					if preIndex <> 0 else currentWord
				if childrenTopic is not None:
					if r.sismember(
						mqttutil.gen_redis_topic_key(parentTopic),
						childrenTopic):
						parentTopics.append(childrenTopic)
				childrenTopic = parentTopic + '/' + '+' \
					if preIndex <> 0 else '+'
				if childrenTopic is not None:
					if r.sismember(
						mqttutil.gen_redis_topic_key(parentTopic),
						childrenTopic):
						parentTopics.append(childrenTopic)
				childrenTopic = parentTopic + '/' + '#' \
					if preIndex <> 0 else '#'
				if childrenTopic is not None:
					if r.sismember(
						mqttutil.gen_redis_topic_key(parentTopic),
						childrenTopic):
						matches.append(childrenTopic)
			if index == -1:
				matches.extend(parentTopics)
				break
		return matches

	def flatSubscription(self, topic, resultTopics=None):
		r = self.__r__()
		if resultTopics is None:
			resultTopics = []
		childrenTopics = r.smembers(mqttutil.gen_redis_topic_key(topic))
		for childrenTopic in childrenTopics:
			if mqttutil.pub_topic_check(childrenTopic):
				resultTopics.append(childrenTopic)
				resultTopics.extend(
					self.flatSubscription(childrenTopic, resultTopics))
		return resultTopics





