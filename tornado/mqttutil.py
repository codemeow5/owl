#!/usr/bin/python

import pdb
import uuid
from tornado.options import OptionParser

options = OptionParser()
options.define("address", default='127.0.0.1')
options.define("port", default='1883')
options.parse_config_file(os.path.join(os.getcwd(), "owl.cfg"))

def sub_topic_check(topic):
	c = '\0'
	len_ = 0;
	while True:
		if topic is None or len(topic) == 0:
			break
		if topic[0] == '+':
			if (c <> '\0' and c <> '/') or (len(topic) > 1 and topic[1] <> '/'):
				return False
		elif topic[0] == '#':
			if (c <> '\0' and c <> '/') or len(topic) > 1:
				return False
		len_ = len_ + 1
		c = topic[0]
		topic = topic[1:]
	if len_ > 65535 or len_ == 0:
		return False
	return True

def pub_topic_check(topic):
	len_ = 0
	while True:
		if topic is None or len(topic) == 0:
			break
		if topic[0] == '+' or topic[0] == '#':
			return False
		len_ = len_ + 1
		topic = topic[1:]
	if len_ > 65535 or len_ == 0:
		return False
	return True

def fetch_parent_topic(topic):
	index = topic.rfind('/')
	if index == -1:
		return '$ROOT'
	return topic[:index]

def gen_redis_topic_key(topic):
	return 'TOPIC:' + topic

def gen_redis_sub_key(topic):
	return 'SUBSCRIPTION:TOPIC:' + topic

def gen_redis_client_sub_key(client_id):
	return 'SUBSCRIPTION:CLIENT:' + client_id

def gen_redis_retain_msg_key(topic):
	return 'RETAINMSG:' + topic

def gen_redis_session_key(client_id):
	return 'SESSION:' + client_id

def gen_redis_unrel_key(client_id):
	return 'UNREL:' + client_id

def gen_redis_outgoing_key(client_id):
	return 'OUTGOING:' + client_id

def gen_session_id():
	return options.address + ':' + \
		options.port + '@' + \
		str(uuid.uuid1())

def fetch_detail_from_session_id(session_id):
	index = session_id.find('@')
	if index == -1:
		return None
	cdir = session_id[:index]
	guid = session_id[index + 1:]
	index = cdir.find(':')
	if index == -1:
		return None
	address = cdir[:index]
	port = cdir[index + 1:]
	return (address, port, guid)





