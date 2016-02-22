#!/usr/bin/python

import pdb

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

class TrieNode():

	def __init__(self, word):
		self.word = word
		self.has_sub_info = False
		self.has_retain_message = False
		self.__children__ = {}
		self.__clients__ = {}
		self.__retain_message__ = None

	def get_clients(self):
		return self.__clients__

	def add_child(self, node):
		self.__children__[node.word] = node
		node.parent = self

	def remove_child(self, node):
		self.__children__.pop(node.word, None)

	def get_child(self, word):
		return self.__children__.get(word, None)

	def destroy(self):
		if self.parent is not None:
			self.parent.remove_child(self)

	def add_subscribe(self, topic, client_id, qos, connection=None):
		self.has_sub_info = True
		self.topic = topic
		self.__clients__[client_id] = {
			'connection': connection,
			'qos': qos
		}

	def remove_subscribe(self, topic, client_id):
		if topic <> self.topic:
			return False
		connection = self.__clients__.pop(client_id, None)
		if len(self.__clients__) == 0:
			self.has_sub_info = False
		if ((not self.has_sub_info) and 
			(not self.has_retain_message) and
			(len(self.__children__) == 0)):
			self.destroy()
		if connection is None:
			return False

	def set_retain_message(self, topic, message):
		if message is None:
			self.has_retain_message = False
		else:
			self.has_retain_message = True
			message.retain = True
		self.topic = topic
		self.__retain_message__ = message

	def get_retain_message(self):
		return self.__retain_message__

	def children(self):
		return self.__children__.values()

	def flat(self, node_collection=None):
		if node_collection is None:
			node_collection = []
		for children_node in self.__children__:
			node_collection.append(children_node)
			node_collection.extend(children_node.flat(node_collection))
		return node_collection

class Trie():

	def __init__(self):
		self.__SUBSCRIBES__ = {}
		self.__ROOTNODE__ = TrieNode(word=None)

	def clear(self):
		self.__SUBSCRIBES__.clear()
		self.__ROOTNODE__ = TrieNode(word=None)

	def add_subscribe(self, topic, client_id, qos, connection=None):
		if not sub_topic_check(topic):
			raise Exception('Invalid topic format')
		seq = topic.split('/')
		node = self.__ROOTNODE__
		while True:
			if seq is None or len(seq) == 0:
				break
			word = seq[0]
			child_node = node.get_child(word)
			if child_node is None:
				child_node = TrieNode(word)
				node.add_child(child_node)
			node = child_node
			if len(seq) == 1:
				node.add_subscribe(topic, client_id, qos, connection)
			seq = seq[1:]

	def remove_subscribe(self, topic, client_id):
		if not sub_topic_check(topic):
			raise Exception('Invalid topic format')
		seq = topic.split('/')
		node = self.__ROOTNODE__
		while True:
			if seq is None or len(seq) == 0:
				break
			word = seq[0]
			node = node.get_child(word)
			if node is None:
				return
			if len(seq) == 1:
				node.remove_subscribe(topic, client_id)
			seq = seq[1:]

	def set_retain_message(self, message):
		topic = message.topic
		if not pub_topic_check(topic):
			raise Exception('Invalid topic format')
		seq = topic.split('/')
		node = self.__ROOTNODE__
		while True:
			if seq is None or len(seq) == 0:
				break
			word = seq[0]
			node = node.get_child(word)
			if node is None:
				return
			if len(seq) == 1:
				node.set_retain_message(topic, message)
			seq = seq[1:]

	def matches_sub(self, topic):
		""" Return matching node"""
		if not sub_topic_check(topic):
			raise Exception('Invalid topic format')
		matches = []
		progress = []
		seq = topic.split('/')
		progress.append(self.__ROOTNODE__)
		while True:
			if seq is None or len(seq) == 0:
				break
			word = seq[0]
			progress_ = progress
			progress = []
			if word == '#':
				for prog in progress_:
					progress.extend(prog.flat())
			elif word == '+':
				for prog in progress_:
					progress.extend(prog.children())
			else:
				for prog in progress_:
					child_node = prog.get_child(word)
					if child_node is not None:
						progress.append(child_node)
			if len(seq) == 1:
				matches.extend(progress)
			seq = seq[1:]
		return matches

	def matches_pub(self, topic):
		pdb.set_trace()
		""" Return matching node"""
		if not pub_topic_check(topic):
			raise Exception('Invalid topic format')
		matches = []
		progress = []
		seq = topic.split('/')
		progress.append(self.__ROOTNODE__)
		while True:
			if seq is None or len(seq) == 0:
				break
			word = seq[0]
			progress_ = progress
			progress = []
			for prog in progress_:
				child_node = prog.get_child(word)
				if child_node is not None:
					progress.append(child_node)
				child_node = prog.get_child('+')
				if child_node is not None:
					progress.append(child_node)
				child_node = prog.get_child('#')
				if child_node is not None:
					matches.append(child_node)
			if len(seq) == 1:
				matches.extend(progress)
			seq = seq[1:]
		return matches












