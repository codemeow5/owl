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

	def add_child(self, node):
		self.__children__[node.word] = node
		node.parent = self

	def remove_child(self, node):
		self.__children__.pop(node.word, None)

	def get_child(self, word):
		return self.__children__.get(word, None)

	def del(self):
		if self.parent is not None:
			self.parent.remove_child(self)

	def add_subscribe(self, topic, connection, qos):
		self.has_sub_info = True
		self.topic = topic
		self.__clients__[connection.client_id] = {
			'connection': connection,
			'qos': qos
		}

	def remove_subscribe(self, topic, client_id):
		if topic <> self.topic:
			return False
		connection = self.__clients__.pop(client_id, None)
		if len(self.__clients__) == 0:
			self.has_sub_info = False
		if (not self.has_sub_info) and 
			(not self.has_retain_message) and
			(len(self.__children__) == 0):
			self.del()
		if connection is None:
			return False

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

	def add_subscribe(self, topic, connection, qos):
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
				node.add_subscribe(topic, connection, qos)
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

	def matches(self, topic):
		""" Return matching node"""
		matches = []
		progress = []
		seq = topic.split('/')
		progress.append(self.__ROOTNODE__)
		while True:
			if seq is None or len(seq) == 0:
				break
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

















