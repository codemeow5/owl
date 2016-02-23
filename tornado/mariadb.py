#!/usr/bin/python

import pdb
import os, binascii
import mysql.connector as mariadb
from tornado.options import OptionParser
from tornado.mqttmessage import MqttMessage, BinaryMessage

# TODO Asynchronous
class MariaDB():

	@classmethod
	def current(cls):
		if not hasattr(cls, '__instance__'):
			cls.__instance__ = MariaDB()
		return cls.__instance__

	def __init__(self):
		self.options = OptionParser()
		self.options.define("mysql_username", default='root')
		self.options.define("mysql_password", default='secret')
		self.options.define("mysql_database", default='owl')
		self.options.define("mysql_unix_socket", default='/var/run/mysqld/mysqld.sock')
		self.options.parse_config_file(os.path.join(os.getcwd(),
		                                       "owl.cfg"))
		self.__connector = mariadb.MySQLConnection(user=self.options.mysql_username,
							password=self.options.mysql_password,
							database=self.options.mysql_database,
							unix_socket=self.options.mysql_unix_socket)

	def fetch_connector(self):
		if hasattr(self, '__connector') and self.__connector.is_connected():
			pass
		else:
			self.__connector = mariadb.MySQLConnection(user=self.options.mysql_username,
								password=self.options.mysql_password,
								database=self.options.mysql_database,
								unix_socket=self.options.mysql_unix_socket)
		return self.__connector

	def import_to_memory(self, bucket):
		if bucket is None:
			return
		bucket.clear()
		query = ("SELECT topic, client_id, qos FROM mqtt_subscribes")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query)
		for (topic, client_id, qos) in cursor:
			bucket.add_subscribe(topic, client_id, qos)
		connector.commit()
		cursor.close()
		query = ("SELECT topic, payload, qos FROM mqtt_retain_messages")
		cursor = connector.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		for (topic, payload, qos) in result:
			message = MqttMessage(topic, payload, qos, True)
			bucket.set_retain_message(message)
		connector.commit()
		cursor.close()
		return True

	def add_subscribe(self, item):
		if item is None:
			return
		topic = item.get('topic', None)
		if topic is None:
			return
		client_id = item.get('client_id', None)
		if client_id is None:
			return
		qos = item.get('qos', None)
		if qos is None:
			return
		add_subscribe_ = ("INSERT INTO mqtt_subscribes "
				"(topic, client_id, qos) "
				"VALUES (%(topic)s, %(client_id)s, %(qos)s)")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(add_subscribe_, item)
		connector.commit()
		cursor.close()
		return True

	def remove_subscribe(self, item):
		if item is None:
			return
		topic = item.get('topic', None)
		if topic is None:
			return
		client_id = item.get('client_id', None)
		if client_id is None:
			return
		remove_subscribe_ = ("DELETE FROM mqtt_subscribes "
				"WHERE topic = %s AND client_id = %s")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(remove_subscribe_, (topic, client_id))
		connector.commit()
		cursor.close()
		return True

	def fetch_subscribes(self, client_id):
		if client_id is None:
			return
		query = ("SELECT topic, qos FROM mqtt_subscribes WHERE client_id = %s")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query, (client_id,))
		return cursor.fetchall()

	def add_retain_message(self, message):
		if message is None:
			return
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.callproc('add_retain_message', 
			(message.topic, message.payload, message.qos))
		connector.commit()
		cursor.close()
		return True

	def add_unreleased_message(self, client_id, message_id, message):
		if client_id is None or message_id is None or message is None:
			return
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.callproc('add_unreleased_message',
			(client_id, message_id, message.topic, message.payload, message.qos, message.retain))
		connector.commit()
		cursor.close()

	def remove_unreleased_message(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		query = ("DELETE FROM mqtt_unreleased_messages WHERE client_id = %s AND message_id = %s")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query, (client_id, message_id))
		cursor.close()

	def fetch_unreleased_messages(self, client_id):
		if client_id is None:
			return
		query = ("SELECT message_id, topic, payload, qos, retain FROM mqtt_unreleased_messages WHERE client_id = %s")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query, (client_id,))
		messages = {}
		for (message_id, topic, payload, qos, retain) in cursor.fetchall():
			messages[message_id] = MqttMessage(topic, payload, qos, retain)
		return messages

	def add_outgoing_message(self, client_id, message):
		if client_id is None or message is None or message.message_id is None:
			return
		connector = self.fetch_connector()
		cursor = connector.cursor()
		buffstr = binascii.b2a_uu(message.buffer) # So ugly :(
		cursor.callproc('add_outgoing_message',
			(client_id, message.message_id, buffstr, message.message_type, message.qos))
		connector.commit()
		cursor.close()

	def remove_outgoing_message(self, client_id, message_id):
		if client_id is None or message_id is None:
			return
		query = ("DELETE FROM mqtt_outgoing_messages WHERE client_id = %s AND message_id = %s")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query, (client_id, message_id))
		cursor.close()

	def fetch_outgoing_messages(self, client_id):
		if client_id is None:
			return
		query = ("SELECT message_id, buffer, message_type, qos FROM mqtt_outgoing_messages WHERE client_id = %s ORDER BY message_id")
		connector = self.fetch_connector()
		cursor = connector.cursor()
		cursor.execute(query, (client_id,))
		messages = []
		for (message_id, buffstr, message_type, qos) in cursor.fetchall():
			buffer = binascii.a2b_uu(buffstr) # So ugly :(
			messages.append(BinaryMessage(buffer, message_type, qos, 0, message_id))
		return messages






