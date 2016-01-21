#!/usr/bin/python

import pdb
import os
import mysql.connector as mariadb
from tornado.options import OptionParser

# TODO Asynchronous
class MariaDB():

	@classmethod
	def current(cls):
		if not hasattr(cls, '__instance__'):
			cls.__instance__ = MariaDB()
		return cls.__instance__

	def __init__(self):
		options = OptionParser()
		options.define("mysql_username", default='root')
		options.define("mysql_password", default='secret')
		options.define("mysql_database", default='owldb')
		options.define("mysql_unix_socket", default='/var/run/mysqld/mysqld.sock')
		#options.parse_config_file(os.path.join(os.path.dirname(__file__),
		options.parse_config_file(os.path.join(os.getcwd(),
		                                       "owldb.cfg"))
		self.connector = mariadb.MySQLConnection(user=options.mysql_username,
							password=options.mysql_password,
							database=options.mysql_database,
							unix_socket=options.mysql_unix_socket)

	def import_to_memory(self, bucket):
		if bucket is None:
			return
		bucket.clear()
		query = ("SELECT topic, client_id, qos FROM mqtt_subscribes")
		cursor = self.connector.cursor()
		cursor.execute(query)
		for (topic, client_id, qos) in cursor:
			topic_context = bucket.get(topic, None)
			if topic_context is None:
				topic_context = bucket[topic] = {}
			clients = topic_context.get('clients', None)
			if clients is None:
				clients = topic_context['clients'] = {}
			clients[client_id] = {'connection': None,
					'qos': qos}
		cursor.close()
		query = ("SELECT topic, payload, qos FROM mqtt_retain_message")
		cursor = self.connector.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		for (topic, payload, qos) in result:
			topic_context = bucket.get(topic, None)
			if topic_context is None:
				continue
			topic_context['retain_message'] = {'qos': qos,
					'payload': payload}
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
		cursor = self.connector.cursor()
		cursor.execute(add_subscribe_, item)
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
		cursor = self.connector.cursor()
		cursor.execute(remove_subscribe_, (topic, client_id))
		cursor.close()
		return True

	def fetch_subscribes(self, client_id):
		if client_id is None:
			return
		query = ("SELECT topic, qos FROM mqtt_subscribes WHERE client_id = %s")
		cursor = self.connector.cursor()
		cursor.execute(query, (client_id,))
		return cursor.fetchall()

	def add_retain_message(self, item):
		if item is None:
			return
		topic = item.get('topic', None)
		if topic is None:
			return
		payload = item.get('payload', None)
		if payload is None:
			return
		qos = item.get('qos', None)
		if qos is None:
			return
		cursor = self.connector.cursor()
		cursor.callproc('add_retain_message', (topic, payload, qos))
		cursor.close()
		return True



