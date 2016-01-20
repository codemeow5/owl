#!/usr/bin/python

import mysql.connector as mariadb
from tornado.options import OptionParser()

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
		options.parse_config_file(os.path.join(os.path.dirname(__file__),
		                                       "owldb.cfg"))
		self.connector = mariadb.MySQLConnection(user=options.mysql_username,
							password=options.mysql_password,
							database=options.mysql_database,
							unix_socket=options.unix_socket)

