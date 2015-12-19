#!/usr/bin/python

import sys, os, socket

if __name__ == '__main__':
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(('127.0.0.1', 8888))
	while 1:
		cmd = raw_input("Please input cmd:")
		sock.sendall(cmd)
	sock.close()
