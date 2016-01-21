#!/usr/bin/python

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import pdb
import time

def on_connect(client, userdata, rc):
	#pdb.set_trace()
	print 'ok'

def on_message(client, userdata, msg):
	print 'Recived(%s): %s' % (msg.topic, msg.payload)
	
client = mqtt.Client(client_id=raw_input('Your Client Id:'))
client.on_connect = on_connect
client.on_message = on_message

client.connect('127.0.0.1', 8888, 60)

client.loop_start()

time.sleep(3)

while True:
	msg = raw_input('Send: ')
	client.publish('public/a', msg, 0)
	client.publish('public/b', msg, 0)
	client.publish('public/c', msg, 1)
	client.publish('public/c', '%s(Retain)' % msg, 1, True)
	client.publish('public/d', msg, 2)

#client.loop_forever()

client.loop_stop()
