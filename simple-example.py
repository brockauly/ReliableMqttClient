#!/usr/bin/env python3

from reliableMqttClient import ReliableMqttClient
import socket
import threading
from time import sleep

def main():
	USERNAME 	= "user"
	PASSWORD 	= "1234"
	SERVERIP 	= "127.0.0.1"
	PORT 		= 1883
	CLIENTID	= socket.gethostname()
	KEEPALIVE 	= 20
	QOS 		= 1
	LOGGER 		= False
	PRINTER 	= True
	LOGPREFIX 	= "*** Simple example - "
	BUFNAME		= "persistentbuffer.db"

	mqttClient = ReliableMqttClient(USERNAME, PASSWORD, SERVERIP, clientid=CLIENTID, qos=QOS,
		keepalive=KEEPALIVE, logger=LOGGER, printer=PRINTER, logprefix=LOGPREFIX, persistentBufferFilename=BUFNAME)

	def call_stats():
		while True:
			sleep(10)
			mqttClient.message_stats(log=True, reset=False)
	threading.Thread(target=call_stats).start()

	i = 1
	while True:
		sleep(3)
		topic = 'test'
		payload = f'example {i}'
		print(f'Publishing {topic} {payload}')
		mqttClient.publish(topic, payload)
		i += 1

if __name__ == '__main__':
	# main()
	help(ReliableMqttClient)
