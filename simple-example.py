#!/usr/bin/env python3

from reliableMqttClient import ReliableMqttClient
import socket
import threading
import time

def main():
	USERNAME    = "user"
	PASSWORD    = "1234"
	SERVERIP    = "localhost"
	PORT 		= 1883
	CLIENTID	= socket.gethostname()
	KEEPALIVE 	= 20
	QOS 		= 1
	LOGGER 		= False
	PRINTER 	= True
	LOGPREFIX 	= "*** Simple example - "
	LOGLEVEL	= "debug"
	BUFNAME		= "persistentbuffer.db"

	mqttClient = ReliableMqttClient(USERNAME, PASSWORD, SERVERIP, clientid=CLIENTID, qos=QOS,
		keepalive=KEEPALIVE, logger=LOGGER, printer=PRINTER, logprefix=LOGPREFIX, loglevel=LOGLEVEL, persistentBufferFilename=BUFNAME)

	def call_stats():
		while True:
			time.sleep(10)
			mqttClient.message_stats(log=True, reset=False)
	threading.Thread(target=call_stats).start()

	i = 1
	while True:
		time.sleep(1)
		msg = f'test field={i} {int(time.time()*1e9)}'
		topic = 'topic'
		print(f'Publishing {topic} - {msg}')
		mqttClient.publish(topic, msg)
		i += 1

if __name__ == '__main__':
	# main()
	help(ReliableMqttClient)
