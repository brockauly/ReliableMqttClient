import paho.mqtt.client as mqtt
import os
import socket
import sqlite3 as sql
import threading

class ReliableMqttClient:
	'''
	Manages a reliable and persistent MQTT connection.
	With the default settings (qos > 0, a valid client_id, clean_session=False), when internet connection is lost, published messages are buffered and retried until success.

	Notes and known issues
		- When reconnecting with a non-empty persistent buffer, messages are sent by batch of <maxThreadNumber> every <keepalive> seconds.
			-> It may happen on rare occasion that a message is sent twice (event with qos=2), cause not identified yet.
		- What happens if a new message gets the same <mid> as one in the persistent storage (because script restarted) ?
			-> Old message is removed from storage and re-published (gets new <mid>), then rewritten in storage if not delivered.
			-> Known issue : if crash or reboot at that moment (unlikely), those messages are lost.
	
	:author: Nicolas Brusselmans <nicolas.brusselmans@uclouvain.be>
	:license: MPL-2.0
	:version: 1.0
	'''
	def __init__(self, username, password, serverip, port=1883, clientid=socket.gethostname(), keepalive=20, qos=1,
		logger=False, printer=True, logprefix="", loglevel='info', persistentBufferFilename=None, maxThreadNumber=1000):
		'''
		Connects and starts a client with the provided parameters.

		Parameters
			username : str
				username to connect to the broker
			password : str
				associated password
			serverip : str
				ip address of the broker
			port : int, optional
				port of the broker, default is 1883
			clientid : str, optional
				**unique** client id, default is <socket.gethostname()>
			keepalive : int, optional
				interval between ping messages in seconds, default is 20
				the client is disconnected if no response is received after that interval, so the max time to detect a down connection is 2 * keepalive
				if <persistentBufferFilename> is set, <keepalive> is also the delay before storing pending messages
			qos : int, optional
				quality of service (0, 1 or 2), default is 1
			logger : bool, optional
				if True, logs on system logs, default is False
			printer : bool, optional
				if True, logs on console, default is True
			logprefix : str, optional
				prefix of each log message, default is empty
			loglevel : str, optional
				log verbosity ('error', 'info' (+ connecitivity logs) or 'debug' (+ message status logs)), default is 'info'
			persistentBufferFilename : str, optional
				filename of the SQL database used to pesistently store messages in case of disconnection or reboot
				pending messages are persistently buffered after <keepalive> seconds
				if None, the persistent buffering function is not used
			maxThreadNumber : int, optional
				maximum number of threads that can be spawned concurrently.
				can raise a RuntimeError if too large, system-dependent.
				default value of 1000 should be safe for all systems.
		'''
		self.qos = qos
		self.keepalive = keepalive
		self.logger = logger
		self.printer = printer
		self.logprefix = logprefix
		self.loglevel = loglevel

		self.debounce_disconnect = False # because on_disconnect is called twice when keepalive times out
		self.pending_msg_dicts = {} # dict of {mid: {'msg_info':<>, 'topic':<>, 'payload':<>}, ... }

		self.stat_published = 0
		self.stat_sent = 0

		if persistentBufferFilename:
			self.persistentBuffer = PersistentBuffer(persistentBufferFilename, self.printlog)
		else:
			self.persistentBuffer = None
		# Maximum number of threads, used by persistent buffer for storing message after timeout.
		self.max_thread_number_semaphore = threading.BoundedSemaphore(maxThreadNumber)

		self.client = mqtt.Client(client_id=clientid, clean_session=False)
		self.client.on_connect = self.on_connect
		self.client.on_message = self.on_message
		self.client.on_publish = self.on_publish
		self.client.on_disconnect = self.on_disconnect

		self.client.username_pw_set(username, password=password)
		self.client.connect_async(serverip, port=port, keepalive=keepalive)
		self.client.loop_start()

	def publish(self, topic, payload, oldmid=None):
		'''
		Queues a message for publishing.
		Stores it persistently if client is disconnected or if the message cannot be published.
		:param str topic: Topic of the mqtt message
		:param str payload: Payload of the mqtt message
		:param int oldmid: mid of the message if already existing, should not be used by user.
		'''
		# Do not publish (nor store, it stays stored until sent) a message if it is already pending (in the case of deco/reco, stored messages come back here !)
		if oldmid:
			if oldmid in self.pending_msg_dicts.keys() and self.pending_msg_dicts[oldmid]['topic'] == topic and self.pending_msg_dicts[oldmid]['payload'] == payload:
				self.printlog('debug', f'ReliableMqttClient.publish({str(topic)}, {str(payload)}, {str(oldmid)}) : not re-publishing message already in queue')
				return
			else: # message should be removed from storage and republished with new mid otherwise mid conflict
				self.persistentBuffer.remove(oldmid, topic, payload)
		msg_info = self.client.publish(topic=topic, payload=payload, qos=self.qos)
		mid = msg_info.mid
		self.pending_msg_dicts[mid] = {'msg_info':msg_info, 'topic':topic, 'payload':payload}
		self.stat_published += 1
		# If connected, set a timeout equal to keepalive before storing persistently
		# If not, directly store persistently.
		if self.persistentBuffer:
			if self.client.is_connected():
				publish_timeout = self.keepalive
			else:
				publish_timeout = 0
			def store_and_release_semaphore(mid):
				if self.persistentBuffer and mid in self.pending_msg_dicts.keys():
					try:
						self.persistentBuffer.add(mid, self.pending_msg_dicts[mid]['topic'], self.pending_msg_dicts[mid]['payload'])
					except Exception as e:
						self.printlog('error', f'ReliableMqttClient.store_persistently({str(mid)}) : Error: {str(e)}')
				self.max_thread_number_semaphore.release()
			if oldmid == None: # if mid is set, semaphore is already acquired by publish_persistent_buffer_on_connect()
				self.max_thread_number_semaphore.acquire()
			threading.Timer(publish_timeout, lambda:store_and_release_semaphore(mid)).start()

	def disconnect(self):
		'''
		Disconnects and stops the client.
		It is advised that the parent script waits for at least <keepalive> seconds before exiting
		for all queued messages to be stored persistently.
		'''
		self.client.disconnect()
		self.client.loop_stop()

	def reconnect(self):
		'''
		Reconnects and restarts the client.
		To be used after disconnect().
		'''
		self.client.reconnect()
		self.client.loop_start()

	def on_connect(self, client, userdata, flags, rc):
		self.printlog('info', f'Connected with result code {str(rc)}')
		if self.persistentBuffer:
			def publish_persistent_buffer_on_connect():
				while stored_msg := self.persistentBuffer.pop_one():
					self.max_thread_number_semaphore.acquire()
					self.publish(stored_msg['topic'], stored_msg['payload'], stored_msg['mid'])
			threading.Thread(target=publish_persistent_buffer_on_connect).start()

	def on_message(self, client, userdata, msg):
		self.printlog('debug', f'Message received : {str(msg.topic)} {str(msg.payload)}')

	def on_publish(self, client, userdata, mid):
		try:
			self.stat_sent += 1
			self.printlog('debug', f"Message sent : {str(mid)} {str(self.pending_msg_dicts[mid]['topic'])} {str(self.pending_msg_dicts[mid]['payload'])}")
			if self.persistentBuffer:
				# TODO does it have to be done every time ? or only for known stored mid ?
				self.persistentBuffer.remove(mid, self.pending_msg_dicts[mid]['topic'], self.pending_msg_dicts[mid]['payload'])
			del self.pending_msg_dicts[mid]
		except KeyError as e:
			self.printlog('error', f'ReliableMqttClient.on_publish({str(mid)}) : KeyError: {str(e)}')

	def reset_debounce_disconnect(self):
		self.debounce_disconnect = False

	def on_disconnect(self, client, userdata, rc):
		# ignore if double call which happens when keepalive times out
		if self.debounce_disconnect:
			return
		self.debounce_disconnect = True
		# 1sec debounce time : every call to on_disconnect within 1sec after the first call is ignored
		threading.Timer(1, self.reset_debounce_disconnect).start()
		self.printlog('info', f'Disconnected with result code {str(rc)}')

	def printlog(self, level, msg):
		'''
		Write to syslog and/or print a log depending on config.
		:param str level: 'error' or 'info' or 'debug'
		:param str msg: message to be logged
		'''
		def loglevel_number(l):
			if l == 'error':
				return 0
			if l == 'info':
				return 1
			if l == 'debug':
				return 2
			return -1
		if loglevel_number(level) <= loglevel_number(self.loglevel):
			if(self.logger):
				os.system(f'logger \"{self.logprefix}{msg}\"')
			if(self.printer):
				print(f'{self.logprefix}{msg}')

	def message_stats(self, log=True, reset=False):
		'''
		Get statistics about messages published, sent, pending and stored.
		Without loglevel='debug', this is a convenient way of logging aggregated message logs by periodically calling this function in user application.
		:param bool log: if True, stats are written to syslog and/or printed. default to True
		:param bool reset: if True, reset after fetching stats. default to False
		:return: {'published':<>, 'sent':<>, 'pending':<> [, 'stored':<>]} # 'stored' if persistent buffer is used
		'''
		stats = {'published': self.stat_published, 'sent': self.stat_sent, 'pending': len(self.pending_msg_dicts)}
		if self.persistentBuffer:
			stats['stored'] = self.persistentBuffer.get_length()
		if reset:
			self.reset_message_stats()
		if log:
			self.printlog('always', f'Message statistics - {stats}')
		return stats

	def reset_message_stats(self):
		'''
		Reset message statistics counters 'published' and 'sent'.
		'pending' and 'stored' are state variables and can not be reset.
		'published' counter is set equal to 'pending' counter for coherence.
		'''
		self.stat_published = len(self.pending_msg_dicts)
		self.stat_sent = 0


class PersistentBuffer:
	'''
	Helper class to wrap read and write commands to the persistent buffer.
	This implementation uses SQL in Write-Ahead Log mode, which ensures atomic operation (TBV).
	'''

	EXPECTED_COLUMNS = {
		'id': 'INTEGER',
		'mid': 'INTEGER',
		'topic': 'TEXT',
		'payload': 'TEXT'
	}
	TABLE_NAME = 'messages'

	def __init__(self, filename, printlogfunc):
		'''
		:param str filename: Filename of the SQL database to read and write to.
		:param function printlogfunc: Function to call to emit a log.
		'''
		def schema_is_valid(conn):
			try:
				cursor = conn.execute(f'PRAGMA table_info({self.TABLE_NAME})')
				columns = {row[1]: row[2] for row in cursor.fetchall()}
				for col, col_type in self.EXPECTED_COLUMNS.items():
					if col not in columns or col_type not in columns[col].upper():
						return False
				return True
			except Exception:
				return False

		self.printlog = printlogfunc
		self.filename = filename
		self.conn_mutex = threading.Lock()

		file_exists = os.path.exists(self.filename)
		with self.conn_mutex:
			with self.get_conn() as conn:
				create_table = False
				if file_exists:
					if not schema_is_valid(conn):
						self.printlog('info', 'PersistentBuffer(): Existing DB schema invalid. Recreating table.')
						conn.execute(f"DROP TABLE IF EXISTS {self.TABLE_NAME}")
						create_table = True
				else:
					create_table = True

				if create_table:
					conn.execute(f"""CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
											id INTEGER PRIMARY KEY AUTOINCREMENT,
											mid INTEGER NOT NULL,
											topic TEXT NOT NULL,
											payload TEXT NOT NULL
										)
										""")
					conn.commit()

	def get_conn(self):
		'''
		Returns a connection object to the SQL database.
		Needed because each thread must have its own conn object.
		:return: SQL conn object.
		'''
		conn = sql.connect(self.filename)
		conn.execute("PRAGMA journal_mode=WAL")
		return conn

	def add(self, mid, topic, payload):
		'''
		Write a new message to the persistent buffer, updating it if <id> already exists.
		:param int mid: The message identifier
		:param str topic: The topic of the message
		:param str payload: The payload of the message
		:return: None if success and an error message otherwise
		'''
		self.printlog('debug', f"PersistentBuffer.add({str(mid)}, {str(topic)}, {str(payload)}) : Message stored.")		
		with self.conn_mutex:
			with self.get_conn() as conn:
				conn.execute(f"INSERT INTO {self.TABLE_NAME} (mid, topic, payload) VALUES (?, ?, ?)", (mid, topic, payload))
				conn.commit()

	def remove(self, mid, topic, payload):
		'''
		Remove a existing message from the persistent buffer.
		Doesn't raise error if the message does not exist.
		:param int mid: The message identifier
		:param str topic: The topic of the message
		:param str payload: The payload of the message
		'''
		with self.conn_mutex:
			with self.get_conn() as conn:
				cur = conn.execute(f"DELETE FROM {self.TABLE_NAME} WHERE mid = ? AND topic = ? AND payload = ?", (mid, topic, payload))
				conn.commit()
				if cur.rowcount > 0 :
					self.printlog('debug', f"PersistentBuffer.remove({str(mid)}, {str(topic)}, {str(payload)}): Message removed.")

	def pop_one(self):
		'''
		Read and delete one message stored in the persistent buffer.
		:return: {'mid':<>, 'topic':<>, 'payload':<>} or None
		'''
		with self.conn_mutex:
			with self.get_conn() as conn:
				cur = conn.execute(f"DELETE FROM {self.TABLE_NAME} WHERE id IN ( SELECT id FROM {self.TABLE_NAME} ORDER BY id LIMIT 1 ) RETURNING mid, topic, payload")
				ret = cur.fetchone()
				conn.commit()
				if ret:
					self.printlog('debug', f"PersistentBuffer.pop_one(): Message popped {ret}.")
				return dict(zip(["mid", "topic", "payload"], ret)) if ret else None

	def get_length(self):
		'''
		:return: buffer length = number of message stored
		'''
		with self.conn_mutex:
			with self.get_conn() as conn:
				cur = conn.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
				return cur.fetchone()[0]
