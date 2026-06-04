import socket
import threading
import struct
import os
from broker.zookeeper_client import ZookeeperClient
from broker.protocol import PRODUCE, FETCH, METADATA, CREATE_TOPIC, PRODUCE_RESPONSE, FETCH_RESPONSE, METADATA_RESPONSE
from broker.partition import Partition

class Broker:
    def __init__(self, broker_id, host, port, zk_host, zk_port):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.zk_host = zk_host
        self.zk_port = zk_port
        self.topics = {}
        self.running = False
        self.zk_client = None
    
    def start(self):
        self.running = True
        self.zk_client = ZookeeperClient(self.zk_host, self.zk_port)
        self.zk_client.connect() 
        self.zk_client.register_broker(self.broker_id, self.host, self.port)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        threading.Thread(target=self._accept_connections, daemon=True).start()

    def _accept_connections(self):
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
            except OSError:
                break
            
    def _handle_client(self, client_socket):
        while self.running:
            msg_type = client_socket.recv(1)
            if not msg_type:
                break 
            msg_type = msg_type[0]  
            if msg_type == PRODUCE:
                self._handle_produce(client_socket)
            elif msg_type == FETCH:
                self._handle_fetch(client_socket)
            elif msg_type == METADATA:
                self._handle_metadata(client_socket)
            elif msg_type == CREATE_TOPIC:
                self._handle_create_topic(client_socket)
    
    def _handle_produce(self, client_socket):
        length_bytes = client_socket.recv(2)
        (length,) = struct.unpack(">H", length_bytes)
        topic = client_socket.recv(length).decode("utf-8")
        part_num_len = client_socket.recv(4)
        (part_num,) = struct.unpack(">i", part_num_len)
        msg_len_bytes = client_socket.recv(4)
        (msg_len,) = struct.unpack(">i", msg_len_bytes)
        content = client_socket.recv(msg_len)
        partition = self.topics[topic][part_num]
        offset = partition.append(content)
        response = struct.pack(">B", PRODUCE_RESPONSE) + struct.pack(">q", offset) + struct.pack(">B", 0)
        client_socket.sendall(response)

    def _handle_fetch(self, client_socket):
        topic_len = client_socket.recv(2)
        (length,) = struct.unpack(">H", topic_len)
        topic = client_socket.recv(length).decode("utf-8")
        part_num_len = client_socket.recv(4)
        (part_num,) = struct.unpack(">i", part_num_len)
        offset_len = client_socket.recv(8)
        (offset,) = struct.unpack(">q", offset_len)
        max_bytes_len = client_socket.recv(4)
        (max_bytes,) = struct.unpack(">i", max_bytes_len)
        partition = self.topics[topic][part_num]
        messages = partition.read_messages(offset, max_bytes)
        response = struct.pack(">B", FETCH_RESPONSE) + struct.pack(">i", len(messages))
        for i, msg in enumerate(messages):
            response += struct.pack(">q", offset + i)  
            response += struct.pack(">i", len(msg))
            response += msg
        client_socket.sendall(response)
        
    def _handle_metadata(self, client_socket):
        brokers = self.zk_client.get_brokers()
        topics = list(self.topics.keys())
        response = struct.pack(">B", METADATA_RESPONSE) 
        response += struct.pack(">i", len(brokers))
        for broker in brokers:
            response += struct.pack(">i", broker["id"])
            host = broker["host"].encode("utf-8")
            response += struct.pack(">H", len(host)) + host
            response += struct.pack(">i", broker["port"])
        response += struct.pack(">i", len(topics))
        for topic in topics:
            encoded = topic.encode("utf-8")
            response += struct.pack(">H", len(encoded)) + encoded
        client_socket.sendall(response)

    def _handle_create_topic(self, client_socket):
        topic_len = client_socket.recv(2)
        (length,) = struct.unpack(">H", topic_len)
        topic = client_socket.recv(length).decode("utf-8")
        num_part_len = client_socket.recv(4)
        (num_part,) = struct.unpack(">i", num_part_len)
        repli_factor_len = client_socket.recv(2)
        (repli_factor,) = struct.unpack(">H", repli_factor_len)
        self.topics[topic] = []
        for i in range(num_part):
            p = Partition(id=i, leader=self.broker_id, base_dir=f"data/{topic}/{i}")
            p.initialize()
            self.topics[topic].append(p)

        self.zk_client.create_topic(topic, num_part, repli_factor)
        client_socket.sendall(struct.pack(">B", 0)) 
    
    def stop(self):
        self.running = False
        self.server_socket.close()
        self.zk_client.disconnect()


