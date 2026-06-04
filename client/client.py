import socket
from broker.protocol import (
    encode_produce_request, decode_produce_response,
    encode_fetch_request, decode_fetch_response, encode_create_topic_request
)

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def disconnect(self):
        self.socket.close()
    
    def send(self, topic, partition, message):
        data = encode_produce_request(topic, partition, message)
        self.socket.sendall(data)
        response = self.socket.recv(10)
        offset = decode_produce_response(response)
        return offset

    def fetch(self, topic, partition, offset, max_bytes):
        data = encode_fetch_request(topic, partition, offset, max_bytes)
        self.socket.sendall(data)
        response = self.socket.recv(4096)
        message = decode_fetch_response(response)
        return message
    
    def create_topic(self, topic, num_partitions, replication_factor):
        data = encode_create_topic_request(topic, num_partitions, replication_factor)
        self.socket.sendall(data)
        self.socket.recv(1) 
