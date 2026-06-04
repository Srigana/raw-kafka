from .client import Client
from client.client import Client

class Producer:
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self.client = Client(host, port)
        self.client.connect()
    
    def send(self, message):
        msg = message.encode("utf-8")
        offset = self.client.send(self.topic, 0, msg)
        return offset

    def close(self):
        self.client.disconnect()
        
