from client.client import Client

class Consumer:
    def __init__(self, host, port, topic, partition):
        self.host = host
        self.port = port
        self.topic = topic
        self.partition = partition
        self.current_offset = 0
        self.client = Client(host, port)
        self.client.connect()

    def poll(self):
        messages = self.client.fetch(self.topic, self.partition, self.current_offset, 1024*1024)
        if messages:
            self.current_offset += len(messages)
        return messages

    def seek(self, offset):
        self.current_offset = offset

    def get_current_offset(self):
        return self.current_offset



