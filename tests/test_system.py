import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from broker.broker import Broker
from client.producer import Producer
from client.consumer import Consumer
from client.client import Client
from broker.broker import Broker
from client.producer import Producer
from client.consumer import Consumer
from client.client import Client
import shutil
shutil.rmtree("data", ignore_errors=True)

def main():
    broker = Broker(broker_id=1, host="localhost", port=9092, zk_host="localhost", zk_port=2181)
    broker.start()
    time.sleep(1)

    # create topic
    client = Client("localhost", 9092)
    client.connect()
    client.create_topic("test-topic", 1, 1)

    # produce 5 messages
    producer = Producer("localhost", 9092, "test-topic")
    for i in range(5):
        offset = producer.send(f"message {i}")
        print(f"Sent message {i} at offset {offset}")

    time.sleep(1)

    # consume messages
    consumer = Consumer("localhost", 9092, "test-topic", 0)
    messages = consumer.poll()
    for msg in messages:
        print(f"Received: {msg.decode('utf-8')}")

    broker.stop()

if __name__ == "__main__":
    main()