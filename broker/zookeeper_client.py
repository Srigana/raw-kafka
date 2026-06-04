from kazoo.client import KazooClient
import json
import kazoo.exceptions


class ZookeeperClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.zk = None
    
    def connect(self):
        self.zk = KazooClient(hosts=f"{self.host}:{self.port}")
        self.zk.start()
        self.zk.ensure_path("/brokers")
        self.zk.ensure_path("/topics")
        self.zk.ensure_path("/controller")

    def disconnect(self):
        self.zk.stop()

    def register_broker(self, broker_id, host, port):
        data = json.dumps({"host": host, "port": port})
        data_bytes = data.encode("utf-8")
        try:
            self.zk.create(f"/brokers/{broker_id}", data_bytes, ephemeral=True)
        except kazoo.exceptions.NodeExistsError:
            pass
    
    def get_brokers(self):
        ids = self.zk.get_children("/brokers")
        brokers = []
        for broker_ids in ids:
            data_bytes, stat = self.zk.get(f"/brokers/{broker_ids}")
            data_str = data_bytes.decode("utf-8")
            broker = json.loads(data_str)
            brokers.append(broker)
        return brokers

    def create_topic(self, topic_name, num_partitions, replication_factor):
        data = json.dumps({"partitions": num_partitions, "replication_factor": replication_factor})
        encode = data.encode("utf-8")
        try:
            self.zk.create(f"/topics/{topic_name}", encode)
        except kazoo.exceptions.NodeExistsError:
            pass
    
    def watch_brokers(self, callback):
        self.zk.ChildrenWatch("/brokers", callback)


        

