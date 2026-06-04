class BrokerInfo:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port
    
    def __eq__(self, other):
        if not isinstance(other, BrokerInfo):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)
    
