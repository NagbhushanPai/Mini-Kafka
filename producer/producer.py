class Producer:
    def __init__(self, broker):
        self.broker = broker

    def send(self, topic, key, value):
        return self.broker.produce(topic, key, value)

