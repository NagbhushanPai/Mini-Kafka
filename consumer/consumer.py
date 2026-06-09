class Consumer:
    def __init__(self, broker):
        self.broker = broker

    def poll(self, topic, partition, offset, batch_size):
        return self.broker.consume(topic, partition, offset, batch_size)

