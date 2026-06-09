class Consumer:
    def __init__(self, client):
        self.client = client

    def poll(self, topic, partition, offset, batch_size):
        return self.client.consume(topic, partition, offset, batch_size)
