class Producer:
    def __init__(self, client):
        self.client = client

    def send(self, topic, key, value):
        return self.client.produce(topic, key, value)
