class Consumer:
    def __init__(self, client, group_id, consumer_id, topic):
        self.client = client
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.topic = topic

    def join_group(self):
        return self.client.join_group(self.group_id, self.consumer_id, self.topic)

    def leave_group(self):
        return self.client.leave_group(self.group_id, self.consumer_id)

    def poll(self, batch_size=10):
        return self.client.consume_assigned(self.group_id, self.consumer_id, batch_size)
