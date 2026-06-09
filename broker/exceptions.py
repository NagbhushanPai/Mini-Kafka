class MiniKafkaError(Exception):
    pass


class TopicNotFoundError(MiniKafkaError):
    pass


class PartitionNotFoundError(MiniKafkaError):
    pass


class TopicAlreadyExistsError(MiniKafkaError):
    pass

