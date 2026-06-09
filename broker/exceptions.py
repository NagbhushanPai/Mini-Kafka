class MiniKafkaError(Exception):
    pass


class TopicNotFoundError(MiniKafkaError):
    pass


class PartitionNotFoundError(MiniKafkaError):
    pass


class TopicAlreadyExistsError(MiniKafkaError):
    pass


class GroupNotFound(MiniKafkaError):
    pass


class ConsumerNotFound(MiniKafkaError):
    pass


class OffsetNotFound(MiniKafkaError):
    pass


class AssignmentNotFound(MiniKafkaError):
    pass


class OffsetNotFound(MiniKafkaError):
    pass
