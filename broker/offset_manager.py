from .exceptions import AssignmentNotFound


class OffsetManager:
    def __init__(self, group_manager):
        self.group_manager = group_manager

    def commit_offset(self, group_id, topic, partition, offset):
        return self.group_manager.commit_offset(group_id, topic, partition, offset)

    def get_offset(self, group_id, topic, partition):
        return self.group_manager.get_offset(group_id, topic, partition)

