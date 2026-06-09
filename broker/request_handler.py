from .exceptions import (
    AssignmentNotFound,
    ConsumerNotFound,
    GroupNotFound,
    PartitionNotFoundError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
)


class RequestHandler:
    def __init__(self, broker):
        self.broker = broker

    def handle(self, request):
        if not isinstance(request, dict):
            return self._error("InvalidRequest", "Request must be a JSON object")

        if request.get("version") != 1:
            return self._error("InvalidRequest", "Unsupported protocol version")

        request_type = request.get("type")
        try:
            if request_type == "create_topic":
                name = request["name"]
                partitions = request["partitions"]
                self.broker.create_topic(name, partitions)
                return {"status": "success"}

            if request_type == "produce":
                result = self.broker.produce(request["topic"], request["key"], request["value"])
                return {"status": "success", **result}

            if request_type == "consume":
                messages = self.broker.consume(
                    request["topic"],
                    request["partition"],
                    request["offset"],
                    request["batch_size"],
                )
                return {"status": "success", "messages": messages}

            if request_type == "join_group":
                assigned = self.broker.join_group(request["group_id"], request["consumer_id"], request["topic"])
                return {"status": "success", "assigned_partitions": assigned}

            if request_type == "leave_group":
                return self.broker.leave_group(request["group_id"], request["consumer_id"])

            if request_type == "commit_offset":
                return self.broker.commit_offset(
                    request["group_id"],
                    request["topic"],
                    request["partition"],
                    request["offset"],
                )

            if request_type == "get_offset":
                result = self.broker.get_offset(request["group_id"], request["topic"], request["partition"])
                return result

            if request_type == "consume_assigned":
                result = self.broker.consume_assigned(request["group_id"], request["consumer_id"], request["batch_size"])
                return result

            if request_type == "metrics":
                return self.broker.metrics()

            return self._error("InvalidRequest", "Unknown request type")
        except KeyError:
            return self._error("InvalidRequest", "Missing required fields")
        except TopicNotFoundError as exc:
            return self._error("TopicNotFound", str(exc))
        except PartitionNotFoundError as exc:
            return self._error("PartitionNotFound", str(exc))
        except TopicAlreadyExistsError as exc:
            return self._error("TopicAlreadyExists", str(exc))
        except GroupNotFound as exc:
            return self._error("GroupNotFound", str(exc))
        except ConsumerNotFound as exc:
            return self._error("ConsumerNotFound", str(exc))
        except AssignmentNotFound as exc:
            return self._error("AssignmentNotFound", str(exc))
        except ValueError as exc:
            return self._error("InvalidRequest", str(exc))
        except Exception as exc:  # pragma: no cover - defensive fallback
            return self._error("InternalServerError", str(exc))

    def _error(self, error, message):
        return {"status": "error", "error": error, "message": message}
