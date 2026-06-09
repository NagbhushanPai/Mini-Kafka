from .exceptions import PartitionNotFoundError, TopicAlreadyExistsError, TopicNotFoundError


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

            return self._error("InvalidRequest", "Unknown request type")
        except KeyError:
            return self._error("InvalidRequest", "Missing required fields")
        except TopicNotFoundError as exc:
            return self._error("TopicNotFound", str(exc))
        except PartitionNotFoundError as exc:
            return self._error("PartitionNotFound", str(exc))
        except TopicAlreadyExistsError as exc:
            return self._error("TopicAlreadyExists", str(exc))
        except ValueError as exc:
            return self._error("InvalidRequest", str(exc))
        except Exception as exc:  # pragma: no cover - defensive fallback
            return self._error("InternalServerError", str(exc))

    def _error(self, error, message):
        return {"status": "error", "error": error, "message": message}

