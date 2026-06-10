from client.kafka_client import KafkaClient


def main():
    client = KafkaClient()
    try:
        client.create_topic("notifications", 2)
        client.produce("notifications", "user-1", {"event": "UserSignedUp", "channel": "email"})
        print("Notification topic seeded with signup event.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

