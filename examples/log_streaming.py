from client.kafka_client import KafkaClient


def main():
    client = KafkaClient()
    try:
        client.create_topic("application-logs", 4)
        client.produce("application-logs", "service-a", {"level": "INFO", "message": "service started"})
        print("Log streaming topic seeded with a sample log line.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

