from client.kafka_client import KafkaClient


def main():
    client = KafkaClient()
    try:
        client.create_topic("orders", 3)
        client.produce("orders", "order-1", {"event": "OrderCreated", "order_id": "order-1"})
        client.produce("orders", "order-2", {"event": "OrderCreated", "order_id": "order-2"})
        print("Order processing topic seeded with example events.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

