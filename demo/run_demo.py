import argparse
import random
import threading
import time

from client.kafka_client import KafkaClient
from consumer.consumer import Consumer
from producer.producer import Producer

TOPIC = "demo-events"
GROUP_ID = "demo-group"

EVENT_TYPES = ["OrderCreated", "OrderShipped", "PaymentReceived", "InventoryUpdated", "OrderCancelled"]


def log(message):
    print(f"[demo] {message}", flush=True)


def build_parser():
    parser = argparse.ArgumentParser(description="Run a narrated MiniKafka demo against a live broker")
    parser.add_argument("--host", default="localhost", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")
    parser.add_argument("--partitions", type=int, default=3, help="Partitions for the demo topic")
    parser.add_argument("--messages", type=int, default=40, help="Number of messages to produce")
    parser.add_argument("--consumers", type=int, default=2, help="Number of consumers in the demo group")
    parser.add_argument("--delay", type=float, default=0.3, help="Seconds to sleep between produced messages")
    return parser


def ensure_topic(client, partitions):
    result = client.create_topic(TOPIC, partitions)
    if result.get("status") == "success":
        log(f"Created topic '{TOPIC}' with {partitions} partitions.")
    else:
        log(f"Topic '{TOPIC}' already exists — reusing it.")


def run_consumer(consumer_id, host, port, stop_event, poll_delay=0.5):
    client = KafkaClient(host=host, port=port)
    consumer = Consumer(client, group_id=GROUP_ID, consumer_id=consumer_id, topic=TOPIC)
    consumer.join_group()
    log(f"Consumer '{consumer_id}' joined group '{GROUP_ID}'.")
    try:
        while not stop_event.is_set():
            result = consumer.poll(batch_size=5)
            records = result.get("records", []) if isinstance(result, dict) else []
            next_offsets = {}
            for record in records:
                log(f"Consumer '{consumer_id}' read partition {record['partition']} offset {record['offset']}: {record['value']}")
                next_offsets[record["partition"]] = max(next_offsets.get(record["partition"], 0), record["offset"] + 1)
            for partition, offset in next_offsets.items():
                client.commit_offset(GROUP_ID, TOPIC, partition, offset)
            time.sleep(poll_delay)
    finally:
        consumer.leave_group()
        client.close()
        log(f"Consumer '{consumer_id}' left group '{GROUP_ID}'.")


def run_producer(host, port, message_count, delay):
    client = KafkaClient(host=host, port=port)
    producer = Producer(client)
    try:
        for i in range(message_count):
            order_id = f"order-{i + 1}"
            event = random.choice(EVENT_TYPES)
            value = {"event": event, "order_id": order_id, "amount": round(random.uniform(9.99, 249.99), 2)}
            result = producer.send(TOPIC, order_id, value)
            log(f"Produced {event} for {order_id} -> partition {result['partition']} offset {result['offset']}")
            time.sleep(delay)
    finally:
        client.close()


def main(argv=None):
    args = build_parser().parse_args(argv)

    log(f"Connecting to broker at {args.host}:{args.port} ...")
    setup_client = KafkaClient(host=args.host, port=args.port)
    try:
        ensure_topic(setup_client, args.partitions)
    finally:
        setup_client.close()

    stop_event = threading.Event()
    consumer_threads = [
        threading.Thread(target=run_consumer, args=(f"demo-consumer-{i + 1}", args.host, args.port, stop_event), daemon=True)
        for i in range(args.consumers)
    ]
    for thread in consumer_threads:
        thread.start()
    time.sleep(1)  # let consumers join and get partition assignments before the flood starts

    log(f"Producing {args.messages} events to '{TOPIC}' ...")
    run_producer(args.host, args.port, args.messages, args.delay)

    log("Draining remaining messages ...")
    time.sleep(2)
    stop_event.set()
    for thread in consumer_threads:
        thread.join(timeout=5)

    log("Demo complete. Open the dashboard to inspect topics, groups, and offsets.")


if __name__ == "__main__":
    main()
