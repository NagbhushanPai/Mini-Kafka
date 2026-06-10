# MiniKafka

MiniKafka is a from-scratch, Kafka-inspired event streaming system built in Python. It demonstrates the core mechanics behind modern stream processing systems: topic partitioning, append-only storage, consumer groups, offset management, heartbeats, automatic rebalance, and benchmark-driven validation.

## Overview

The goal of this project is to show how a broker can:

- accept producer and consumer traffic over TCP
- store records in durable partition logs
- balance work across consumer groups
- recover progress through offsets
- detect dead consumers automatically
- expose metrics and benchmark evidence for interviews

## Features

- Topics and partitions
- Append-only log storage
- Consumer groups and offset commits
- Automatic heartbeats and timeout-based rebalance
- Benchmark runner with reproducible scenarios
- Dockerized broker startup
- Recruiter-friendly dashboard
- Example applications for common streaming use cases

## Architecture

See the diagram set in [docs/diagrams](docs/diagrams/).

High-level flow:

```text
Producer -> TCP -> Broker Server -> Broker -> Topic -> Partition -> Storage -> Consumer
```

## Quick Start

```bash
git clone <repo-url>
cd mini_kafka
docker compose up --build
```

The broker listens on `localhost:9092`.

## Local Development

Run the broker without Docker:

```bash
python -m broker
```

Run the dashboard:

```bash
python -m dashboard --broker-port 9092 --port 8000
```

Run a benchmark:

```bash
python -m benchmark baseline
```

## Example Usage

Producer:

```python
from client.kafka_client import KafkaClient
from producer.producer import Producer

client = KafkaClient(port=9092)
producer = Producer(client)
client.create_topic("orders", 3)
producer.send("orders", "order-1", {"event": "OrderCreated"})
client.close()
```

Consumer:

```python
from client.kafka_client import KafkaClient
from consumer.consumer import Consumer

client = KafkaClient(port=9092)
consumer = Consumer(client, group_id="orders-group", consumer_id="consumer-a", topic="orders")
consumer.join_group()
records = consumer.poll(batch_size=10)
consumer.leave_group()
client.close()
```

## Benchmarks

Published benchmark evidence is available in [docs/benchmark_results.md](docs/benchmark_results.md).

Measured scenarios:

- 1 producer, 1 consumer, 10,000 messages
- 5 producers, 5 consumers, 50,000 messages
- 10 producers, 20 consumers, 100,000 messages
- 20 producers, 20 consumers, 16 partitions, 100,000 messages

## Engineering Metrics

- Unit tests: `pytest`
- Integration tests: TCP networking tests
- Benchmark results: `docs/benchmark_results.md`
- Architecture diagrams: `docs/diagrams/`
- Docker support: `Dockerfile` and `docker-compose.yml`

## Project Structure

```text
mini_kafka/
├── benchmark/
├── broker/
├── client/
├── consumer/
├── dashboard/
├── docs/
├── examples/
├── network/
├── producer/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Future Work

- Broker replication
- Leader election
- Multi-broker clusters
- Raft-based consensus
- Message compression
- Retention policies
- Dead-letter queues

## Resume Value

This project is designed to be easy to explain in interviews. It shows distributed systems thinking, backend engineering fundamentals, test coverage, operational visibility, and deployment readiness.

