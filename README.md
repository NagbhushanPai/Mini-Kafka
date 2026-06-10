# Mini Kafka

A lightweight event streaming platform inspired by Apache Kafka, built from scratch to understand distributed systems fundamentals such as event streaming, topic partitioning, log-based storage, offset management, and concurrent consumer processing.

## Overview

Mini Kafka is a simplified implementation of an event streaming system that follows the producer-consumer architecture. The project focuses on core Kafka concepts rather than production-scale distributed deployment.

### Key Concepts Implemented

* Producer-Consumer Architecture
* Topic-Based Messaging
* Topic Partitioning
* Append-Only Log Storage
* Offset Tracking
* Consumer Groups
* Concurrent Consumer Processing
* Message Replay
* Fault Recovery
* Performance Benchmarking

---

## Architecture

```text
+-----------+
| Producer  |
+-----------+
      |
      v
+----------------+
| Mini Kafka     |
| Broker         |
+----------------+
      |
      +--------------------------+
      |                          |
      v                          v

+-------------+          +-------------+
| Partition 0 |          | Partition 1 |
+-------------+          +-------------+
      |                          |
      v                          v

 Append-only Logs        Append-only Logs

      |
      v

+-----------+
| Consumer  |
+-----------+
```

---

## Features

### Event Production

Publish messages to topics through producers.

```python
producer.send(
    topic="orders",
    message="Order Created"
)
```

### Topic Partitioning

Messages are distributed across partitions using a partitioning strategy.

```python
partition = hash(key) % num_partitions
```

Benefits:

* Improved scalability
* Parallel processing
* Ordered processing per key

### Log-Based Storage

Messages are stored in append-only logs.

```text
offset,message

0,Order Created
1,Payment Received
2,Order Shipped
```

### Offset Management

Consumers track their progress using offsets.

```json
{
  "consumer_1": {
    "orders-0": 15
  }
}
```

### Replay Capability

Consumers can reprocess historical messages.

```python
consumer.seek(0)
```

### Concurrent Consumers

Multiple consumers can process messages simultaneously using multi-threading.

```text
Partition 0 -> Consumer Thread 1
Partition 1 -> Consumer Thread 2
Partition 2 -> Consumer Thread 3
```

### Fault Recovery

Offsets are persisted to disk, enabling consumers to resume processing after failures.

---

## Project Structure

```text
mini-kafka/

├── broker/
│   ├── broker.py
│   ├── topic_manager.py
│   ├── partition.py
│   ├── storage.py
│   ├── offset_manager.py
│   └── group_manager.py
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── benchmark/
│   └── benchmark.py
│
├── data/
│   └── topics/
│
├── offsets/
│   └── consumer_offsets.json
│
├── tests/
│   └── test_broker.py
│
├── requirements.txt
└── README.md
```

---

## Message Flow

### Producing Messages

```text
Producer
   |
   v
Broker
   |
   v
Topic Partition
   |
   v
Append to Log
```

### Consuming Messages

```text
Consumer
   |
Read Offset
   |
Read Messages
   |
Process Messages
   |
Commit Offset
```

---

## Storage Format

Messages are stored as JSON Lines.

```json
{"offset":0,"timestamp":"2026-01-01T10:00:00","message":"hello"}
{"offset":1,"timestamp":"2026-01-01T10:00:01","message":"world"}
```

Advantages:

* Human-readable
* Easy debugging
* Efficient sequential reads
* Supports replay

---

## Benchmarking

The project includes benchmarking tools to simulate real-world workloads.

Metrics:

* Throughput (messages/sec)
* Consumer Lag
* Processing Latency
* Partition Distribution
* Resource Utilization

Example:

```bash
python benchmark/benchmark.py
```

---

## Technology Stack

* Python 3.11+
* Socket Programming
* Threading
* Concurrent Futures
* JSON
* File-Based Storage
* Pytest

---

## Learning Outcomes

This project demonstrates practical understanding of:

* Distributed Systems Fundamentals
* Event Streaming Architectures
* Message Queues
* Concurrent Programming
* Log-Based Storage Systems
* Fault Tolerance Mechanisms
* Consumer Group Coordination
* System Performance Analysis

---

## Future Enhancements

* Broker Replication
* Leader-Follower Architecture
* Distributed Brokers
* Raft-Based Consensus
* Message Compression
* Retention Policies
* Dead Letter Queues
* Web Dashboard
* Prometheus Metrics
* Docker Deployment

## Dashboard

Run the live recruiter-facing dashboard with:

```bash
python -m dashboard --broker-port 9092 --port 8000
```

Then open `http://localhost:8000` to view:

* Topic and partition layout
* Current offsets and consumer groups
* Sample messages per partition
* A metrics snapshot for the broker

---

## Inspiration

This project is inspired by Apache Kafka and is intended as an educational implementation to understand the internal mechanics of modern event streaming platforms.
