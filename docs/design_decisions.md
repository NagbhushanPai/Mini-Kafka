# Design Decisions

## Why Append-Only Logs?

MiniKafka stores records in append-only logs because sequential writes are simple, fast, and predictable. This makes replay straightforward and keeps recovery logic easy to reason about after a restart.

## Why Partitioning?

Partitioning gives the system horizontal processing within a single broker process. It also preserves ordering within a partition while allowing multiple partitions to be consumed in parallel.

## Why Consumer Groups?

Consumer groups let multiple consumers share work without duplicating every message to every instance. That enables horizontal scaling while keeping processing responsibility balanced.

## Why Offset Commit?

Offsets let each consumer resume from its last successful position after a restart or failure. That is the core mechanism that makes replay and recovery practical.

## Why Heartbeats?

Heartbeats let the broker distinguish healthy consumers from dead ones without waiting for an explicit shutdown. That is what enables automatic timeout detection and rebalance.

