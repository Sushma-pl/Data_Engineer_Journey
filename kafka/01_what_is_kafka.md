# 01 — What is Kafka?

## The Simple Answer

Apache Kafka is a **distributed event streaming platform**. It lets applications publish events, store them durably and in order, and let any number of other applications consume those events — in real time or later.

Think of it as a **central nervous system** for your data architecture. Every system that produces data writes to Kafka. Every system that needs data reads from Kafka. Nothing talks to anything else directly.

---

## What is an Event?

An event is a record that something happened. It has:

```
Key:       "user-42"
Value:     {"action": "add_to_cart", "product_id": "p-101", "qty": 2}
Timestamp: 2026-10-24T14:32:00Z
Headers:   {"source": "web-app", "version": "2.1"}
```

- **Key** — identifies what the event is about (a user, an order, a device). Controls which partition the event lands in.
- **Value** — the actual data. Usually JSON, Avro, or Protobuf.
- **Timestamp** — when it happened (set by producer or Kafka broker).
- **Headers** — optional metadata (routing info, schema version, trace IDs).

Events are **immutable** — once written, they cannot be changed. Kafka is an append-only log.

---

## Event Streaming vs Traditional Messaging

You may have used message queues before (RabbitMQ, AWS SQS). Kafka is different in a fundamental way:

| | Traditional Queue | Kafka |
|---|---|---|
| Message storage | Deleted after consumed | Retained (configurable — days, weeks, forever) |
| Multiple consumers | One consumer per message | Every consumer group reads all messages |
| Replay | Not possible | Yes — rewind to any point |
| Order guarantee | Usually FIFO | Per partition |
| Throughput | Thousands/sec | Millions/sec |
| Primary use | Task queues, work distribution | Event streaming, data pipelines |

The key insight: **Kafka is a log, not a queue.** Messages don't disappear when they're read. This makes it fundamentally different from RabbitMQ or SQS.

---

## Why Data Engineers Use Kafka

### Decouple producers from consumers

Without Kafka:
```
Order Service ──────────────────────► Warehouse Loader
             └──────────────────────► Email Service
             └──────────────────────► Analytics Service
```
Every producer must know about every consumer. Adding a new consumer means changing the producer.

With Kafka:
```
Order Service ──► Kafka topic: orders ──► Warehouse Loader
                                      ──► Email Service
                                      ──► Analytics Service
                                      ──► (add new consumer, zero changes to producer)
```

### Real-time data pipelines

Replace nightly batch ETL with continuous streaming:
```
Postgres ──CDC──► Kafka ──► Snowflake (updated in seconds, not hours)
```

### Buffer for spikes

Kafka absorbs bursts. If your warehouse loader can process 10,000 events/sec but a flash sale generates 500,000 events in a minute, Kafka buffers the excess and your consumer catches up at its own pace.

### Replay

Kafka retains events. If a consumer has a bug and processes events incorrectly, you fix the bug and replay from the beginning of the affected time window. With a queue, those messages are gone.

---

## Core Components

### Broker

A **broker** is a Kafka server. It receives events from producers, stores them on disk, and serves them to consumers. A production Kafka cluster has 3+ brokers for fault tolerance.

```
Kafka Cluster
├── Broker 1  (port 9092)
├── Broker 2  (port 9093)
└── Broker 3  (port 9094)
```

Every broker can handle reads and writes for any topic partition it hosts. If you connect to one broker, you can discover all the others (every broker is a **bootstrap server**).

### Topic

A **topic** is a named log where events are stored. Producers write to topics, consumers read from topics.

```
Topic: "orders"
  └── an ordered, immutable, append-only log of order events
```

Think of it like a database table, but for events — append-only, with a retention policy instead of permanent storage.

### Partition

Topics are split into **partitions** — multiple parallel logs stored across different brokers. Partitions are the unit of parallelism in Kafka.

```
Topic: orders (3 partitions)
  Partition 0: [event_1, event_4, event_7, ...]  → stored on Broker 1
  Partition 1: [event_2, event_5, event_8, ...]  → stored on Broker 2
  Partition 2: [event_3, event_6, event_9, ...]  → stored on Broker 3
```

More partitions = more parallelism = higher throughput.

### Producer

A **producer** is any application that writes events to Kafka. Your streaming pipeline, Debezium, your web app, an IoT sensor — all producers.

### Consumer

A **consumer** is any application that reads events from Kafka. Your warehouse loader, your ML pipeline, your alerting system — all consumers.

### Consumer Group

A **consumer group** is a set of consumers that cooperate to read a topic. Each partition is read by exactly one consumer in the group. This is how Kafka scales reads.

### Kafka Connect

A framework for connecting Kafka to external systems without writing code. Debezium is a Kafka Connect source connector. Connectors for S3, Elasticsearch, JDBC, BigQuery etc. are sink connectors.

### ZooKeeper / KRaft

Historically, Kafka used **ZooKeeper** to manage cluster metadata (which broker is the leader for each partition, etc.). Modern Kafka (2.8+) replaces ZooKeeper with **KRaft** (Kafka Raft) — Kafka manages its own metadata internally. You'll still see ZooKeeper in many examples and older deployments.

---

## How Kafka Fits in a Data Engineering Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Sources                                 │
│  Postgres  │  MySQL  │  Web App  │  IoT Sensors  │  Clickstream │
└─────┬───────────┬──────────┬──────────┬──────────────┬──────────┘
      │           │          │          │              │
      └───────────┴──────────┴──────────┴──────────────┘
                                │
                          Kafka Connect
                       (Debezium, etc.)
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Apache Kafka                                │
│   topics: orders │ users │ clicks │ sensor-readings │ payments  │
└─────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
     Kafka Streams        Spark Streaming       Consumers
     (real-time          (batch + stream)      (your code)
      processing)              │                   │
            │                  ▼                   ▼
            └──────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Data Destinations                            │
│  Snowflake │  BigQuery │  Elasticsearch │  Redis │  S3  │  DB   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Kafka Guarantees

Understanding what Kafka promises (and doesn't promise) is essential for building correct pipelines:

| Guarantee | What it means |
|-----------|--------------|
| **Ordering within a partition** | Events with the same key go to the same partition and are read in order |
| **Durability** | Written events are persisted to disk and replicated |
| **At-least-once delivery** | Default — consumers may see duplicates after failure |
| **Exactly-once** | Possible with Kafka transactions, but complex |
| **High throughput** | Millions of events/sec with the right configuration |
| **Fault tolerance** | A broker can fail without losing data (with replication) |

---

## What Confused Me

- **Kafka is not a database.** You can't query it with SQL. You can't say "give me all orders from user 42." You read forward from an offset. For queryable state, you consume into a database.
- **A message is not deleted when consumed.** This was counterintuitive coming from queues. Messages stay until the retention period expires. Multiple consumer groups each read the full topic independently.
- **ZooKeeper vs KRaft confusion.** Most tutorials still use ZooKeeper. Modern Kafka (3.x) uses KRaft. In Docker, `confluentinc/cp-kafka` images still support Zookeeper mode, which is what most examples use.
- **Topic ≠ Queue.** A queue delivers each message to exactly one consumer. A Kafka topic delivers messages to every consumer group. One producer, many independent consumers.

---

## Resources

- [Confluent Introduction to Kafka](https://docs.confluent.io/kafka/introduction.html)
- [Kafka official documentation](https://kafka.apache.org/documentation/)
- [Kafka in a Nutshell (article)](https://sookocheff.com/post/kafka/kafka-in-a-nutshell/)

---

*Next: [02 — Topics, Partitions and Offsets](./02_topics_partitions_offsets.md)*

*Back to [Kafka index](./README.md)*
