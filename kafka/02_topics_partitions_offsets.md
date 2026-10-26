# 02 — Topics, Partitions, and Offsets

## The Simple Answer

A **topic** is a named log of events. A **partition** is a slice of that log stored on a specific broker. An **offset** is a number that identifies the position of an event within a partition. Together, these three concepts define how Kafka stores and organizes data.

---

## Topics — The Fundamental Unit

A topic is like a database table for events — but append-only and with a retention policy instead of permanent storage.

```
Topic: "orders"
─────────────────────────────────────────────────────────► time
  [order placed] [order updated] [order shipped] [order delivered] ...
```

### Key characteristics

- **Append-only** — you can only add to the end, never modify or delete individual events
- **Immutable** — once written, an event cannot be changed
- **Multi-producer** — many producers can write to the same topic
- **Multi-consumer** — many consumer groups can independently read the same topic
- **Retention** — events are kept for a configurable time period or size, then deleted

### Creating topics

```bash
# create a topic with 3 partitions and replication factor 1 (single broker)
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# list all topics
kafka-topics --bootstrap-server localhost:9092 --list

# describe a topic (see partitions, leaders, replicas)
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

Output of `--describe`:
```
Topic: orders   TopicId: XqZ1...   PartitionCount: 3   ReplicationFactor: 1
  Topic: orders   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
  Topic: orders   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
  Topic: orders   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

---

## Partitions — Parallelism and Scale

A topic is split into **partitions**. Each partition is an independent, ordered log. Partitions are what make Kafka scalable.

```
Topic: orders  (3 partitions)

Partition 0:  [offset 0] [offset 1] [offset 2] [offset 3] ...
Partition 1:  [offset 0] [offset 1] [offset 2] ...
Partition 2:  [offset 0] [offset 1] [offset 2] [offset 3] [offset 4] ...
```

### Why partitions matter

**Parallelism for producers:**
Multiple producers (or a single producer) can write to different partitions simultaneously. Throughput scales with the number of partitions.

**Parallelism for consumers:**
Each partition is consumed by exactly one consumer in a consumer group. More partitions → more consumers can work in parallel → higher throughput.

```
Topic: orders (3 partitions)

Consumer Group "warehouse-loaders" (3 consumers):
  Consumer 1 reads Partition 0
  Consumer 2 reads Partition 1
  Consumer 3 reads Partition 2
  (all three run in parallel)
```

**Distribution across brokers:**
Partitions are spread across brokers. If a broker fails, only the partitions it hosted are temporarily unavailable (and replicas take over).

### Ordering guarantee

Events are ordered **within a partition**. There is **no ordering guarantee across partitions**.

```
Partition 0:  order-1 placed → order-1 updated → order-1 shipped  ✅ always in this order
Partition 1:  order-2 placed → order-2 updated

Across partitions: no guarantee which order these appear relative to each other
```

**This is why keys matter.** If you need all events for a given entity (e.g., a specific order) to be in order, you must route them to the same partition using a consistent key. Same key → same partition → guaranteed order for that entity.

### How many partitions?

This is one of the most common Kafka design questions. Rules of thumb:

| Scenario | Guidance |
|----------|---------|
| Starting out | 3–6 partitions per topic |
| High throughput (>100MB/s) | More partitions, benchmark first |
| Consumer group size | Max partitions = max useful consumers. More consumers than partitions = idle consumers |
| CDC topics (Debezium) | Match to your consumer parallelism needs |

Partitions can be **increased** but not decreased — plan accordingly.

---

## Offsets — Position in the Log

Every event in a partition gets a unique, sequential **offset** — an integer starting at 0.

```
Partition 0:
  offset 0: {"order_id": 1, "status": "placed"}
  offset 1: {"order_id": 1, "status": "confirmed"}
  offset 2: {"order_id": 5, "status": "placed"}
  offset 3: {"order_id": 1, "status": "shipped"}
  offset 4: {"order_id": 8, "status": "placed"}
             ▲
             next offset to be written: 5
```

### Offsets are per-partition

The same offset number (e.g., offset 2) exists in every partition, but they refer to different events:

```
Partition 0, offset 2: order-5 placed
Partition 1, offset 2: order-9 updated    ← different event, same offset number
Partition 2, offset 2: order-2 shipped
```

To uniquely identify an event: **topic + partition + offset**.

### Consumer offsets

A consumer tracks its position in each partition using offsets. After processing an event, the consumer **commits** its offset — telling Kafka "I've processed up to here."

```
Consumer reading Partition 0:
  Read offset 0 → process → commit offset 0
  Read offset 1 → process → commit offset 1
  Read offset 2 → process → CRASH

  Consumer restarts:
  Last committed offset: 1
  → resumes from offset 2 ✅
```

Committed offsets are stored in a special internal Kafka topic: `__consumer_offsets`.

### Offset reset policy

What happens when a consumer group reads a topic for the first time (no committed offset yet)?

```
# earliest — start from the very beginning of the topic
auto.offset.reset = earliest

# latest — start from now (only new events after the consumer starts)
auto.offset.reset = latest
```

Use `earliest` when you need to process all historical events (e.g., initial pipeline load).
Use `latest` when you only care about new events going forward.

---

## Retention — How Long Events Are Kept

Events are not kept forever. Kafka has two retention policies:

### Time-based retention

```bash
# keep events for 7 days (default)
retention.ms = 604800000   # 7 days in milliseconds

# keep for 1 day
kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=86400000
```

### Size-based retention

```bash
# keep max 1GB per partition
retention.bytes = 1073741824
```

Whichever limit is hit first triggers deletion of the oldest events.

### Retention for CDC topics

For Debezium CDC topics, decide based on:
- How long your consumers might be offline (set retention > max expected downtime)
- How far back you need to replay (set retention ≥ your replay window)
- A common setting: 7 days for operational topics, indefinite for audit/compliance topics

---

## Log Compaction — A Special Retention Mode

Normal retention deletes old events by age/size. **Log compaction** is different — it keeps only the **most recent event per key**.

```
Before compaction:
  offset 0: key="order-1" value={"status":"placed"}
  offset 1: key="order-2" value={"status":"placed"}
  offset 2: key="order-1" value={"status":"shipped"}    ← newer value for order-1
  offset 3: key="order-1" value={"status":"delivered"}  ← newest value for order-1

After compaction:
  offset 1: key="order-2" value={"status":"placed"}     ← only order ever seen
  offset 3: key="order-1" value={"status":"delivered"}  ← most recent for order-1
```

### When to use log compaction

- **Changelog topics** — the topic represents the latest state of records (like a database table)
- **CDC sink topics** — when you want to compact the change history to just the current state
- **Configuration topics** — when you want the latest config per key

```bash
# enable log compaction on a topic
kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name user-profiles \
  --add-config cleanup.policy=compact
```

### Delete tombstones

When you delete a record in a compacted topic, you write an event with the key and a **null value** (tombstone). The compactor eventually removes all events with that key.

```python
producer.send("user-profiles", key="user-42", value=None)  # marks user-42 as deleted
```

This is exactly what Debezium does when a row is deleted — it sends a tombstone event.

---

## Topic Configuration Reference

```bash
# view current config for a topic
kafka-configs --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name orders

# useful configs to know
retention.ms              # how long to keep events (default: 7 days)
retention.bytes           # max size per partition before deletion
cleanup.policy            # delete (default) or compact
min.insync.replicas       # min replicas that must ack a write (safety setting)
replication.factor        # copies of each partition across brokers
num.partitions            # default partition count for new topics
max.message.bytes         # max event size (default: 1MB)
```

---

## Practical: Exploring Topics in Docker

```bash
# exec into kafka container
docker exec -it kafka bash

# create a topic
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

# describe it
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# see the log files on disk (where Kafka actually stores data)
ls /var/lib/kafka/data/
# you'll see: orders-0/  orders-1/  orders-2/

# write some events
kafka-console-producer --bootstrap-server localhost:9092 --topic orders
> {"order_id": 1, "status": "placed"}
> {"order_id": 2, "status": "placed"}
> {"order_id": 1, "status": "shipped"}
> ^C

# read from the beginning
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning

# check consumer group offsets
kafka-consumer-groups --bootstrap-server localhost:9092 --list
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group
```

---

## What Confused Me

- **Offset 0 in partition 0 and offset 0 in partition 1 are different events.** The same offset number in different partitions refers to completely different events. An event is uniquely identified by topic + partition + offset.
- **No ordering guarantee across partitions.** If order-1 events go to partition 0 and order-2 events go to partition 1, you can't know which arrived "first" globally. If cross-event ordering matters, use a single partition (but lose the parallelism benefit).
- **You can't reduce partition count.** If you create a topic with 6 partitions and later want 3, you have to create a new topic and migrate. Err on the side of more partitions.
- **Log compaction doesn't happen immediately.** The compactor runs in the background. You'll still see old events for a period before they're cleaned up. Don't rely on compaction for real-time deduplication.
- **`auto.offset.reset = latest` means you miss past events.** First time I set up a consumer with `latest` and wondered why I wasn't seeing events that were already in the topic. Use `earliest` when replaying or bootstrapping.

---

## Resources

- [Kafka topics deep dive](https://docs.confluent.io/kafka/introduction.html)
- [Log compaction docs](https://kafka.apache.org/documentation/#compaction)
- [Partitioning strategies](https://www.confluent.io/blog/how-to-choose-the-number-of-topicspartitions-in-a-kafka-cluster/)

---

*Next: [03 — Producers](./03_producers.md)*

*Back to [Kafka index](./README.md)*
