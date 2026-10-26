# 03 — Producers

## The Simple Answer

A producer is any application that writes events to Kafka. You tell it which topic to write to, give it a key and a value, and it handles the rest — picking a partition, batching for efficiency, retrying on failure, and confirming the write succeeded.

Understanding how producers make decisions is important because those decisions affect ordering, throughput, and durability.

---

## How a Producer Works — Internals

When you call `producer.send(topic, key, value)`, several things happen before the event hits Kafka:

```
Your code
  │
  │  producer.send("orders", key="order-1", value={...})
  ▼
┌──────────────────────────────────────────────────┐
│              Producer Internals                   │
│                                                  │
│  1. Serializer     → converts key/value to bytes │
│  2. Partitioner    → decides which partition     │
│  3. Record buffer  → batches records             │
│  4. Sender thread  → flushes batches to broker   │
└──────────────────┬───────────────────────────────┘
                   │  batch of records
                   ▼
              Kafka Broker
```

These steps happen asynchronously. Your code doesn't wait for each event to hit the broker — the producer accumulates a batch and sends it, which is how Kafka achieves high throughput.

---

## Partitioning — Where Does the Event Go?

The producer decides which partition an event lands in. This matters because **partition = ordering unit** and **partition = consumer assignment**.

### Rule 1: No key → round-robin across partitions

```python
producer.send("orders", value={"status": "placed"})
# goes to partition 0, then 1, then 2, then 0 again...
```

Events are spread evenly. Good for throughput, but no ordering guarantee between events.

### Rule 2: With key → hash(key) % num_partitions

```python
producer.send("orders", key="order-1", value={"status": "placed"})
producer.send("orders", key="order-1", value={"status": "shipped"})
# both always go to the same partition (say partition 2)
# → guaranteed to be read in order
```

Same key **always** maps to the same partition. This is the fundamental way to preserve ordering for a stream of related events.

```
key="order-1"  → hash → partition 2
key="order-2"  → hash → partition 0
key="order-3"  → hash → partition 1
key="order-1"  → hash → partition 2  ← same as before, always
```

### Rule 3: Explicit partition

```python
producer.send("orders", partition=0, value={"status": "placed"})
# forces partition 0, regardless of key
```

Rarely used in practice — usually you let the key decide.

### What to use as a key?

| Scenario | Good key |
|----------|---------|
| Order events | `order_id` |
| User events | `user_id` |
| CDC events (Debezium) | primary key of the row |
| IoT sensor readings | `device_id` |
| Log events (no ordering needed) | `null` (round-robin) |

---

## Acknowledgements — How Sure Are You the Write Succeeded?

The `acks` setting controls how many broker acknowledgements the producer waits for before considering a write successful.

### `acks=0` — Fire and forget

```
Producer ──► Broker  (doesn't wait for any response)
```

- Fastest possible throughput
- No guarantee the event was stored
- Data loss is possible (broker crashes before writing)
- Use case: high-volume metrics where losing a few events is acceptable (rare for data engineering)

### `acks=1` — Leader acknowledges

```
Producer ──► Leader Broker ──► "got it" ──► Producer continues
                └──► Follower Brokers (async, producer doesn't wait)
```

- Good balance of speed and safety
- Data loss if the leader crashes before replicating to followers
- Default in many clients

### `acks=all` (or `acks=-1`) — All in-sync replicas acknowledge

```
Producer ──► Leader Broker
                ├──► Follower 1 ──► "replicated"
                └──► Follower 2 ──► "replicated"
                          │
                  Leader ──► "all ISR confirmed" ──► Producer continues
```

- Strongest durability guarantee
- Slightly higher latency
- **Use this for data pipelines where data loss is unacceptable**
- Works together with `min.insync.replicas` on the broker side

```python
# Python (confluent-kafka)
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'retries': 5,
    'retry.backoff.ms': 300
})
```

---

## Batching and Compression

Kafka producers batch records before sending. This is key to high throughput.

```
Your code calls send() 1000 times quickly
  → producer accumulates in buffer
  → sends as one batch to broker
  → much more efficient than 1000 individual network round trips
```

### Key batching settings

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 10,        # wait up to 10ms to accumulate a batch (default: 0)
    'batch.size': 65536,    # max batch size in bytes (default: 16KB)
    'compression.type': 'lz4',  # compress batches (none|gzip|snappy|lz4|zstd)
})
```

| Setting | Effect |
|---------|--------|
| `linger.ms=0` | Send immediately (low latency, small batches) |
| `linger.ms=10` | Wait 10ms to fill the batch (higher latency, better throughput) |
| `compression.type=lz4` | Compress batches — less network and disk, slightly more CPU |

**For data pipelines:** use `linger.ms=5-20` and `compression.type=lz4` for a good throughput/latency balance.

---

## Retries and Idempotence

### Retries

Networks fail. Brokers restart. The producer retries automatically:

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'retries': 10,              # retry up to 10 times
    'retry.backoff.ms': 100,    # wait 100ms between retries
    'delivery.timeout.ms': 120000,  # total time allowed for delivery (2 min)
})
```

Retries can cause **duplicates** — if the broker wrote the event but the ack was lost in transit, the producer retries and sends it again.

### Idempotent producer

Enable idempotence to prevent duplicates from retries:

```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,   # automatically sets acks=all, retries=MAX
})
```

With idempotence enabled, Kafka assigns each producer session a unique ID and a sequence number to each message. Duplicates from retries are detected and discarded by the broker.

**Always enable idempotence for data pipelines.**

---

## A Real Producer in Python

```python
from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    """Called once per message when delivery succeeds or fails."""
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'linger.ms': 5,
    'compression.type': 'lz4',
})

orders = [
    {"order_id": 1, "user_id": 42, "product": "Laptop", "status": "placed"},
    {"order_id": 2, "user_id": 17, "product": "Mouse",  "status": "placed"},
    {"order_id": 1, "user_id": 42, "product": "Laptop", "status": "shipped"},
]

for order in orders:
    producer.produce(
        topic="orders",
        key=str(order["order_id"]),     # order_id as key → same order → same partition
        value=json.dumps(order).encode("utf-8"),
        on_delivery=delivery_report
    )
    producer.poll(0)  # trigger delivery report callbacks

# wait for all outstanding messages to be delivered
producer.flush()
print("All messages delivered.")
```

### Run it against Docker

```bash
# install confluent-kafka
pip install confluent-kafka

# make sure your docker-compose kafka is running on localhost:9092
# create the topic first
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

# run the producer
python producer.py
```

---

## CLI Producer (for quick testing)

```bash
# simple producer — type messages, press Enter to send
docker exec -it kafka \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders

# producer with keys — format is key:value
docker exec -it kafka \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"

# type:
# order-1:{"status":"placed"}
# order-1:{"status":"shipped"}
# order-2:{"status":"placed"}
```

---

## Producer Metrics to Watch

```python
# in Python, poll() returns per-message delivery stats
# for monitoring at scale, track:
# - delivery success rate (failed deliveries / total)
# - p99 delivery latency
# - record-send-rate (messages/sec)
# - batch-size-avg (efficiency indicator)
```

On the broker side:
```bash
# messages in per second to a topic
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092\
  --describe --topic orders
```

---

## What Confused Me

- **`producer.send()` is asynchronous.** Calling `send()` does NOT immediately write to Kafka. It adds to an internal buffer. Call `flush()` before exiting or you'll lose the last batch.
- **Keys are bytes, not strings.** The partitioner hashes the raw bytes. If you send key `"order-1"` from Python (UTF-8) and from Java (different serialization), they may map to different partitions. Be consistent with key serialization.
- **`acks=1` is not safe enough for pipelines.** An event acknowledged by the leader can still be lost if the leader crashes before replicating. Use `acks=all` + `min.insync.replicas=2` in production.
- **Retries without idempotence = duplicates.** Enable idempotence. It's free (just a config flag) and prevents a whole class of bugs in your consumers.
- **Partition count changes break key-partition mapping.** If you add partitions, `hash(key) % num_partitions` changes — the same key now maps to a different partition. This breaks ordering for that key across the partition change. Plan partition counts upfront.

---

## Resources

- [Kafka producer docs](https://kafka.apache.org/documentation/#producerapi)
- [confluent-kafka Python docs](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Idempotent producer deep dive](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)

---

*Next: [04 — Consumers and Consumer Groups](./04_consumers_and_consumer_groups.md)*

*Back to [Kafka index](./README.md)*
