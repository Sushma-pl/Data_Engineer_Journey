# 04 — Consumers and Consumer Groups

## The Simple Answer

A consumer reads events from Kafka topics. A **consumer group** is a set of consumers that share the work of reading a topic — each partition is read by exactly one consumer in the group, so the group processes the topic in parallel. Multiple groups can independently read the same topic, each getting every event.

---

## How a Consumer Reads

A consumer tracks its position in each partition using an **offset**. It reads events sequentially, processes them, and periodically commits its offset back to Kafka.

```
Partition 0:  [evt@0] [evt@1] [evt@2] [evt@3] [evt@4]
                                        ▲
                               consumer's current position
                               (has processed 0, 1, 2 — next is 3)
```

When the consumer commits offset 2, it tells Kafka: "I have successfully processed all events up to offset 2 in this partition."

If the consumer crashes and restarts, it resumes from the last committed offset — in this case, offset 3.

---

## Consumer Groups — Parallel Processing

A consumer group is identified by a `group.id`. Kafka divides the partitions of a topic across the consumers in the group.

### Example: 3 consumers, 3 partitions

```
Topic: orders (P0, P1, P2)

Consumer Group "warehouse-loaders":
  Consumer A → reads P0
  Consumer B → reads P1
  Consumer C → reads P2
```

All three run in parallel. The group processes the topic at 3x the speed of a single consumer.

### Example: 2 consumers, 3 partitions (one consumer gets two partitions)

```
Topic: orders (P0, P1, P2)

Consumer Group "warehouse-loaders":
  Consumer A → reads P0, P1
  Consumer B → reads P2
```

### Example: 4 consumers, 3 partitions (one consumer is idle)

```
Topic: orders (P0, P1, P2)

Consumer Group "warehouse-loaders":
  Consumer A → reads P0
  Consumer B → reads P1
  Consumer C → reads P2
  Consumer D → idle (no partition assigned)
```

You **cannot have more active consumers than partitions** in a group. The max useful parallelism for a consumer group = number of partitions.

### Multiple groups reading the same topic independently

```
Topic: orders

Consumer Group "warehouse-loaders"  → reads all events for warehouse
Consumer Group "email-notifications" → reads all events for emails
Consumer Group "analytics"           → reads all events for analytics
```

Each group maintains its own offset per partition. They never interfere with each other. This is one of Kafka's most powerful features.

---

## Rebalancing

When a consumer joins or leaves a group, Kafka **rebalances** — redistributes partitions among the remaining consumers.

```
Before:
  Consumer A → P0, P1
  Consumer B → P2

Consumer B dies:
  Rebalance triggered
  Consumer A → P0, P1, P2  (takes over B's partition)

Consumer C joins:
  Rebalance triggered
  Consumer A → P0, P1
  Consumer C → P2
```

During a rebalance, **all consumers in the group stop processing** until the new assignment is complete. This is called a **stop-the-world rebalance**.

### Minimizing rebalance impact

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'warehouse-loaders',
    'session.timeout.ms': 30000,         # how long before consumer is considered dead (default: 45s)
    'heartbeat.interval.ms': 3000,       # how often consumer sends heartbeat (default: 3s)
    'max.poll.interval.ms': 300000,      # max time between polls before consumer is kicked (default: 5min)
    'partition.assignment.strategy': 'cooperative-sticky'  # less disruptive rebalancing (Kafka 2.4+)
})
```

- `session.timeout.ms` — if no heartbeat received in this time, consumer is declared dead
- `max.poll.interval.ms` — if `poll()` isn't called in this time (e.g., processing takes too long), consumer is kicked from the group
- `cooperative-sticky` — only reassigns partitions that actually need to move, not all of them

---

## Offset Management

### Automatic offset commit (default)

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,           # default: True
    'auto.commit.interval.ms': 5000,      # commit every 5 seconds
})
```

Every 5 seconds, the consumer auto-commits the highest offset it has polled. 

**Risk:** if the consumer crashes after polling but before processing, the offset is committed and those events are lost.

### Manual offset commit (recommended for pipelines)

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,          # disable auto-commit
})

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    # process the event
    process_event(msg.value())

    # only commit AFTER successful processing
    consumer.commit(msg)
```

With manual commit:
- Event is never marked as "done" until processing succeeds
- Consumer crash → resumes from last committed offset
- Risk: **at-least-once delivery** — event may be processed twice if crash happens after processing but before commit

### Commit modes

```python
# synchronous commit (waits for broker ack — safer, slightly slower)
consumer.commit(msg)

# asynchronous commit (fire and forget — faster, small risk of losing commit)
consumer.commit(msg, asynchronous=True)
```

For data pipelines, synchronous commit after each batch is the safest pattern.

---

## Consumer Lag — The Most Important Metric

**Consumer lag** = how far behind a consumer group is from the latest events in the topic.

```
Topic: orders, Partition 0
  Latest offset (end of log): 10,000
  Consumer group "warehouse" last committed offset: 9,500
  Lag: 500 events
```

Lag = 0 → consumer is fully caught up  
Lag growing → consumer is processing slower than events are arriving → problem

### Check lag from CLI

```bash
# check lag for all consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# detailed lag per partition for a specific group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group warehouse-loaders
```

Output:
```
GROUP             TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
warehouse-loaders orders 0          9500            10000           500  consumer-1
warehouse-loaders orders 1          8000            8000            0    consumer-2
warehouse-loaders orders 2          7200            7201            1    consumer-2
```

**Always monitor consumer lag in production.** Growing lag means your pipeline is falling behind.

---

## A Real Consumer in Python

```python
from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'warehouse-loaders',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

consumer.subscribe(['orders'])

print("Listening for events...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue  # no message in this poll interval

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # reached end of partition — not an error
                print(f"End of partition {msg.partition()} at offset {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
            continue

        # decode and process
        event = json.loads(msg.value().decode('utf-8'))
        key = msg.key().decode('utf-8') if msg.key() else None

        print(f"[P{msg.partition()}@{msg.offset()}] key={key} | {event}")

        # --- your processing logic here ---
        # e.g., write to warehouse, update cache, etc.

        # commit offset only after successful processing
        consumer.commit(msg)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # commit final offsets and leave the group cleanly
```

### Run it

```bash
pip install confluent-kafka

# in one terminal — run consumer
python consumer.py

# in another terminal — produce some events
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"
# type:
# order-1:{"order_id":1,"status":"placed"}
# order-1:{"order_id":1,"status":"shipped"}
```

---

## Offset Reset — Replaying Events

Sometimes you need to rewind and reprocess events (bug fix, new pipeline, backfill).

### Reset via CLI

```bash
# preview what reset would do (dry run)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group warehouse-loaders \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --dry-run

# actually reset to earliest (reprocess all events)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group warehouse-loaders \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute

# reset to a specific offset
--reset-offsets --to-offset 1000

# reset to a point in time
--reset-offsets --to-datetime 2026-10-01T00:00:00.000

# reset to latest (skip everything, start fresh)
--reset-offsets --to-latest
```

> ⚠️ The consumer group must be stopped (no active consumers) before resetting offsets.

### Reset programmatically

```python
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

consumer = Consumer({...})
consumer.subscribe(['orders'])

def on_assign(consumer, partitions):
    # reset all partitions to the beginning on assignment
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)

consumer.subscribe(['orders'], on_assign=on_assign)
```

---

## Seeking — Read From a Specific Position

```python
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({...})

# manually assign specific partitions (instead of subscribe)
tp = TopicPartition('orders', partition=0, offset=500)
consumer.assign([tp])

# seek to a specific offset after subscribe + assignment
consumer.seek(TopicPartition('orders', 0, 1000))

# seek to beginning of a specific partition
consumer.seek_to_beginning(TopicPartition('orders', 0))
```

Useful when you want to reprocess a specific partition from a specific point without resetting the whole group.

---

## What Confused Me

- **`consumer.close()` matters.** If you just kill the process without calling `close()`, the session timeout (default 45s) has to expire before Kafka realizes the consumer is gone and triggers a rebalance. Always call `close()` on shutdown to leave the group cleanly and immediately.
- **`max.poll.interval.ms` not `session.timeout.ms` kicks slow processors.** If your processing takes longer than `max.poll.interval.ms` (default 5 min), Kafka kicks the consumer from the group — even if it's still alive and sending heartbeats. If you have long processing, increase this setting.
- **Committing too early = data loss. Committing too late = duplicates.** There's no "safe" option — you choose between at-most-once (commit before processing) or at-least-once (commit after processing). Design consumers to be idempotent.
- **Consumer group lag shows 0 doesn't mean all events were processed successfully.** It means offsets were committed. A bad consumer could commit offsets after failing to process — the events are silently dropped. Monitor your downstream systems too.
- **`--reset-offsets` on a running group does nothing.** You must stop all consumers in the group first. Kafka rejects the reset if any consumer is active.

---

## Resources

- [Kafka consumer docs](https://kafka.apache.org/documentation/#consumerapi)
- [Consumer groups deep dive](https://docs.confluent.io/kafka/design/consumer-design.html)
- [Confluent Python consumer examples](https://github.com/confluentinc/confluent-kafka-python/tree/master/examples)

---

*Next: [05 — Brokers and Replication](./05_brokers_and_replication.md)*

*Back to [Kafka index](./README.md)*
