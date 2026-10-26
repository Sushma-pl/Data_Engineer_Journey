# 06 — Kafka CLI: Hands-On Practice

## The Simple Answer

This note is your practical lab. Everything is runnable on a single Docker container on your laptop. We'll bring up Kafka, explore every important CLI tool, produce real events, consume them, inspect the cluster, and simulate failures — all from the command line.

No Python needed for this note. Just Docker and a terminal.

---

## Step 1 — Start Kafka with Docker Compose

Create this `docker-compose.yml` in a new folder (e.g., `kafka-lab/`):

```yaml
version: "3.9"

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD-SHELL", "echo ruok | nc localhost 2181 | grep imok"]
      interval: 10s
      timeout: 5s
      start_period: 20s
      retries: 5
    networks:
      - kafka-lab

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # disable auto-creation, create topics explicitly
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      start_period: 60s
      retries: 10
    networks:
      - kafka-lab

networks:
  kafka-lab:
    driver: bridge
```

```bash
# start it
docker compose up -d

# wait for healthy (takes ~60s for kafka)
docker compose ps

# you should see:
# kafka       running (healthy)
# zookeeper   running (healthy)
```

All CLI commands below run inside the Kafka container. Open a shell:

```bash
docker exec -it kafka bash
```

All Kafka CLI tools live in `/usr/bin/` and connect via `--bootstrap-server localhost:9092`.

---

## Step 2 — Topic Management

### Create topics

```bash
# create a topic with 3 partitions, replication factor 1 (single broker)
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# create more topics for practice
kafka-topics --bootstrap-server localhost:9092 --create --topic users --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic payments --partitions 1 --replication-factor 1
```

### List all topics

```bash
kafka-topics --bootstrap-server localhost:9092 --list

# output:
# orders
# payments
# users
```

### Describe a topic

```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# output:
# Topic: orders   PartitionCount: 3   ReplicationFactor: 1
#   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
#   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
#   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

### Describe all topics at once

```bash
kafka-topics --bootstrap-server localhost:9092 --describe
```

### Modify a topic — change retention

```bash
# set retention to 1 hour (3600000 ms)
kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=3600000

# view current config for a topic
kafka-configs --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name orders
```

### Delete a topic

```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic payments
```

---

## Step 3 — Producing Events

### Basic producer — no keys

```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders
```

You'll get a `>` prompt. Type each event and press Enter:
```
> {"order_id": 1, "status": "placed", "amount": 99.99}
> {"order_id": 2, "status": "placed", "amount": 149.00}
> {"order_id": 1, "status": "shipped"}
> ^C
```

Each line becomes one event (one Kafka record). `Ctrl+C` to exit.

### Producer with keys

```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Now type `key:value` pairs:
```
> order-1:{"order_id": 1, "status": "placed"}
> order-2:{"order_id": 2, "status": "placed"}
> order-1:{"order_id": 1, "status": "shipped"}
> order-3:{"order_id": 3, "status": "placed"}
> order-1:{"order_id": 1, "status": "delivered"}
> ^C
```

All `order-1` events go to the same partition → they'll always be read in order.

### Produce from a file

```bash
# create a file with events (one per line)
cat > /tmp/events.txt << 'EOF'
order-4:{"order_id": 4, "status": "placed"}
order-5:{"order_id": 5, "status": "placed"}
order-4:{"order_id": 4, "status": "confirmed"}
order-5:{"order_id": 5, "status": "shipped"}
EOF

# pipe it to the producer
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:" \
  < /tmp/events.txt

echo "Events sent!"
```

---

## Step 4 — Consuming Events

### Read all events from the beginning

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

You'll see all events you produced. Notice the **order may look shuffled** — that's because events from different partitions are interleaved. Events from the SAME partition are always in order.

Press `Ctrl+C` to stop.

### Read with metadata (partition + offset + key)

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true
```

Output shows:
```
Partition:0     Offset:0        Timestamp:1698123456789    order-2 {"order_id": 2, ...}
Partition:1     Offset:0        Timestamp:1698123456790    order-1 {"order_id": 1, ...}
Partition:1     Offset:1        Timestamp:1698123456791    order-1 {"order_id": 1, "status": "shipped"}
```

Notice `order-1` events are on Partition 1, offsets 0 and 1 — always in order.

### Read only new events (from now)

```bash
# don't use --from-beginning → reads only future events
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Open a second terminal, produce more events, watch them arrive in real time.

### Read with a consumer group

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --group my-pipeline
```

The consumer group `my-pipeline` is created and its offset is tracked. Run it a second time — it picks up from where it left off, not from the beginning.

---

## Step 5 — Consumer Groups

### List consumer groups

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Describe a consumer group — see lag

```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-pipeline
```

```
GROUP        TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
my-pipeline  orders 0          3               3               0    consumer-1-...
my-pipeline  orders 1          4               4               0    consumer-1-...
my-pipeline  orders 2          2               2               0    consumer-1-...
```

`LAG=0` means fully caught up. Stop the consumer, produce more events, run describe again — you'll see lag growing.

### Practice — watch lag grow

```bash
# terminal 1: start consumer with group
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group lag-demo

# press Ctrl+C to stop it

# terminal 2: produce 10 more events
for i in $(seq 1 10); do
  echo "order-$i:{\"order_id\": $i, \"status\": \"placed\"}"
done | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"

# check lag (consumer is stopped, lag should be 10)
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group lag-demo
```

### Reset consumer group offsets

```bash
# dry run first — see what would happen
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-pipeline \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --dry-run

# apply the reset (consumer must be stopped)
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-pipeline \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute

# now run the consumer again — it starts from the beginning
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --group my-pipeline
```

---

## Step 6 — Parallel Consumers (See Partition Assignment)

This demonstrates how consumer groups distribute partitions.

Open **three terminals**, all running this command (but don't press Enter yet):

```bash
# terminal 1, 2, 3 — same command
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group parallel-demo \
  --property print.partition=true
```

Start terminal 1 first. Check the group:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group parallel-demo
# Consumer 1 is assigned all 3 partitions
```

Start terminal 2. Check again:
```bash
# Rebalance happened! Now:
# Consumer 1 → P0, P1
# Consumer 2 → P2
```

Start terminal 3:
```bash
# Consumer 1 → P0
# Consumer 2 → P1
# Consumer 3 → P2   ← one partition each
```

Now produce events with different keys — watch each event appear in exactly one consumer terminal based on which partition it lands in.

Kill terminal 2 (Ctrl+C) — rebalance again, consumer 3 takes over P1.

---

## Step 7 — Partition and Offset Exploration

### Check end offsets (how many events in each partition)

```bash
kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --time -1
# -1 = latest offset (= number of events ever written to that partition)
```

```
orders:0:12
orders:1:8
orders:2:5
```

### Check earliest offset (oldest event still available)

```bash
kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --time -2
# -2 = earliest offset
```

After retention kicks in, the earliest offset will be > 0.

### Read from a specific partition and offset

```bash
# read only from partition 1, starting at offset 2
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partition 1 \
  --offset 2
```

---

## Step 8 — Topic Configuration Experiments

### Experiment: see log compaction in action

```bash
# create a compacted topic
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic user-profiles \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=100

# produce multiple events for the same keys
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type:
```
user-1:{"name": "Alice", "city": "New York"}
user-2:{"name": "Bob", "city": "London"}
user-1:{"name": "Alice", "city": "Chicago"}     ← update user-1
user-3:{"name": "Carol", "city": "Tokyo"}
user-2:{"name": "Bob", "city": "Paris"}          ← update user-2
user-1:{"name": "Alice", "city": "San Francisco"} ← another update
^C
```

After compaction runs (may take a minute with the aggressive settings above), reading from the beginning will only show the latest value per key.

### Experiment: retention in action

```bash
# create a topic with very short retention (10 seconds)
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic short-lived \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=10000 \
  --config segment.ms=5000

# produce events
echo "event1" | kafka-console-producer --bootstrap-server localhost:9092 --topic short-lived
echo "event2" | kafka-console-producer --bootstrap-server localhost:9092 --topic short-lived

# read immediately — see both events
kafka-console-consumer --bootstrap-server localhost:9092 --topic short-lived --from-beginning

# wait 15 seconds, then read again — events should be gone
sleep 15
kafka-console-consumer --bootstrap-server localhost:9092 --topic short-lived --from-beginning
# no output (or just the latest segment)
```

---

## Step 9 — Useful Diagnostic Commands

### Check broker is healthy

```bash
kafka-broker-api-versions --bootstrap-server localhost:9092
# if this returns output, the broker is reachable
```

### Dump a partition's log (see raw events on disk)

```bash
kafka-dump-log \
  --files /var/lib/kafka/data/orders-0/00000000000000000000.log \
  --print-data-log
```

Output shows every event stored in that segment file — offset, timestamp, key, value. Useful for debugging.

### Check cluster metadata

```bash
kafka-metadata-quorum --bootstrap-server localhost:9092 --command-line-quorum-monitor describe
# (KRaft mode only — shows controller state)
```

### Find which partition a key would go to

```bash
# Kafka's default partitioner uses murmur2 hash
# You can figure it out by producing with the key and checking --print.partition
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:" | tee /dev/null &

kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property print.key=true \
  --property print.partition=true &
```

---

## Complete CLI Quick Reference

```bash
# ── Topic Management ──────────────────────────────────────────────────────
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server localhost:9092 --describe --topic <name>
kafka-topics --bootstrap-server localhost:9092 --create --topic <name> --partitions <n> --replication-factor <n>
kafka-topics --bootstrap-server localhost:9092 --delete --topic <name>

# ── Config ────────────────────────────────────────────────────────────────
kafka-configs --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name <name>
kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name <name> --add-config <key>=<value>

# ── Produce ───────────────────────────────────────────────────────────────
kafka-console-producer --bootstrap-server localhost:9092 --topic <name>
kafka-console-producer --bootstrap-server localhost:9092 --topic <name> --property parse.key=true --property key.separator=:

# ── Consume ───────────────────────────────────────────────────────────────
kafka-console-consumer --bootstrap-server localhost:9092 --topic <name> --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic <name> --group <group> --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic <name> --partition <n> --offset <n>
kafka-console-consumer ... --property print.key=true --property print.partition=true --property print.offset=true

# ── Consumer Groups ───────────────────────────────────────────────────────
kafka-consumer-groups --bootstrap-server localhost:9092 --list
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <name>
kafka-consumer-groups --bootstrap-server localhost:9092 --group <name> --topic <name> --reset-offsets --to-earliest --execute
kafka-consumer-groups --bootstrap-server localhost:9092 --group <name> --topic <name> --reset-offsets --to-latest --execute
kafka-consumer-groups --bootstrap-server localhost:9092 --group <name> --topic <name> --reset-offsets --to-offset <n> --execute
kafka-consumer-groups --bootstrap-server localhost:9092 --group <name> --topic <name> --reset-offsets --to-datetime 2026-10-01T00:00:00.000 --execute

# ── Offsets ───────────────────────────────────────────────────────────────
kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic <name> --time -1   # latest
kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic <name> --time -2   # earliest

# ── Diagnostics ───────────────────────────────────────────────────────────
kafka-broker-api-versions --bootstrap-server localhost:9092
kafka-dump-log --files <path-to-segment.log> --print-data-log
kafka-leader-election --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions
```

---

## Exercises — Work Through These

These exercises build your intuition. Do them in order.

**Exercise 1 — Ordering within a partition**
Create topic `test-order` with 3 partitions. Produce 10 events with key `key-A` and 10 with key `key-B`. Consume and observe: all `key-A` events come from the same partition in order. All `key-B` events come from a different partition in order. No ordering guarantee between the two.

**Exercise 2 — Consumer group parallelism**
Create topic `test-parallel` with 4 partitions. Open 4 consumer terminals in the same group. Produce 40 events. Observe: each consumer gets roughly 10 events (one partition each). Close one consumer — remaining 3 split the work 2/1/1 after rebalance.

**Exercise 3 — Lag and reset**
Create topic `test-lag` with 3 partitions. Start a consumer group, consume all events, stop it. Produce 50 more events. Check lag (should be 50). Reset offsets to earliest. Start consumer — replays everything from offset 0.

**Exercise 4 — Retention**
Create topic `test-retention` with `retention.ms=30000` (30 seconds) and `segment.ms=5000`. Produce 5 events. Read them. Wait 35 seconds. Produce 2 more. Read from beginning — only the last 2 events should be visible.

**Exercise 5 — Log compaction**
Create a compacted topic `test-compact`. Produce 5 events for key `item-1` with different values. Wait for compaction (aggressive settings from Step 8). Read from beginning — only the last event for `item-1` should be there.

---

## What Confused Me

- **Events produced without a key don't respect partition order.** If you produce 5 events without a key, they round-robin across partitions. Consumer reads them in partition order — not the order you sent them. Always use keys when order matters.
- **`--from-beginning` with a consumer group only works once.** The first time, it reads from offset 0. After that, the group has committed offsets — `--from-beginning` is ignored. To truly start over, reset the group's offsets or use a new group name.
- **`kafka-console-consumer` creates a consumer group even if you don't specify one.** It auto-generates a group ID like `console-consumer-12345`. These accumulate over time. Use `--group my-group` explicitly so you can manage the group later.
- **CLI tools connect to `localhost:9092` from inside the container.** From your host machine (outside Docker), they'd also use `localhost:9092` because of the port mapping `-p 9092:9092`. The container-to-container address is `kafka:29092`.
- **`kafka-dump-log` needs the exact file path.** You need to exec into the container and find the partition directory first. Use `ls /var/lib/kafka/data/` to find your topic's partition folders.

---

## Resources

- [Kafka CLI tools reference](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
- [confluent-kafka Python quickstart](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Kafka Docker examples](https://github.com/confluentinc/cp-all-in-one)

---

*Back to [Kafka index](./README.md)*
