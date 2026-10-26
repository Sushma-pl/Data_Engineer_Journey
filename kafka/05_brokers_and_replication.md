# 05 — Brokers and Replication

## The Simple Answer

A **broker** is a Kafka server — it receives events from producers, stores them on disk, and serves them to consumers. A **Kafka cluster** is a group of brokers working together. **Replication** means each partition has copies on multiple brokers so data survives if a broker dies.

This is what makes Kafka fault-tolerant and the reason data engineers trust it for critical pipelines.

---

## Brokers

### What a broker does

```
Producer
  │
  │  send(topic="orders", partition=0, value=...)
  ▼
Broker 1  (hosts partition 0 of orders)
  ├── writes to disk (WAL-style commit log)
  ├── replicates to Broker 2 and 3
  └── serves reads to consumers
```

Every broker:
- Stores the partitions assigned to it on local disk
- Handles produce requests (writes) and fetch requests (reads) for its partitions
- Acts as a **bootstrap server** — connecting to any one broker gives you metadata about the whole cluster

### Cluster topology

```
Kafka Cluster (3 brokers)

Broker 1  (port 9092)   hosts: orders-P0, users-P1, payments-P2
Broker 2  (port 9093)   hosts: orders-P1, users-P2, payments-P0
Broker 3  (port 9094)   hosts: orders-P2, users-P0, payments-P1
```

Partitions are spread across brokers so load is balanced. No single broker holds all the data.

### ZooKeeper vs KRaft

**ZooKeeper mode (older, still common)**
Kafka used ZooKeeper to store cluster metadata — which broker is leader for which partition, topic configs, consumer group offsets, etc. ZooKeeper is a separate service you need to run alongside Kafka.

```
ZooKeeper (port 2181)
  ├── /brokers/ids/1
  ├── /brokers/ids/2
  ├── /brokers/topics/orders/partitions/0/state → {"leader": 1, "isr": [1,2,3]}
  └── /controller → broker 1 is the controller
```

**KRaft mode (modern, Kafka 3.x)**
Kafka manages its own metadata using the Raft consensus algorithm — no ZooKeeper needed. KRaft is simpler to operate and is the future of Kafka. Confluent images are in the process of transitioning.

For most Docker examples you'll encounter today (including this repo), ZooKeeper is still used. The concepts are identical — only the metadata storage changes.

---

## Replication — How Kafka Survives Failures

Every partition can have multiple copies (replicas) spread across different brokers. If one broker dies, another replica takes over.

### Replication factor

```bash
# create a topic with replication factor 3 (3 copies of each partition)
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

With `replication-factor=3`, every partition exists on 3 different brokers:

```
Partition 0:  Broker 1 (LEADER) ← Broker 2 (follower) ← Broker 3 (follower)
Partition 1:  Broker 2 (LEADER) ← Broker 1 (follower) ← Broker 3 (follower)
Partition 2:  Broker 3 (LEADER) ← Broker 1 (follower) ← Broker 2 (follower)
```

- **Leader** — the broker that handles all reads and writes for that partition
- **Follower** — copies data from the leader, ready to take over if the leader fails

### Seeing replicas in action

```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

```
Topic: orders   PartitionCount: 3   ReplicationFactor: 3
  Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
  Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
  Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

- **Replicas** — all brokers that should have a copy of this partition
- **Isr** (In-Sync Replicas) — replicas that are actually caught up to the leader

---

## In-Sync Replicas (ISR)

The ISR is the set of replicas that are **fully caught up** with the leader. This is the most important concept for understanding Kafka's durability guarantees.

```
Leader (Broker 1) receives a new event
  │
  ├──► Follower on Broker 2: caught up → in ISR ✅
  ├──► Follower on Broker 3: caught up → in ISR ✅
  └──► Follower on Broker 4: 500 events behind → NOT in ISR ❌
```

A replica falls out of ISR if it falls too far behind (controlled by `replica.lag.time.max.ms`, default 30s).

### Why ISR matters for producers

`acks=all` means **all ISR replicas must acknowledge the write** — not necessarily all replicas, just those in the ISR.

```
Topic config: min.insync.replicas = 2
ISR: [Broker 1 (leader), Broker 2]  (Broker 3 is lagging, not in ISR)

Producer sends with acks=all:
  → Leader writes
  → Broker 2 writes and acks
  → ISR size (2) >= min.insync.replicas (2) → write succeeds ✅
```

### `min.insync.replicas`

This is the safety lever. It sets the minimum number of ISR replicas that must acknowledge a write:

```bash
# set on the topic
kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config min.insync.replicas=2
```

**Recommended production setup:**
```
replication.factor    = 3
min.insync.replicas   = 2
acks (producer)       = all
```

This tolerates one broker failing without losing data:
- 3 replicas exist
- At least 2 must ack every write
- If one broker dies, you still have 2 in ISR → writes still succeed
- If two brokers die, only 1 ISR → write fails with `NotEnoughReplicasException` → producer retries → **no data loss, just blocked writes**

---

## What Happens When a Broker Fails

Walk through a broker failure step by step:

```
Normal state:
  Partition 0: Leader=Broker1, ISR=[1,2,3]
  Partition 1: Leader=Broker2, ISR=[1,2,3]
  Partition 2: Leader=Broker3, ISR=[1,2,3]

Broker 1 DIES:
  Step 1: ZooKeeper / KRaft detects broker 1 is gone
  Step 2: Controller broker triggers leader election for partitions 0 was leading
  Step 3: A new leader is elected from ISR (say Broker 2 becomes new leader for P0)
  Step 4: Metadata propagated to all brokers and clients

After failover:
  Partition 0: Leader=Broker2, ISR=[2,3]   ← only 2 now, Broker 1 is gone
  Partition 1: Leader=Broker2, ISR=[2,3]
  Partition 2: Leader=Broker3, ISR=[2,3]

Broker 1 comes back:
  Step 1: Broker 1 registers with ZooKeeper/KRaft
  Step 2: Starts catching up (replaying WAL from leader)
  Step 3: Once caught up, rejoins ISR
  Step 4: May reclaim leadership (if auto.leader.rebalance.enable=true)

After recovery:
  Partition 0: Leader=Broker2, ISR=[1,2,3]  ← Broker 1 back in ISR
```

**Producer and consumer behavior during failover:**
- Producers retry automatically (if `retries > 0`) → no data loss with `acks=all`
- Consumers transparently reconnect to new leader → temporary pause, then continues
- Failover typically completes in **under 30 seconds** for a healthy cluster

---

## The Controller Broker

One broker in the cluster is elected as the **controller**. It handles cluster-level operations:

- Leader election when a broker fails
- Partition reassignment
- Topic creation/deletion metadata

If the controller fails, another broker is automatically elected. The controller is an internal role — you don't interact with it directly.

```sql
-- in ZooKeeper, you can see who the controller is
-- (in practice, just use kafka-broker-api-versions to check cluster health)
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

---

## Partition Leadership and Preferred Leaders

Each partition has a **preferred leader** — the broker listed first in its replica list. When Kafka was first set up, partitions were assigned so preferred leaders are spread evenly across brokers (load balancing).

Over time, after failures and recoveries, leadership can drift — many partitions end up on the same broker, creating hotspots.

```bash
# trigger preferred leader election to rebalance leadership
kafka-leader-election --bootstrap-server localhost:9092 \
  --election-type PREFERRED \
  --all-topic-partitions
```

Or enable automatic rebalancing:
```bash
# on the broker (server.properties or environment variable)
auto.leader.rebalance.enable=true          # default: true
leader.imbalance.check.interval.seconds=300 # check every 5 minutes
```

---

## Unclean Leader Election

What if a broker dies and ALL replicas in the ISR are also gone? Only out-of-sync replicas exist.

```
Partition 0: ISR=[Broker1(dead), Broker2(dead)]
             Out-of-sync replica: Broker3 (500 events behind)

Options:
  A) Wait for ISR to come back → partition unavailable (no data loss)
  B) Elect Broker3 as leader   → partition available (but 500 events LOST)
```

This is controlled by `unclean.leader.election.enable`:

```bash
# default: false — never elect out-of-sync replica (prefer unavailability over data loss)
unclean.leader.election.enable=false

# true — elect out-of-sync replica to stay available (risks data loss)
unclean.leader.election.enable=true
```

**For data pipelines: always keep this `false`.** Data loss is worse than temporary unavailability.

---

## Disk Storage — How Brokers Store Data

Kafka stores each partition as a series of **segment files** on disk:

```
/var/lib/kafka/data/orders-0/        ← partition 0 data directory
  00000000000000000000.log            ← segment: events from offset 0
  00000000000000000000.index          ← offset index (fast lookup)
  00000000000000000000.timeindex      ← timestamp index
  00000000000001048576.log            ← next segment starts at offset 1048576
  00000000000001048576.index
  leader-epoch-checkpoint
```

- Each `.log` file is a segment — a chunk of the partition log
- When a segment reaches `log.segment.bytes` (default: 1GB), a new segment is created
- Old segments are deleted when they exceed the retention policy
- The index files allow consumers to jump to an offset quickly without scanning the whole log

```bash
# see actual kafka data files inside docker container
docker exec -it kafka ls /var/lib/kafka/data/orders-0/
```

---

## Key Broker Configuration Reference

```ini
# --- Replication ---
default.replication.factor=3          # default replication for new topics
min.insync.replicas=2                  # min ISR acks required (with acks=all)
unclean.leader.election.enable=false  # never elect lagging replica as leader

# --- Storage ---
log.dirs=/var/lib/kafka/data          # where partition data is stored
log.segment.bytes=1073741824          # segment size before rolling (1GB)
log.retention.hours=168               # keep data for 7 days
log.retention.bytes=-1                # no size limit (set per topic if needed)

# --- Performance ---
num.network.threads=3                 # threads for network I/O
num.io.threads=8                      # threads for disk I/O
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

# --- Replication timing ---
replica.lag.time.max.ms=30000        # how long before a replica is removed from ISR
```

---

## Visualizing the Cluster — Describe Topics

```bash
# full cluster health check
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe

# look for partitions where ISR ≠ Replicas (under-replicated)
# example of a problem:
#   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1   ← only leader in ISR = danger!
```

A healthy cluster shows `Isr` matching `Replicas` for every partition. When they don't match, you have **under-replicated partitions** — a warning sign that needs immediate attention.

---

## What Confused Me

- **The leader handles ALL reads AND writes.** Unlike some databases where replicas can serve reads, Kafka consumers always read from the leader (by default). Followers only exist for fault tolerance. Kafka 2.4+ introduced **follower fetching** to allow consumers to read from the nearest replica, but the leader is still the default.
- **`replication.factor` vs `min.insync.replicas`** — replication factor is how many copies exist. `min.insync.replicas` is how many must confirm a write. You need both set correctly for true durability. Replication factor alone doesn't protect you if `acks=1`.
- **`acks=all` does NOT mean all replicas.** It means all ISR replicas. If a broker is lagging and falls out of ISR, it's excluded from the ack requirement. The combination of `acks=all` + `min.insync.replicas=2` + `replication.factor=3` is the correct production recipe.
- **Single-broker Docker clusters can't have `replication.factor > 1`.** For learning locally, use `replication-factor=1`. A `replication-factor=3` with only one broker fails — you can't replicate to brokers that don't exist.
- **ISR shrinks silently.** A follower can fall out of ISR with no alarms unless you're monitoring. Add an alert on under-replicated partitions (`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`).

---

## Resources

- [Kafka replication deep dive](https://kafka.apache.org/documentation/#replication)
- [ISR and acks explained](https://www.confluent.io/blog/kafka-replication-the-crux-of-kafka-reliability/)
- [KRaft mode overview](https://docs.confluent.io/kafka/introduction.html)

---

*Next: [06 — Kafka CLI Hands-On](./06_kafka_cli_hands_on.md)*

*Back to [Kafka index](./README.md)*
