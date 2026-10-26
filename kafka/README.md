# Apache Kafka 📨

Kafka is the backbone of modern data engineering. It's a distributed event streaming platform that acts as a high-throughput, fault-tolerant pipe between every system in your architecture — databases, services, pipelines, warehouses, caches. If data moves in your company, Kafka is probably in the middle of it.

These notes are written for someone building real streaming pipelines. Every concept connects to practical examples you can run on Docker.

---

## The Big Picture

```
                 ┌──────────────┐
                 │   Producers   │  (your app, Debezium, sensors)
                 └──────┬───────┘
                        │  write events
                        ▼
┌───────────────────────────────────────────────────────┐
│                   Kafka Cluster                        │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Topic: orders  (3 partitions, replicated)       │ │
│  │  [P0: evt1, evt3, evt6 ...]                      │ │
│  │  [P1: evt2, evt4, evt7 ...]                      │ │
│  │  [P2: evt5, evt8, evt9 ...]                      │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  Broker 1      Broker 2      Broker 3                  │
└───────────────────────────────────────────────────────┘
                        │  read events
                        ▼
                 ┌──────────────┐
                 │   Consumers  │  (your pipeline, warehouse loader, ML)
                 └──────────────┘
```

---

## Notes in Order

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 01 | [What is Kafka?](./01_what_is_kafka.md) | Events, event streaming, Kafka's role, use cases, core components overview |
| 02 | [Topics, Partitions and Offsets](./02_topics_partitions_offsets.md) | How data is stored, ordering guarantees, retention, log compaction |
| 03 | [Producers](./03_producers.md) | How producers work, keys, partitioning strategy, acknowledgements, batching |
| 04 | [Consumers and Consumer Groups](./04_consumers_and_consumer_groups.md) | Reading events, consumer groups, offset management, lag, rebalancing |
| 05 | [Brokers and Replication](./05_brokers_and_replication.md) | Cluster internals, leaders, ISR, fault tolerance, what happens when a broker fails |
| 06 | [Kafka CLI — Hands-On](./06_kafka_cli_hands_on.md) | Full Docker setup, every important CLI command, end-to-end practical exercises |

---

## How to Use These Notes

- **If you're completely new:** read 01 → 06 in order
- **If you want to run things immediately:** start with `06_kafka_cli_hands_on.md` — it has the Docker setup — then come back to read the theory
- **If you're preparing for interviews:** focus on 02, 03, 04, 05 — those cover the concepts that come up most

---

*Back to [main index](../README.md)*
