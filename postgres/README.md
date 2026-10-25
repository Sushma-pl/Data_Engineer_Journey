# PostgreSQL & CDC 🐘

This section covers PostgreSQL from a data engineering perspective — specifically how it works as a **source database for Change Data Capture (CDC)**. If you already know SQL and MySQL basics, these notes will bridge you to Postgres and show you how to stream real-time database changes into Kafka using Debezium.

---

## Why Postgres for CDC?

Postgres has first-class support for logical replication — a built-in mechanism that lets you stream every INSERT, UPDATE, and DELETE as a structured event. Debezium reads this stream and publishes it to Kafka. This is the foundation of most modern data pipelines.

---

## Notes in Order

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 01 | [Postgres vs MySQL — Key Differences](./01_postgres_vs_mysql.md) | What's different coming from MySQL, Postgres-specific concepts you need to know |
| 02 | [WAL and Logical Replication](./02_wal_and_replication.md) | Write-Ahead Log, logical decoding, replication slots — the engine behind CDC |
| 03 | [CDC Concepts](./03_cdc_concepts.md) | What CDC is, how it works, patterns, and why data engineers use it |
| 04 | [Debezium](./04_debezium.md) | What Debezium is, its architecture, connectors, and how it sits between Postgres and Kafka |
| 05 | [Postgres + Debezium + Kafka Setup](./05_debezium_postgres_kafka.md) | Full hands-on: configuring Postgres for CDC, deploying Debezium, connecting to Kafka |

---

## The Big Picture

```
                    ┌─────────────────────────────────────────┐
                    │           Your Application               │
                    │   INSERT / UPDATE / DELETE               │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │           PostgreSQL                      │
                    │   Tables + Write-Ahead Log (WAL)         │
                    └──────────────────┬──────────────────────┘
                                       │  logical replication slot
                    ┌──────────────────▼──────────────────────┐
                    │           Debezium Connector             │
                    │   reads WAL → transforms → publishes     │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │              Kafka                        │
                    │   one topic per table                    │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │   Consumers (your pipeline)              │
                    │   data warehouse / search / cache / ML   │
                    └─────────────────────────────────────────┘
```

---

*Back to [main index](../README.md)*
