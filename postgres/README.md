# PostgreSQL & CDC 🐘

This section covers PostgreSQL from a data engineering perspective — specifically how it works as a **source database for Change Data Capture (CDC)**. If you already know SQL and MySQL basics, these notes will bridge you to Postgres and show you how to stream real-time database changes into Kafka using Debezium.

---

## Why Postgres for CDC?

Postgres has first-class support for logical replication — a built-in mechanism that lets you stream every INSERT, UPDATE, and DELETE as a structured event. Debezium reads this stream and publishes it to Kafka. This is the foundation of most modern data pipelines.

---

## Notes in Order

### Priority 1 — Core (build ShopStream with these)

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 01 | [Postgres vs MySQL — Key Differences](./01_postgres_vs_mysql.md) | Syntax diffs, data types (NUMERIC, TIMESTAMPTZ, JSONB, arrays), schemas, SERIAL/IDENTITY, ON CONFLICT, RETURNING, psql commands, generate_series |
| 02 | [WAL and Logical Replication](./02_wal_and_replication.md) | WAL internals, wal_level (minimal/replica/logical), LSN, logical decoding, output plugins (pgoutput, test_decoding), replication slots, publications, REPLICA IDENTITY |
| 03 | [CDC Concepts](./03_cdc_concepts.md) | What CDC is, polling vs log-based, event envelope, CDC patterns, snapshot modes, delivery guarantees, idempotent consumers |
| 04 | [Debezium](./04_debezium.md) | Architecture, Kafka Connect, connector config, topic naming, SMTs, offset management, Schema Registry, monitoring via REST API |
| 05 | [Postgres + Debezium + Kafka Setup](./05_debezium_postgres_kafka.md) | Full hands-on Docker Compose stack, init.sql, connector deployment, end-to-end verification, common errors and fixes |

### Priority 2 — Production & Interviews

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 06 | [Advanced CDC Internals](./06_advanced_cdc_internals.md) | TOAST + `__debezium_unavailable_value`, heartbeat events, DDL changes, large transactions, WAL safeguards (`max_slot_wal_keep_size`), monitoring views (`pg_stat_replication`, `pg_replication_slots`), AWS RDS setup |

### Priority 3 — Read and Understand

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 07 | [Internals Reference](./07_reference_and_internals.md) | Failover and logical slot loss, physical vs logical replication compared, MVCC explained, VACUUM and autovacuum, CDC interaction with dead tuples |

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
