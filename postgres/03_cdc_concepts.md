# 03 — CDC Concepts

## The Simple Answer

**Change Data Capture (CDC)** is the process of detecting and capturing every change (INSERT, UPDATE, DELETE) made to a database and making those changes available as a stream of events — in real time, in order, reliably.

Instead of asking "what does the database look like right now?", CDC asks "what changed, when, and what did it look like before and after?"

---

## Why CDC? The Problem It Solves

Imagine you have an orders table in Postgres. Three downstream systems need to stay in sync:
- A data warehouse (for analytics)
- An Elasticsearch index (for search)
- A cache (for fast reads)

### The Naive Approach — Polling

```
Every 5 minutes:
  SELECT * FROM orders WHERE updated_at > last_check_time
```

Problems:
- **Latency** — up to 5 minutes behind
- **Missing deletes** — deleted rows don't show up in a SELECT
- **Requires `updated_at` column** — not all tables have this
- **Inefficient** — scans the whole table window even if nothing changed
- **Missed changes** — if two updates happen in one poll window, you only see the final state

### The CDC Approach

```
Every time a row changes:
  → emit an event: {table, operation, before, after, timestamp, lsn}
```

- **Real-time** — events published within milliseconds
- **Captures all operations** — INSERT, UPDATE, DELETE, even DDL changes
- **No schema changes needed** — reads from WAL, not from your tables
- **Complete history** — every intermediate state is captured
- **Zero load on the source DB** — reading WAL is lightweight

---

## A CDC Event — What It Actually Looks Like

When Debezium captures a change, it produces a structured event like this:

```json
{
  "before": null,
  "after": {
    "id": 1001,
    "user_id": 42,
    "amount": 99.99,
    "status": "pending",
    "created_at": 1698123456789
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "dbserver1",
    "ts_ms": 1698123456800,
    "db": "pipeline_db",
    "schema": "public",
    "table": "orders",
    "lsn": 1752231,
    "txId": 756
  },
  "op": "c",
  "ts_ms": 1698123456900
}
```

### Operation codes

| `op` | Meaning |
|------|---------|
| `c` | CREATE (INSERT) |
| `u` | UPDATE |
| `d` | DELETE |
| `r` | READ (initial snapshot) |
| `t` | TRUNCATE |

For an UPDATE:
```json
{
  "before": { "id": 1001, "status": "pending" },
  "after":  { "id": 1001, "status": "shipped" },
  "op": "u"
}
```

For a DELETE:
```json
{
  "before": { "id": 1001, "status": "shipped", "amount": 99.99 },
  "after": null,
  "op": "d"
}
```

This is the "envelope" format Debezium uses. Your consumers receive these events from Kafka and process them.

---

## CDC Patterns

### Pattern 1: Event Streaming to Data Warehouse

The most common use case. Stream every database change into a warehouse (BigQuery, Snowflake, Redshift) for analytics.

```
Postgres ──CDC──► Kafka ──consumer──► Snowflake / BigQuery
                                     (append to fact tables,
                                      maintain slowly changing dims)
```

Instead of nightly batch ETL jobs, your warehouse is continuously updated within seconds of a transaction.

### Pattern 2: Cache Invalidation

Keep a Redis or Memcached cache in sync with the database automatically.

```
Postgres ──CDC──► Kafka ──consumer──► Redis
(orders table)                        (invalidate/update cache key)
```

When an order status changes, the cache entry is updated in real time — no stale data.

### Pattern 3: Search Index Sync

Keep Elasticsearch or OpenSearch in sync with Postgres.

```
Postgres ──CDC──► Kafka ──consumer──► Elasticsearch
(products table)                      (update product index)
```

No more manual reindexing scripts. Every product change reflects in search within seconds.

### Pattern 4: Microservice Event Bus

Use CDC to decouple services. Services don't call each other directly — they react to database changes.

```
Order Service writes to Postgres
  → CDC captures the INSERT
  → Kafka event published
  → Notification service consumes: sends email
  → Inventory service consumes: reserves stock
  → Analytics service consumes: updates metrics
```

This is the **Outbox Pattern** in practice — more reliable than dual writes.

### Pattern 5: Database Migration with Zero Downtime

Migrate from old database to new one while keeping both in sync.

```
Old Postgres ──CDC──► Kafka ──consumer──► New Postgres (or different DB)
                                          (apply changes in real time)
```

Once the new DB has caught up, switch traffic over. Zero downtime migration.

---

## CDC Approaches — Not Just WAL

There are a few different ways to implement CDC. It helps to know them all.

### 1. Log-Based CDC (what Debezium does)

Reads the database's transaction log (WAL in Postgres, binlog in MySQL).

```
App → DB → WAL → CDC tool → Kafka
```

✅ Zero impact on source DB (log reading is lightweight)
✅ Captures all operations including deletes
✅ Captures the exact order of changes
✅ No changes to application or schema
❌ Requires access to database internals

### 2. Trigger-Based CDC

Database triggers fire on INSERT/UPDATE/DELETE and write to a change log table.

```sql
-- example trigger approach
CREATE TABLE change_log (
    id SERIAL,
    table_name TEXT,
    operation TEXT,
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION log_changes()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO change_log (table_name, operation, old_data, new_data)
  VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

✅ Works on any database
❌ Adds overhead to every write (triggers execute synchronously)
❌ Doesn't capture bulk operations well
❌ Requires schema changes

### 3. Timestamp / Polling CDC

Queries for rows changed since last poll using a timestamp column.

```sql
SELECT * FROM orders WHERE updated_at > :last_polled_at
```

✅ Simple to implement
❌ Can't capture deletes
❌ Requires `updated_at` column on every table
❌ Latency (polling interval)
❌ Misses intermediate states

**For data engineering, log-based CDC (Debezium) is almost always the right choice.**

---

## The Snapshot — How CDC Starts

When you first set up Debezium, your Kafka topics are empty but the database already has data. Debezium solves this with an **initial snapshot**.

```
Day 0: database has 1 million existing rows

Debezium startup:
  Phase 1 — Snapshot:
    Read all existing rows from the table
    Publish each as an "r" (read) event to Kafka
    Once complete, record the current WAL position

  Phase 2 — Streaming:
    Start reading WAL from the position captured during snapshot
    All new changes are published as "c", "u", "d" events
```

This ensures your Kafka topic has a complete view of the data from day one. Snapshot modes are configurable:

| Mode | Behavior |
|------|---------|
| `initial` | Snapshot on first run only (default) |
| `always` | Snapshot every time Debezium starts |
| `never` | Skip snapshot, start streaming from now |
| `initial_only` | Snapshot then stop (no streaming) |
| `exported` | Snapshot at a consistent point, then stream |

---

## Exactly-Once, At-Least-Once, At-Most-Once

CDC systems deal with delivery guarantees. This matters for how you write consumers.

| Guarantee | Meaning | Risk |
|-----------|---------|------|
| **At-most-once** | Events may be lost | Data loss |
| **At-least-once** | Events may be duplicated | Duplicate processing |
| **Exactly-once** | Each event processed once | Complex, requires coordination |

Debezium + Kafka gives you **at-least-once** delivery by default. Your consumers may see the same event more than once (e.g., after a restart). Design them to be **idempotent** — processing the same event twice has the same result as processing it once.

```python
# idempotent consumer example — upsert instead of insert
INSERT INTO warehouse.orders (id, status, amount)
VALUES (:id, :status, :amount)
ON CONFLICT (id) DO UPDATE SET
    status = EXCLUDED.status,
    amount = EXCLUDED.amount;
```

---

## Ordering and Transactions

Debezium preserves transaction order. Changes within a single transaction are published in order, and the transaction boundary is visible via the `txId` field in events.

```
Transaction 1:
  INSERT orders (id=1)   → event (txId=100, op=c)
  UPDATE users (id=42)   → event (txId=100, op=u)
  COMMIT

Transaction 2:
  UPDATE orders (id=1)   → event (txId=101, op=u)
  COMMIT
```

Both events from transaction 1 appear before transaction 2 events. This lets consumers reconstruct exactly what happened in what order.

---

## What Confused Me

- **CDC is not ETL.** ETL reads the current state periodically. CDC streams every change as it happens. They solve different problems — but CDC often replaces or augments ETL in modern pipelines.
- **Deletes are easy to miss.** With polling-based approaches, you'll never know a row was deleted. Log-based CDC gives you an explicit delete event with the full row before it was deleted.
- **The snapshot phase can be slow.** On a large table with millions of rows, the initial snapshot takes time. Plan for this — your Kafka topic will be backfilled before streaming starts.
- **At-least-once means idempotent consumers.** I assumed events would arrive exactly once. They don't. Every consumer must handle duplicates gracefully.

---

## Resources

- [CDC explained (Confluent blog)](https://www.confluent.io/learn/change-data-capture/)
- [Debezium CDC overview](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [The log: What every software engineer should know (Jay Kreps)](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)

---

*Next: [04 — Debezium](./04_debezium.md)*

*Back to [Postgres & CDC index](./README.md)*
