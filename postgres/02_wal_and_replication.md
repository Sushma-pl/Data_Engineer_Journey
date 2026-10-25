# 02 — WAL and Logical Replication

## The Simple Answer

The **Write-Ahead Log (WAL)** is Postgres's internal record of every change made to the database. Before Postgres writes data to disk, it first writes a description of that change to the WAL. This log is what makes CDC possible — Debezium reads it to capture every INSERT, UPDATE, and DELETE without touching your application or your tables.

---

## What is the WAL?

WAL stands for Write-Ahead Log. The "write-ahead" part means: **write to the log before writing to the actual data files.**

```
Your app runs:  INSERT INTO orders (amount) VALUES (99.99);

What Postgres does:
  1. Write to WAL:  "inserted row with amount=99.99 into orders at LSN 0/1A3F28"
  2. Confirm transaction committed
  3. Eventually flush data to actual table files on disk
```

The WAL exists primarily for **crash recovery** — if Postgres crashes before flushing data to disk, it replays the WAL on restart and no data is lost. But it turns out this log is also a perfect stream of every database change — which is exactly what CDC needs.

### WAL as a timeline

```
WAL (append-only log)
─────────────────────────────────────────────────────────►  time

  [LSN 0/1A3F10]  BEGIN
  [LSN 0/1A3F18]  INSERT INTO orders: {id:1, amount:99.99}
  [LSN 0/1A3F20]  COMMIT
  [LSN 0/1A3F28]  BEGIN
  [LSN 0/1A3F30]  UPDATE orders SET status='shipped' WHERE id=1
  [LSN 0/1A3F38]  COMMIT
  [LSN 0/1A3F40]  DELETE FROM orders WHERE id=1
  [LSN 0/1A3F48]  COMMIT
```

**LSN** = Log Sequence Number. Every WAL record has a unique, ever-increasing LSN. This is how Debezium tracks its position — it remembers the last LSN it read and picks up from there.

---

## Two Types of Replication in Postgres

Postgres has two replication modes. Understanding the difference is essential for CDC.

### Physical Replication (streaming replication)

Copies raw disk blocks — the exact binary data — from primary to replica. Used for **high availability and read replicas**.

```
Primary Postgres ──── raw WAL bytes ────► Replica Postgres
(exact copy of everything)
```

- Replica must be same Postgres version and same OS architecture
- Used for disaster recovery, read scaling
- Not useful for CDC — you can't decode individual row changes from raw bytes

### Logical Replication

Decodes the WAL into **logical row-level changes** — actual INSERT/UPDATE/DELETE events with before and after values. Used for **CDC, selective replication, and streaming to external systems**.

```
Primary Postgres ──── decoded row changes ────► Debezium / consumer
(structured events: what changed, in which table, what were the values)
```

- Works across Postgres versions
- Can replicate specific tables
- Produces human-readable, structured events
- This is what Debezium uses

---

## Logical Decoding

Logical decoding is the process of translating raw WAL bytes into structured row-change events. Postgres uses **output plugins** to do this translation.

```
Raw WAL bytes → [output plugin] → structured change events
```

The output plugin formats the change events. The most important one for CDC:

| Plugin | Format | Used by |
|--------|--------|---------|
| `pgoutput` | Postgres native binary | Debezium (default, built into Postgres 10+) |
| `wal2json` | JSON | Older setups, custom consumers |
| `decoderbufs` | Protobuf | Older Debezium versions |

**Use `pgoutput`** — it's built into Postgres (no installation needed), stable, and what modern Debezium uses by default.

---

## Replication Slots

A **replication slot** is a named cursor into the WAL. It tracks how far a consumer has read and ensures Postgres **doesn't delete WAL files the consumer hasn't read yet**.

```
WAL:  [event1] [event2] [event3] [event4] [event5] ← new events
                           ▲
                    replication slot
                  "debezium_slot" at LSN 0/1A3F30
                  (Debezium has read up to here)

Postgres will NOT delete event3, event4, event5
even if they would normally be cleaned up.
```

### Creating a replication slot manually

```sql
-- create a logical replication slot
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- list existing slots
SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;

-- drop a slot
SELECT pg_drop_replication_slot('debezium_slot');
```

In practice, Debezium creates and manages the slot for you. But you need to understand this because:

> ⚠️ **Critical warning:** If a replication slot exists but its consumer stops reading (e.g., Debezium is down), Postgres will keep accumulating WAL files indefinitely. This can fill up your disk and crash Postgres. Always monitor replication slot lag.

### Monitor slot lag

```sql
-- check how far behind each slot is
SELECT
    slot_name,
    plugin,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

If `lag` is growing and the slot is not `active`, your consumer is down or stuck.

---

## Publications — What to Replicate

A **publication** is a named set of tables (or all tables) that you want to expose for logical replication. Think of it as "subscribing to changes on these tables."

```sql
-- publish all tables
CREATE PUBLICATION debezium_pub FOR ALL TABLES;

-- publish specific tables only
CREATE PUBLICATION debezium_pub FOR TABLE public.orders, public.users, public.products;

-- publish only specific operations
CREATE PUBLICATION debezium_pub FOR TABLE public.orders
    WITH (publish = 'insert, update');   -- skip deletes

-- list publications
SELECT * FROM pg_publication;

-- list tables in a publication
SELECT * FROM pg_publication_tables;

-- drop a publication
DROP PUBLICATION debezium_pub;
```

Debezium will also create a publication for you automatically if you configure it to — but knowing how to do it manually gives you control over exactly which tables are replicated.

---

## Configuring Postgres for Logical Replication

By default, Postgres is configured for physical replication or no replication. You need to change a few settings to enable logical replication for CDC.

### `postgresql.conf` settings

```ini
# Enable logical replication (the most important setting)
wal_level = logical           # default is 'replica', must change to 'logical'

# How many replication slots can exist simultaneously
max_replication_slots = 10    # default is 10, one slot per Debezium connector

# How many WAL senders (replication connections) are allowed
max_wal_senders = 10          # default is 10

# How long to keep WAL files (backup, not usually needed with slots)
wal_keep_size = 0             # default, slots handle retention
```

**`wal_level = logical` requires a Postgres restart.** This is the only setting that needs a restart. The others can be changed with a reload.

### Checking current settings

```sql
-- check wal_level
SHOW wal_level;

-- check all replication-related settings
SELECT name, setting, unit, context
FROM pg_settings
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders');
```

If `wal_level` is not `logical`, CDC will not work.

### In Docker — setting via environment variable

```yaml
# docker-compose.yml
postgres:
  image: postgres:16
  command: postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
  environment:
    POSTGRES_USER: admin
    POSTGRES_PASSWORD: secret
    POSTGRES_DB: pipeline_db
```

Or via a custom config file:

```yaml
postgres:
  image: postgres:16
  volumes:
    - ./postgres/postgresql.conf:/etc/postgresql/postgresql.conf
  command: postgres -c config_file=/etc/postgresql/postgresql.conf
```

---

## REPLICA IDENTITY — The Before Image

By default, when you UPDATE or DELETE a row, the WAL only records the **primary key** of the changed row as the "before" value — not the full old row. For CDC, you usually want the full before and after state.

```sql
-- default: only PK in before image
ALTER TABLE orders REPLICA IDENTITY DEFAULT;

-- full: entire old row in before image (recommended for CDC)
ALTER TABLE orders REPLICA IDENTITY FULL;

-- using an index instead of full row (compromise)
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_pkey;
```

**For CDC with Debezium, set `REPLICA IDENTITY FULL` on tables you care about.** Without it, UPDATE and DELETE events will be missing the old values.

```sql
-- check current replica identity for all tables
SELECT schemaname, tablename, replica_identity
FROM pg_tables
JOIN pg_class ON relname = tablename
WHERE schemaname = 'public';
```

| `replica_identity` value | Meaning |
|--------------------------|---------|
| `d` | DEFAULT — only PK in before image |
| `f` | FULL — entire old row in before image |
| `i` | INDEX — specific index columns |
| `n` | NOTHING — no before image at all |

---

## The Full Picture — How It All Connects

```
┌──────────────────────────────────────────────────────────┐
│                    PostgreSQL                             │
│                                                          │
│  Tables ──writes──► WAL (wal_level=logical)              │
│                      │                                   │
│  Publication ◄────── │ ──────────────────────────────┐  │
│  (which tables)      │                               │  │
│                      ▼                               │  │
│  Replication Slot ◄──── tracks consumer position    │  │
│  (debezium_slot)         ensures WAL is retained    │  │
│                                                     │  │
│  pg_hba.conf ────── allows replication connections ─┘  │
└──────────────────────────────────────────────────────────┘
              │
              │  logical replication protocol
              ▼
         Debezium Connector
```

---

## pg_hba.conf — Allow Replication Connections

Postgres access control is configured in `pg_hba.conf`. Replication connections need explicit permission.

```
# pg_hba.conf
# TYPE   DATABASE        USER            ADDRESS         METHOD
host     replication     debezium_user   0.0.0.0/0       md5
host     pipeline_db     debezium_user   0.0.0.0/0       md5
```

In Docker setups, the official Postgres image is usually permissive enough locally. In production, lock this down.

---

## What Confused Me

- **`wal_level = logical` requires a restart.** I changed it and expected it to take effect immediately. It doesn't — Postgres must restart. Check with `SHOW wal_level;` after restart.
- **Replication slots accumulate WAL.** The first time I stopped Debezium and left the slot in place, disk usage crept up. If you're stopping CDC for a while, either drop the slot or monitor lag.
- **`REPLICA IDENTITY DEFAULT` gives you incomplete events.** UPDATE events with only the PK as "before" image are confusing. Set `REPLICA IDENTITY FULL` on any table you're doing CDC on.
- **Publications vs Replication Slots are separate concepts.** The publication says "what" to replicate. The slot says "where the consumer is." You need both.

---

## Resources

- [PostgreSQL WAL docs](https://www.postgresql.org/docs/current/wal-intro.html)
- [Logical replication docs](https://www.postgresql.org/docs/current/logical-replication.html)
- [Logical decoding docs](https://www.postgresql.org/docs/current/logicaldecoding.html)
- [pg_replication_slots view](https://www.postgresql.org/docs/current/view-pg-replication-slots.html)

---

*Next: [03 — CDC Concepts](./03_cdc_concepts.md)*

*Back to [Postgres & CDC index](./README.md)*
