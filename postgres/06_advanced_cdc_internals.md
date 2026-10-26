# 06 — Advanced CDC Internals

These are the topics that separate someone who has set up Debezium once from someone who runs it reliably in production. Every item here has caused real outages or data quality issues in real pipelines. If you're preparing for interviews or building ShopStream to be production-grade, know all of these.

---

## 1. TOAST and `__debezium_unavailable_value`

### What is TOAST?

**TOAST** = The Oversized-Attribute Storage Technique. Postgres stores large column values (anything over ~2KB) out-of-line in a separate TOAST table rather than inline in the main row.

```
Main table row:  id=1, status='pending', description=<TOAST pointer>
                                                        │
                                         TOAST table:  large text value lives here
```

### The CDC problem

When you UPDATE a row and change a small column (like `status`), Postgres writes the updated row to the WAL. But for TOAST columns that **didn't change**, Postgres does NOT re-fetch and re-write the large value — it just omits it from the WAL record to save space.

Debezium can't reconstruct the full row from the WAL alone, so it uses a sentinel value:

```json
{
  "before": {
    "id": 1,
    "status": "pending",
    "description": "__debezium_unavailable_value"   ← TOAST column was not re-fetched
  },
  "after": {
    "id": 1,
    "status": "shipped",
    "description": "__debezium_unavailable_value"   ← same here
  },
  "op": "u"
}
```

Your consumer sees `__debezium_unavailable_value` and must not interpret it as a real value — it means "this column was not included in the WAL record."

### The fix

Set `REPLICA IDENTITY FULL` on tables with TOAST columns. This forces Postgres to include the full old row in the WAL — including re-fetching TOAST values.

```sql
ALTER TABLE products REPLICA IDENTITY FULL;
```

**Trade-off:** `REPLICA IDENTITY FULL` increases WAL volume for tables with large columns because it writes the entire old row on every UPDATE. For tables with large TEXT or JSONB columns that update frequently, this can meaningfully increase WAL size. The alternative is to handle `__debezium_unavailable_value` in your consumers and treat it as "unchanged."

### Which data types trigger TOAST?

| Type | TOAST triggered when |
|------|---------------------|
| `TEXT` | > ~2KB |
| `VARCHAR` | > ~2KB |
| `BYTEA` | > ~2KB |
| `JSONB` | > ~2KB |
| `XML` | > ~2KB |
| `ARRAY` | large arrays |

Small columns (INT, TIMESTAMPTZ, NUMERIC, short VARCHAR) never use TOAST.

---

## 2. Heartbeat Events and Idle-Table Slot Lag

### The problem — idle tables and WAL retention

Replication slot lag is measured against `pg_current_wal_lsn()`. But if a table has **no writes** for a long time, the slot's `confirmed_flush_lsn` doesn't advance either — even though Debezium is healthy and connected.

Meanwhile, Postgres may generate WAL from other activity (autovacuum, other tables). The slot's `restart_lsn` stays pinned at the old position, and WAL files accumulate.

```
Table A: no writes for 2 days
  slot confirmed_flush_lsn: stuck at 2 days ago
  pg_current_wal_lsn:       at today
  → apparent lag: 2 days of WAL (but Debezium is actually healthy!)
```

This causes false alerts on lag monitoring and prevents WAL cleanup.

### The fix — heartbeat events

Debezium has a **heartbeat** feature. It periodically writes a small update to a dedicated heartbeat table, which forces a WAL record, which advances the slot position.

**Step 1 — Create the heartbeat table:**

```sql
CREATE TABLE debezium_heartbeat (
    id      SERIAL PRIMARY KEY,
    ts      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- grant debezium_user write access to this table
GRANT INSERT, UPDATE ON debezium_heartbeat TO debezium_user;
```

**Step 2 — Enable heartbeats in connector config:**

```json
{
  "heartbeat.interval.ms": "30000",
  "heartbeat.action.query": "INSERT INTO public.debezium_heartbeat (ts) VALUES (NOW()) ON CONFLICT (id) DO UPDATE SET ts = NOW()"
}
```

Every 30 seconds, Debezium writes to the heartbeat table. This:
- Advances the replication slot LSN even when monitored tables are idle
- Keeps WAL from accumulating due to pinned slot position
- Confirms Debezium is alive (no heartbeat = connector is stuck)

**Exclude the heartbeat table from your topic:**

```json
"table.exclude.list": "public.debezium_heartbeat"
```

You don't want heartbeat events flooding your Kafka topics.

### Monitoring slot lag correctly

```sql
-- real lag: difference between current WAL and what the slot has confirmed
SELECT
    slot_name,
    active,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
    ) AS confirmed_lag,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) AS restart_lag
FROM pg_replication_slots;
```

`confirmed_lag` is what Debezium has actually processed. `restart_lag` is the total WAL Postgres must retain for the slot. With heartbeats, both stay small even when tables are idle.

---

## 3. Schema Changes (DDL) and Logical Decoding

### What happens when you ALTER TABLE?

DDL changes (ALTER TABLE, ADD COLUMN, DROP COLUMN, RENAME COLUMN) are a major pain point in CDC. How Debezium handles them depends on the change type.

### Safe DDL — adding a nullable column

```sql
ALTER TABLE orders ADD COLUMN notes TEXT;
```

This is **safe**. After the change:
- New events include the `notes` field (null for rows that existed before)
- Debezium detects the schema change on the next event and updates its internal schema
- Kafka topic gets new events with the extra field

Your consumer may need to handle the schema evolution (new field appearing mid-stream).

### Risky DDL — dropping or renaming a column

```sql
ALTER TABLE orders DROP COLUMN notes;
ALTER TABLE orders RENAME COLUMN amount TO total_amount;
```

These are **dangerous** if done without coordination:
- Debezium's cached schema no longer matches the WAL records
- Events may fail to decode or produce garbled output
- You may need to restart the connector to force schema refresh

### How Debezium tracks schema changes

Debezium stores a **schema history** in a dedicated Kafka topic (default: `dbhistory.pipeline_db`). Every time the schema changes, Debezium writes the new DDL to this topic. On restart, it replays the history to reconstruct the schema at any past WAL position.

```json
// connector config — required for schema history
"schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
"schema.history.internal.kafka.topic": "schema-changes.pipeline_db"
```

### Best practice for DDL changes in a running CDC pipeline

1. **Add nullable columns freely** — they're backward compatible
2. **Never drop or rename without a migration plan** — pause the connector, apply DDL, restart
3. **Use `table.include.list`** to limit exposure to only tables you care about
4. **For major schema changes**, consider resetting the connector (delete + recreate) to force a fresh snapshot

### DDL in the WAL — what Postgres captures

With `wal_level = logical`, Postgres writes DDL events to the WAL too. You can see them:

```sql
-- use test_decoding to see DDL in the WAL
SELECT * FROM pg_logical_slot_peek_changes('debug_slot', NULL, NULL)
WHERE data LIKE '%ALTER%' OR data LIKE '%CREATE%';
```

---

## 4. Transactions, Commit Order, and Large Transactions

### Commit order — what Debezium guarantees

Debezium emits events **in commit order**, not in the order rows were modified. If transaction A commits before transaction B, all of A's events appear before B's in Kafka — even if B's rows were modified first.

```
Timeline:
  T=1: Transaction 1 modifies rows A, B, C (not yet committed)
  T=2: Transaction 2 modifies row D, COMMITS
  T=3: Transaction 1 COMMITS

Kafka events order:
  → row D changed (from Transaction 2, committed first)
  → row A changed (from Transaction 1)
  → row B changed (from Transaction 1)
  → row C changed (from Transaction 1)
```

This is correct and expected. Your consumers must not assume wall-clock order.

### Large transactions — the memory buffer problem

For small transactions, Debezium buffers the changes in memory and emits them when it sees COMMIT. For **very large transactions** (millions of rows in a single transaction), this buffer can exhaust memory.

Debezium 2.0+ handles this by **spooling to disk** when the buffer exceeds a threshold:

```json
"incremental.snapshot.chunk.size": "1024",
"max.batch.size": "2048",
"max.queue.size": "8192"
```

For bulk operations (ETL loads, large migrations) wrapped in a single transaction:

```sql
-- ❌ Bad for CDC — one massive transaction
BEGIN;
INSERT INTO orders SELECT * FROM orders_archive;  -- 10 million rows
COMMIT;

-- ✅ Better — smaller batches, each its own transaction
DO $$
DECLARE
    batch_size INT := 10000;
    offset_val INT := 0;
BEGIN
    LOOP
        INSERT INTO orders
        SELECT * FROM orders_archive LIMIT batch_size OFFSET offset_val;
        EXIT WHEN NOT FOUND;
        COMMIT;
        offset_val := offset_val + batch_size;
    END LOOP;
END $$;
```

### Transaction metadata in events

Each Debezium event carries `txId` so consumers can group events by transaction:

```json
{
  "source": { "txId": 756, "lsn": 1752231 },
  "op": "c"
}
```

If you need transactional consistency downstream (all-or-nothing for a transaction's events), use Debezium's **transaction metadata** feature:

```json
"provide.transaction.metadata": "true"
```

This adds a `transaction` field to each event and publishes BEGIN/END markers to a separate topic.

---

## 5. WAL Safeguards

These are the Postgres parameters that protect you from WAL growing unbounded. Every production CDC setup must have these tuned.

### `max_replication_slots`

```ini
max_replication_slots = 10   # default
```

Maximum number of replication slots that can exist at the same time. Each Debezium connector uses one slot. If you try to create more slots than this allows, Debezium fails to start.

**Gotcha:** old, inactive slots count against this limit. Always drop slots you're no longer using:

```sql
-- find inactive slots
SELECT slot_name, active FROM pg_replication_slots WHERE active = false;

-- drop an old slot
SELECT pg_drop_replication_slot('old_slot_name');
```

### `max_wal_senders`

```ini
max_wal_senders = 10   # default
```

Maximum number of simultaneous WAL sender processes (replication connections). Debezium uses one WAL sender per connector. Set this to at least `max_replication_slots + 2` (the +2 is for physical replication if you have read replicas).

### `max_slot_wal_keep_size`

```ini
max_slot_wal_keep_size = -1   # default: -1 = unlimited ← dangerous
```

This is the **most important safety parameter** that most tutorials don't mention.

By default (`-1`), a stale replication slot can retain WAL indefinitely, filling your disk completely and crashing Postgres.

Set a limit:

```ini
max_slot_wal_keep_size = 10GB   # Postgres will invalidate the slot rather than run out of disk
```

When the limit is hit, Postgres **invalidates the slot** (slot_invalidation_reason = 'wal_removed'). This means Debezium will need to re-snapshot. That's painful, but a crashing Postgres server is worse.

```sql
-- check if any slots have been invalidated
SELECT slot_name, active, invalidation_reason
FROM pg_replication_slots
WHERE invalidation_reason IS NOT NULL;
```

### Recommended production settings

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 15
max_slot_wal_keep_size = 10GB    # adjust based on your disk size
wal_keep_size = 0                # let slots handle retention, not this
```

---

## 6. Monitoring CDC in Postgres

### `pg_stat_replication` — active WAL sender connections

```sql
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, flush_lsn)) AS flush_lag,
    sync_state
FROM pg_stat_replication;
```

Shows each active replication connection. When Debezium is connected, you'll see one row here. `state` should be `streaming`.

### `pg_replication_slots` — all slots (active and inactive)

```sql
SELECT
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    invalidation_reason,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) AS total_wal_retained,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
    ) AS unprocessed_lag
FROM pg_replication_slots;
```

Key columns:
- `active` — is Debezium currently connected?
- `confirmed_flush_lsn` — last LSN Debezium confirmed it processed
- `restart_lsn` — oldest WAL Postgres must keep for this slot
- `invalidation_reason` — non-null means the slot was force-invalidated (bad)
- `total_wal_retained` — disk space being held by this slot

### `pg_stat_replication_slots` — slot statistics (Postgres 14+)

```sql
SELECT
    slot_name,
    spill_txns,          -- transactions spilled to disk (large txn indicator)
    spill_count,
    spill_bytes,
    stream_txns,         -- transactions streamed (normal operations)
    total_txns,
    total_bytes
FROM pg_stat_replication_slots;
```

High `spill_txns` means large transactions are spilling to disk — a sign you should split up bulk operations.

### WAL disk usage

```sql
-- total WAL directory size
SELECT pg_size_pretty(sum(size)) AS wal_size
FROM pg_ls_waldir();

-- current WAL position
SELECT pg_current_wal_lsn();

-- WAL being retained by each slot
SELECT
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
```

### Useful monitoring dashboard query

One query to rule them all — paste this into your monitoring tool:

```sql
SELECT
    s.slot_name,
    s.active,
    s.plugin,
    COALESCE(r.application_name, 'disconnected') AS consumer,
    COALESCE(r.state, 'inactive') AS state,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)
    ) AS lag,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)
    ) AS wal_retained,
    s.invalidation_reason
FROM pg_replication_slots s
LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid;
```

**Alert thresholds to set:**
- `lag > 500MB` → Debezium is falling behind
- `wal_retained > 5GB` → potential disk risk
- `active = false` AND slot exists → Debezium is down, WAL accumulating
- `invalidation_reason IS NOT NULL` → slot lost, re-snapshot required

---

## 7. Managed Postgres — AWS RDS

Running Postgres on AWS RDS (or Aurora PostgreSQL) for CDC requires different configuration because you don't have OS-level access to `postgresql.conf`.

### Enable logical replication on RDS

RDS uses **parameter groups** instead of `postgresql.conf`:

```
AWS Console:
  RDS → Parameter Groups → Create parameter group (postgres16)
  → Set: rds.logical_replication = 1
  → Apply to your RDS instance
  → Reboot required
```

Or via AWS CLI:

```bash
aws rds modify-db-parameter-group \
  --db-parameter-group-name my-pg-params \
  --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"

# reboot the instance to apply
aws rds reboot-db-instance --db-instance-identifier my-postgres-instance
```

Setting `rds.logical_replication = 1` automatically sets `wal_level = logical` and adjusts `max_wal_senders` and `max_replication_slots` to safe defaults.

### Verify it worked

```sql
SHOW wal_level;           -- should return 'logical'
SHOW rds.logical_replication;   -- should return 'on'
```

### RDS-specific gotchas

**No superuser access.** The `rds_superuser` role is not a true superuser. Use `rds_replication` group instead:

```sql
-- on RDS, grant replication via the group
GRANT rds_replication TO debezium_user;
```

**Publication creation requires ownership or superuser.** On RDS:

```sql
-- either create it as rds_superuser
CREATE PUBLICATION debezium_pub FOR ALL TABLES;

-- or grant debezium_user access to create publications
-- (requires rds_superuser)
GRANT rds_replication TO debezium_user;
```

**`pg_hba.conf` is managed by RDS.** You can't edit it directly. Use security groups to control network access and VPC settings to restrict who can connect. RDS handles authentication separately.

**`max_slot_wal_keep_size` on RDS.** You can set this in the parameter group, but RDS also has its own storage autoscaling. Monitor both.

### Aurora PostgreSQL differences

Aurora has additional complexity for CDC:

- Use the **writer instance** endpoint for Debezium (not the reader endpoint)
- Aurora uses a shared distributed storage layer — WAL behavior is slightly different
- Set `rds.logical_replication = 1` in the cluster parameter group (not instance)
- Aurora failover: a new writer is promoted — Debezium reconnects, but the **replication slot is on the old writer and is lost**. This requires a re-snapshot after failover. See note 07 for the failover problem in detail.

### Debezium connector config for RDS

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "my-instance.xxxx.us-east-1.rds.amazonaws.com",
  "database.port": "5432",
  "database.user": "debezium_user",
  "database.password": "secret",
  "database.dbname": "pipeline_db",
  "topic.prefix": "rds_pipeline",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_slot",
  "publication.name": "debezium_pub",
  "table.include.list": "public.orders,public.users",
  "snapshot.mode": "initial",
  "heartbeat.interval.ms": "30000",
  "heartbeat.action.query": "INSERT INTO public.debezium_heartbeat(ts) VALUES(NOW()) ON CONFLICT(id) DO UPDATE SET ts=NOW()"
}
```

The only difference from a self-hosted setup is the hostname. Everything else works the same.

---

## What Confused Me

- **`__debezium_unavailable_value` appearing in UPDATE events** — first time I saw this I thought the data was corrupted. It's not. It just means the TOAST column wasn't included in the WAL record. Set `REPLICA IDENTITY FULL` to fix it, but be aware of the WAL size trade-off.
- **Heartbeats are not optional in production.** I skipped them initially and then had alerts firing constantly about "2 days of lag" on tables with no writes. The lag metric was meaningless without heartbeats keeping the slot position current.
- **`max_slot_wal_keep_size` defaults to unlimited.** This is the most dangerous default in Postgres for CDC setups. Set it. A disk-full Postgres crash is catastrophic.
- **RDS `rds.logical_replication` vs `wal_level`** — on RDS you set the former, which controls the latter. Don't try to set `wal_level` directly in RDS parameter groups — it won't work, and the change won't apply.
- **Large transactions hit memory limits.** Loaded 5 million rows in one transaction during a backfill. Kafka Connect ran out of heap and crashed. Always batch bulk operations.

---

## Resources

- [TOAST docs](https://www.postgresql.org/docs/current/storage-toast.html)
- [Debezium TOAST handling](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-replica-identity)
- [Debezium heartbeat docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-heartbeat-interval-ms)
- [pg_stat_replication docs](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW)
- [AWS RDS logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)

---

*Next: [07 — Internals Reference](./07_reference_and_internals.md)*

*Back to [Postgres & CDC index](./README.md)*
