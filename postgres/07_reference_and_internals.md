# 07 — Reference: Failover, MVCC, and Vacuum

These are the "read and understand" topics — you won't configure these directly in your daily work, but knowing how they work makes you a much stronger data engineer. They come up in interviews and help you reason about CDC reliability, performance, and correctness.

---

## 1. Failover and Logical Replication Slots

### The fundamental problem

Logical replication slots are stored on the **primary** Postgres instance. When a failover happens — primary goes down, standby is promoted — the new primary has no knowledge of the logical slots that existed on the old primary.

```
Normal operation:
  Primary ──WAL──► Debezium (slot: debezium_slot, LSN 0/5A00)
  Primary ──WAL──► Standby (physical replica, no logical slots)

Failover:
  Primary CRASHES
  Standby promoted to new Primary
  
  New Primary has:
    ✅ All data (physical replication was in sync)
    ❌ No logical replication slots  ← gone
    ❌ No record of where Debezium was reading from
```

Debezium connects to the new primary, finds no slot, and must **create a new slot and re-run the initial snapshot**. This means:
- Pipeline is down during the failover + re-snapshot window
- Your Kafka topics will get duplicate events (the snapshot re-sends all existing data)
- Your consumers must handle these duplicates idempotently

### Why can't the slot be replicated to the standby?

Physical replication copies raw WAL bytes. Logical slot state is maintained in Postgres's memory and on-disk structures that are specific to the primary. Copying the physical WAL doesn't copy the slot metadata in a way that survives promotion.

### Postgres 17 — logical slot failover (finally!)

Postgres 17 (released 2024) added **logical replication slot synchronization to standbys**:

```sql
-- on primary: mark a slot as synchronized to standbys
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput', false, true);
--                                                                       ^     ^
--                                                              temporary  failover (sync to standby)
```

With this, after promotion the standby has the slot and Debezium can continue from where it left off — no re-snapshot required. This is a major improvement but requires Postgres 17.

### Practical mitigation for pre-Postgres 17

1. **Design consumers to be idempotent** — re-snapshot will re-deliver all existing rows as "r" (read) events. Consumers using upsert (`ON CONFLICT DO UPDATE`) handle this correctly.
2. **Use `snapshot.mode: "never"` after known failover** — if you know a failover happened and you only want new changes from the new slot position, set this temporarily. You'll miss events that happened between failover and re-connection.
3. **Alert on slot re-creation** — monitor for the slot being created with a timestamp much newer than expected.
4. **Aurora/RDS managed failover** — Aurora promotes a replica automatically. Debezium will lose the slot. Always design for this in managed Postgres environments.

### Failover checklist

```
After failover:
  □ Verify new primary endpoint is updated in Debezium config (or use a CNAME/RDS endpoint that auto-updates)
  □ Check connector status — it will likely be FAILED
  □ Delete and recreate the connector to trigger fresh snapshot
  □ Verify consumers handle duplicate "r" events idempotently
  □ Monitor consumer lag until caught up
```

---

## 2. Physical vs Logical Replication — Side by Side

You've seen both mentioned throughout these notes. Here's the definitive comparison:

### How they work

```
Physical Replication:
  Primary ──── raw WAL bytes (binary page changes) ────► Standby
  Standby applies the bytes directly to its data files
  Result: byte-for-byte identical copy of the primary

Logical Replication:
  Primary ──── decoded row changes (INSERT/UPDATE/DELETE) ────► consumer
  Consumer applies the logical changes to its own tables
  Result: logically equivalent data, not necessarily byte-identical
```

### Comparison table

| Dimension | Physical | Logical |
|-----------|----------|---------|
| What's transferred | Raw page blocks | Decoded row changes |
| Requires same Postgres version | Yes (same major version) | No (different versions OK) |
| Requires same OS/arch | Yes | No |
| Can replicate subset of tables | No (all or nothing) | Yes |
| Can replicate to different DB type | No | Yes (via tools like Debezium) |
| Handles DDL | Automatically | Requires coordination |
| Used for | HA, read replicas, DR | CDC, migration, streaming |
| Setup complexity | Lower | Higher |
| Performance overhead on primary | Lower | Slightly higher |
| Supports bidirectional replication | No | Yes (with care) |

### When you use each in the real world

**Physical replication — you use this for:**
- Read replicas (scale reads without touching the primary)
- Standby instances (failover in minutes)
- Backups via `pg_basebackup`
- Disaster recovery (DR)

**Logical replication — you use this for:**
- CDC with Debezium → Kafka
- Zero-downtime major version upgrades (replicate to new version, switch over)
- Selective data sync between databases
- Replicating data to a different database type (Postgres → other systems)

### Can you run both at the same time?

Yes. `wal_level = logical` is a superset of `replica`. A Postgres instance with `wal_level = logical` can simultaneously:
- Serve physical streaming replication to read replicas
- Serve logical replication to Debezium
- Accept application queries

The WAL contains everything needed for both.

---

## 3. MVCC — Multi-Version Concurrency Control

### What is MVCC?

MVCC is how Postgres lets readers and writers work simultaneously without blocking each other. Instead of locking rows when they're read or written, Postgres **keeps multiple versions of each row** and shows each transaction the version that was current when it started.

```
Row: id=1, status='pending'   ← version created by transaction 100

Transaction 101 (UPDATE):
  Creates new version: id=1, status='shipped'   ← visible to txn >= 102
  Old version still exists: id=1, status='pending'   ← visible to txn <= 101

Transaction 99 (SELECT, started before 101):
  Still sees: id=1, status='pending'   ← snapshot isolation

Transaction 102 (SELECT, started after 101 commits):
  Sees: id=1, status='shipped'   ← newest committed version
```

### Why MVCC matters for data engineers

**Snapshot isolation for the initial CDC snapshot:**

When Debezium runs the initial snapshot, it opens a transaction with `REPEATABLE READ` isolation. This gives it a consistent snapshot of the entire database at a single point in time — even if other transactions are modifying data during the snapshot. The snapshot is consistent because MVCC maintains the old row versions until Debezium's transaction ends.

**Long-running queries and table bloat:**

MVCC keeps old row versions as long as any transaction might need them. If you have a long-running query (or a long-running replication slot), old versions accumulate. This is called **table bloat** — the table grows on disk even if the logical row count hasn't changed.

**Transaction IDs and wraparound:**

Postgres uses 32-bit transaction IDs (XIDs). After ~2 billion transactions, they wrap around. Postgres handles this via VACUUM, but you may see warnings like "database is not being vacuumed" in heavily written databases. This is an operational concern but directly affects CDC — if XID wraparound is not managed, Postgres will eventually refuse connections.

```sql
-- check for tables at risk of XID wraparound
SELECT relname, age(relfrozenxid) AS xid_age
FROM pg_class
WHERE relkind = 'r'
ORDER BY xid_age DESC
LIMIT 10;
```

---

## 4. VACUUM and Autovacuum

### What VACUUM does

MVCC creates dead row versions with every UPDATE and DELETE. These dead rows take up disk space and slow down queries (table scans have to skip over them). **VACUUM** reclaims this space.

```
After 1000 UPDATEs on orders table:
  Live rows:  1000 (current versions)
  Dead rows:  1000 (old versions no longer visible to any transaction)
  
VACUUM orders:
  → marks dead rows as reusable space
  → does NOT immediately return space to OS (use VACUUM FULL for that)
  → updates table statistics
  → advances the frozen transaction ID (prevents XID wraparound)
```

### Autovacuum

Postgres runs autovacuum automatically in the background. It monitors tables and vacuums them when dead tuple count exceeds a threshold.

```sql
-- check autovacuum activity
SELECT
    relname,
    last_autovacuum,
    last_autoanalyze,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 1) AS dead_pct
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 10;
```

### CDC impact on VACUUM

Replication slots prevent VACUUM from removing dead rows that are still needed by the slot. If Debezium is running slowly or is stopped, the slot's `catalog_xmin` pins the oldest transaction that the slot might need — and VACUUM cannot remove dead rows created after that transaction.

```sql
-- check catalog_xmin for slots (how far back VACUUM is blocked)
SELECT slot_name, catalog_xmin, age(catalog_xmin) AS xmin_age
FROM pg_replication_slots;
```

If `xmin_age` is very large, VACUUM is being blocked by the slot and table bloat will grow. This is another reason to keep Debezium running and monitor slot lag.

### VACUUM FULL vs VACUUM

```sql
-- Regular VACUUM — non-blocking, marks space as reusable, doesn't shrink file
VACUUM orders;

-- VACUUM FULL — rewrites the entire table, reclaims disk space to OS
-- ⚠️ Locks the table, blocks reads and writes — use in maintenance windows only
VACUUM FULL orders;

-- ANALYZE — updates query planner statistics (often combined with VACUUM)
VACUUM ANALYZE orders;

-- check when tables were last vacuumed
SELECT relname, last_vacuum, last_autovacuum
FROM pg_stat_user_tables
ORDER BY last_autovacuum DESC;
```

### For high-write CDC source tables

Tables with frequent INSERTs/UPDATEs/DELETEs (like `orders` in ShopStream) accumulate dead tuples quickly. Make sure autovacuum is not getting throttled:

```sql
-- per-table autovacuum tuning for high-write tables
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.01,    -- vacuum after 1% of rows are dead (default 20%)
    autovacuum_analyze_scale_factor = 0.005   -- analyze after 0.5% change (default 10%)
);
```

---

## Quick Reference — Views and Functions

```sql
-- ── Replication ───────────────────────────────────────────────
-- Active replication connections
SELECT * FROM pg_stat_replication;

-- All slots (active + inactive)
SELECT * FROM pg_replication_slots;

-- Slot statistics (pg 14+)
SELECT * FROM pg_stat_replication_slots;

-- Current WAL position
SELECT pg_current_wal_lsn();

-- WAL lag between two positions
SELECT pg_wal_lsn_diff('0/5A00'::pg_lsn, '0/1A00'::pg_lsn);

-- ── Tables and bloat ──────────────────────────────────────────
-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC;

-- Dead tuples
SELECT relname, n_dead_tup, n_live_tup FROM pg_stat_user_tables ORDER BY n_dead_tup DESC;

-- Last vacuum/analyze
SELECT relname, last_autovacuum, last_autoanalyze FROM pg_stat_user_tables;

-- XID age (wraparound risk)
SELECT relname, age(relfrozenxid) FROM pg_class WHERE relkind = 'r' ORDER BY 2 DESC;

-- ── Schema ────────────────────────────────────────────────────
-- Publications
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;

-- Replica identity for all tables
SELECT schemaname, tablename, replica_identity FROM pg_tables
JOIN pg_class ON relname = tablename WHERE schemaname = 'public';

-- Settings
SHOW wal_level;
SELECT name, setting FROM pg_settings WHERE name LIKE '%replication%' OR name LIKE '%wal%';
```

---

## What Confused Me

- **Physical and logical replication can coexist.** I thought enabling logical replication would break my read replicas. It doesn't — `wal_level = logical` is a superset of `replica`. Both work simultaneously.
- **MVCC dead tuples and slot lag interact.** A stale replication slot doesn't just hold WAL files — it also prevents VACUUM from cleaning dead tuples. Two problems for the price of one stale slot.
- **Failover invalidates the slot silently.** Debezium doesn't loudly tell you "the slot is gone." It fails to connect, enters FAILED state, and you need to look at the logs. Monitor slot existence, not just connector state.
- **VACUUM FULL locks tables.** I ran it on a production table thinking "full is better." It locked the table for 20 minutes. Never run VACUUM FULL on a live table without a maintenance window.

---

## Resources

- [MVCC in PostgreSQL](https://www.postgresql.org/docs/current/mvcc-intro.html)
- [VACUUM docs](https://www.postgresql.org/docs/current/sql-vacuum.html)
- [Logical replication failover (Postgres 17)](https://www.postgresql.org/docs/17/logical-replication-failover.html)
- [Physical vs logical replication](https://www.postgresql.org/docs/current/high-availability.html)

---

*Back to [Postgres & CDC index](./README.md)*
