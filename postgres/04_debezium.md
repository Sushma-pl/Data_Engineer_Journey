# 04 — Debezium

## The Simple Answer

Debezium is an **open-source CDC platform** that sits between your database and Kafka. It reads your database's transaction log, transforms each change into a structured event, and publishes it to a Kafka topic. You configure it, and it handles the rest — replication slots, offsets, retries, schema changes.

Think of Debezium as a translator: Postgres speaks WAL, Kafka speaks events. Debezium is fluent in both.

---

## Where Debezium Fits

```
┌────────────┐     WAL / binlog     ┌─────────────────┐     events     ┌─────────┐
│  Postgres  │ ──────────────────► │    Debezium      │ ─────────────► │  Kafka  │
│  (source)  │   logical replication│    Connector     │  structured    │ (topics)│
└────────────┘                      └─────────────────┘  JSON events   └─────────┘
                                           │
                                    Kafka Connect
                                    (the runtime)
```

Debezium runs **inside Kafka Connect** — a framework for scalable, fault-tolerant data integration with Kafka. Kafka Connect is what actually runs the connector process, handles offsets, and provides the REST API you use to manage connectors.

---

## Debezium Architecture

### The key components

```
┌──────────────────────────────────────────────────────────────┐
│                      Kafka Connect                            │
│                                                              │
│   ┌──────────────────────────────────────────────────────┐  │
│   │              Debezium PostgreSQL Connector           │  │
│   │                                                      │  │
│   │  ┌────────────┐   ┌──────────────┐   ┌──────────┐  │  │
│   │  │  WAL       │   │   Event      │   │  Schema  │  │  │
│   │  │  Reader    │──►│   Transformer│──►│  Registry│  │  │
│   │  └────────────┘   └──────────────┘   │  (opt.)  │  │  │
│   │        │                             └──────────┘  │  │
│   │        │ replication slot                           │  │
│   └────────┼───────────────────────────────────────────┘  │
│            │                                               │
│   Kafka Connect stores offsets in Kafka topics             │
└────────────┼───────────────────────────────────────────────┘
             │
          Postgres
          (WAL / logical replication)
```

### Kafka Connect

Kafka Connect is the runtime that hosts connectors. It provides:
- **Offset management** — tracks which WAL position was last processed, stored in a Kafka topic
- **Fault tolerance** — if the connector crashes, it resumes from the last saved offset
- **REST API** — deploy, monitor, and manage connectors via HTTP
- **Scaling** — can run in distributed mode across multiple workers

### Source Connectors vs Sink Connectors

| Type | Direction | Example |
|------|-----------|---------|
| **Source connector** | External system → Kafka | Debezium (DB → Kafka) |
| **Sink connector** | Kafka → External system | JDBC Sink (Kafka → DB), Elasticsearch Sink |

Debezium is a **source connector**. You'll often pair it with a sink connector to get data from Kafka into your destination.

---

## Debezium Connectors

Debezium supports multiple databases, each with its own connector:

| Database | Connector | Notes |
|----------|-----------|-------|
| **PostgreSQL** | `io.debezium.connector.postgresql.PostgresConnector` | Uses `pgoutput` plugin |
| **MySQL** | `io.debezium.connector.mysql.MySqlConnector` | Uses binary log |
| **MongoDB** | `io.debezium.connector.mongodb.MongoDbConnector` | Uses oplog / change streams |
| **SQL Server** | `io.debezium.connector.sqlserver.SqlServerConnector` | Uses CDC tables |
| **Oracle** | `io.debezium.connector.oracle.OracleConnector` | LogMiner |
| **Cassandra** | `io.debezium.connector.cassandra.CassandraConnector` | Commit log |

We focus on the **PostgreSQL connector** in these notes.

---

## Kafka Topic Naming

By default, Debezium creates one Kafka topic per table:

```
{server.name}.{schema}.{table}

examples:
  dbserver1.public.orders
  dbserver1.public.users
  dbserver1.inventory.products
```

- `server.name` is set in your connector config (`topic.prefix` in newer versions)
- One topic = one table
- All change types (INSERT/UPDATE/DELETE) go to the same topic for that table

You can override this with **topic routing** (Single Message Transforms) if needed.

---

## The Connector Configuration

You deploy a Debezium connector by POSTing a JSON config to Kafka Connect's REST API. Here's what each property means for the Postgres connector:

```json
{
  "name": "postgres-orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    // --- Connection ---
    "database.hostname": "postgres",          // hostname of your Postgres container/server
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "secret",
    "database.dbname": "pipeline_db",
    "database.server.name": "dbserver1",      // prefix for all topic names

    // --- Replication ---
    "plugin.name": "pgoutput",                // use built-in pgoutput (Postgres 10+)
    "slot.name": "debezium_slot",             // name of the replication slot to create
    "publication.name": "debezium_pub",       // name of the publication to use/create
    "publication.autocreate.mode": "all_tables", // auto-create publication for all tables

    // --- What to capture ---
    "table.include.list": "public.orders,public.users",  // only these tables
    // OR
    "table.exclude.list": "public.flyway_schema_history", // exclude these

    // --- Snapshot ---
    "snapshot.mode": "initial",               // snapshot once on first run

    // --- Kafka ---
    "topic.prefix": "dbserver1",              // newer config name for server.name

    // --- Schema ---
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

### Key config options explained

```
connector.class          → which connector plugin to use
database.hostname        → where is Postgres? (container name in Docker)
database.server.name     → prefix for all Kafka topic names
plugin.name              → pgoutput (built-in) or wal2json
slot.name                → name of WAL replication slot (Debezium creates this)
publication.name         → name of Postgres publication
snapshot.mode            → how to handle existing data on startup
table.include.list       → only capture changes for these tables (schema.table format)
topic.prefix             → prefix for Kafka topic names (newer versions)
```

---

## Offset Management

Debezium tracks its position in the WAL using Kafka Connect's offset storage. Offsets are stored in a special Kafka topic (by default `connect-offsets`).

```
Kafka Connect offsets topic:
  key:   ["postgres-orders-connector", {"server": "dbserver1"}]
  value: {"lsn": 1752231, "txId": 756, "ts_usec": 1698123456800000}
```

This means:
- If Debezium restarts, it resumes from the last committed offset
- If the Kafka Connect offset topic is lost, Debezium will re-snapshot
- Offsets are per-connector, stored durably in Kafka

---

## Single Message Transforms (SMTs)

SMTs let you modify events in-flight before they hit Kafka. Common uses:

### Extract just the `after` field (flatten the envelope)

```json
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode": "rewrite"
```

Without this, every Kafka message has the full Debezium envelope (`before`, `after`, `source`, `op`). With this transform, the message becomes just the row data — simpler for consumers that don't need the metadata.

### Route to different topics

```json
"transforms": "route",
"transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
"transforms.route.renames": "status:order_status"
```

### Add a field

```json
"transforms": "addField",
"transforms.addField.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addField.static.field": "pipeline_version",
"transforms.addField.static.value": "v2"
```

---

## Schema Registry (Optional but Important)

By default, Debezium serializes events as JSON. At scale, this is wasteful (JSON is verbose) and fragile (no schema enforcement). The alternative is **Avro + Schema Registry**.

```
Debezium → Avro serializer → Kafka (compact binary)
                ↕
         Schema Registry
         (stores schema versions)
```

Consumers deserialize with the schema. This gives you:
- **Smaller messages** (Avro is much more compact than JSON)
- **Schema evolution** — add fields without breaking consumers
- **Schema validation** — bad events are rejected

For a learning setup, JSON is fine. For production, use Avro + Schema Registry.

---

## Monitoring Debezium

### Kafka Connect REST API

```bash
# list all connectors
curl http://localhost:8083/connectors

# get connector status
curl http://localhost:8083/connectors/postgres-orders-connector/status

# get connector config
curl http://localhost:8083/connectors/postgres-orders-connector/config

# delete a connector
curl -X DELETE http://localhost:8083/connectors/postgres-orders-connector

# restart a connector
curl -X POST http://localhost:8083/connectors/postgres-orders-connector/restart
```

### Connector status explained

```json
{
  "name": "postgres-orders-connector",
  "connector": { "state": "RUNNING", "worker_id": "kafka-connect:8083" },
  "tasks": [
    { "id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083" }
  ],
  "type": "source"
}
```

States: `RUNNING`, `PAUSED`, `FAILED`, `UNASSIGNED`

If state is `FAILED`, check the Kafka Connect logs:
```bash
docker logs kafka-connect
```

### Replication lag (Postgres side)

```sql
SELECT
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
```

---

## What Confused Me

- **Debezium doesn't run standalone.** It runs inside Kafka Connect. You deploy Kafka Connect, then deploy Debezium as a connector configuration to it. This two-layer setup confused me at first.
- **The connector config is JSON posted to an HTTP endpoint, not a config file.** There's no `debezium.conf` you edit. You POST JSON to `localhost:8083/connectors`. This is Kafka Connect's design.
- **`database.server.name` vs `topic.prefix`** — older Debezium versions use `database.server.name` to prefix topics. Newer versions (2.0+) use `topic.prefix`. The effect is the same. Check your Debezium version.
- **SMT `ExtractNewRecordState`** — without this, every Kafka message has the full Debezium envelope. Many consumers just want the row data. This transform flattens it. I spent time wondering why my consumer was getting nested `after.field` instead of just `field`.
- **Tombstone messages** — when a row is deleted, Debezium sends two messages: a delete event, then a `null` value message (tombstone). The tombstone is for Kafka log compaction — it tells Kafka "this key is gone." Your consumer needs to handle `null` values.

---

## Resources

- [Debezium docs](https://debezium.io/documentation/)
- [Debezium PostgreSQL connector docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Kafka Connect REST API docs](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Debezium SMT docs](https://debezium.io/documentation/reference/stable/transformations/)

---

*Next: [05 — Postgres + Debezium + Kafka Setup](./05_debezium_postgres_kafka.md)*

*Back to [Postgres & CDC index](./README.md)*
