# 05 — Postgres + Debezium + Kafka: Full Setup

## The Simple Answer

This is where everything comes together. We'll set up a complete CDC pipeline:
- **PostgreSQL** configured for logical replication
- **Kafka + Zookeeper** as the event bus
- **Kafka Connect + Debezium** as the CDC connector
- **A test table** to produce change events
- **Verify** that events flow from Postgres all the way into Kafka topics

---

## The Full Stack

```
┌─────────────┐    WAL     ┌───────────────────┐   events   ┌──────────┐
│  PostgreSQL │ ─────────► │  Kafka Connect     │ ─────────► │  Kafka   │
│  port 5432  │            │  + Debezium        │            │  port    │
│             │            │  port 8083         │            │  9092    │
└─────────────┘            └───────────────────┘            └──────────┘
                                                                   │
                                                             ┌──────────┐
                                                             │Zookeeper │
                                                             │port 2181 │
                                                             └──────────┘
```

---

## Step 1 — Docker Compose

Create a `docker-compose.yml` that brings up the full stack:

```yaml
version: "3.9"

services:

  # ── Zookeeper ──────────────────────────────────────────────────────────
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
      - cdc-net

  # ── Kafka ──────────────────────────────────────────────────────────────
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
      # Two listeners: one for internal (container-to-container), one for external (host)
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      start_period: 60s
      retries: 10
    networks:
      - cdc-net

  # ── PostgreSQL ─────────────────────────────────────────────────────────
  postgres:
    image: postgres:16
    container_name: postgres
    # Enable logical replication via command flags
    command: >
      postgres
      -c wal_level=logical
      -c max_replication_slots=10
      -c max_wal_senders=10
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/01_init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pipeline_db"]
      interval: 10s
      timeout: 5s
      start_period: 20s
      retries: 5
    networks:
      - cdc-net

  # ── Kafka Connect + Debezium ───────────────────────────────────────────
  kafka-connect:
    image: debezium/connect:2.4
    container_name: kafka-connect
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:29092                    # internal Kafka address
      GROUP_ID: cdc-connect-cluster
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets
      STATUS_STORAGE_TOPIC: connect-status
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8083/ || exit 1"]
      interval: 15s
      timeout: 10s
      start_period: 60s
      retries: 10
    networks:
      - cdc-net

volumes:
  postgres-data:

networks:
  cdc-net:
    driver: bridge
```

> **Note on the Debezium image:** `debezium/connect:2.4` is a pre-built Kafka Connect image with Debezium connectors already installed. This is the easiest way to get started — no manual plugin installation needed.

---

## Step 2 — init.sql: Set Up Postgres for CDC

Create `init.sql` in the same directory. This runs automatically when the Postgres container first starts.

```sql
-- init.sql

-- Create a dedicated user for Debezium with replication privileges
CREATE ROLE debezium_user WITH LOGIN PASSWORD 'debezium_secret' REPLICATION;
GRANT CONNECT ON DATABASE pipeline_db TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- Create the orders table
CREATE TABLE public.orders (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    product     VARCHAR(255) NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    status      VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create the users table
CREATE TABLE public.users (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Set REPLICA IDENTITY FULL so UPDATE/DELETE events include the full old row
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.users REPLICA IDENTITY FULL;

-- Create a publication for the tables we want to replicate
-- (Debezium can also create this automatically, but explicit is better)
CREATE PUBLICATION debezium_pub FOR TABLE public.orders, public.users;
```

---

## Step 3 — Start the Stack

```bash
docker compose up -d
```

Wait for all services to be healthy. You can check:

```bash
docker compose ps
```

All services should show `(healthy)`. Kafka Connect takes the longest — up to 60 seconds.

---

## Step 4 — Deploy the Debezium Connector

Once Kafka Connect is healthy, deploy the connector by POSTing its config to the REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-cdc-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "debezium_user",
      "database.password": "debezium_secret",
      "database.dbname": "pipeline_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_pub",
      "table.include.list": "public.orders,public.users",
      "snapshot.mode": "initial",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false",
      "transforms.unwrap.delete.handling.mode": "rewrite"
    }
  }'
```

### Check connector status

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status | python -m json.tool
```

You should see:
```json
{
  "name": "postgres-cdc-connector",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }]
}
```

If state is `FAILED`, check the logs:
```bash
docker logs kafka-connect --tail 50
```

---

## Step 5 — Verify Kafka Topics Were Created

```bash
# exec into the Kafka container
docker exec -it kafka bash

# list topics (should see dbserver1.public.orders and dbserver1.public.users)
kafka-topics --bootstrap-server localhost:9092 --list
```

You should see:
```
connect-configs
connect-offsets
connect-status
dbserver1.public.orders
dbserver1.public.users
```

---

## Step 6 — Produce Changes and Watch Events Flow

### Insert data into Postgres

```bash
docker exec -it postgres psql -U admin -d pipeline_db
```

```sql
-- Insert a user
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');

-- Insert an order
INSERT INTO orders (user_id, product, amount) VALUES (1, 'Laptop', 1299.99);

-- Update the order
UPDATE orders SET status = 'shipped' WHERE id = 1;

-- Delete the order
DELETE FROM orders WHERE id = 1;
```

### Consume from Kafka and watch events

```bash
# consume messages from the orders topic
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.orders \
  --from-beginning
```

You should see JSON events for each change:

**INSERT event:**
```json
{
  "id": 1,
  "user_id": 1,
  "product": "Laptop",
  "amount": 1299.99,
  "status": "pending",
  "created_at": "2026-10-24T12:00:00.000Z",
  "updated_at": "2026-10-24T12:00:00.000Z"
}
```

**UPDATE event (status changed to shipped):**
```json
{
  "id": 1,
  "user_id": 1,
  "product": "Laptop",
  "amount": 1299.99,
  "status": "shipped",
  "created_at": "2026-10-24T12:00:00.000Z",
  "updated_at": "2026-10-24T12:00:01.000Z"
}
```

> Note: we used the `ExtractNewRecordState` SMT in step 4, so events show just the row data — not the full Debezium envelope. Remove the `transforms` config if you want to see the full envelope with `before`, `after`, `op`, `source` fields.

---

## Step 7 — Verify the Replication Slot in Postgres

```bash
docker exec -it postgres psql -U admin -d pipeline_db
```

```sql
SELECT slot_name, plugin, slot_type, active, confirmed_flush_lsn
FROM pg_replication_slots;
```

You should see `debezium_slot` as `active = true`.

---

## Common Errors and Fixes

### Connector status is FAILED

```bash
docker logs kafka-connect --tail 100
```

Common causes:
- Postgres not reachable (check hostname — it must match the Docker service name, e.g., `postgres`)
- Wrong credentials
- `wal_level` is not `logical` — check with `SHOW wal_level;` in Postgres
- Publication doesn't exist — run `CREATE PUBLICATION debezium_pub FOR ALL TABLES;` manually

### No messages in Kafka topic

- Check snapshot is complete: look for `Finished performing snapshot` in Kafka Connect logs
- Verify publication includes the table: `SELECT * FROM pg_publication_tables;`
- Check `table.include.list` uses the format `schema.table` (e.g., `public.orders`)

### Snapshot running forever / OOM

- Large tables with millions of rows take time
- Increase Kafka Connect memory: add `KAFKA_HEAP_OPTS: "-Xms512m -Xmx2g"` to environment
- Use `snapshot.mode: "never"` to skip snapshot if you only want new changes

### Tombstone / null message errors in consumer

Delete events produce two messages in Kafka:
1. The delete event with the old row data
2. A `null` value message (tombstone for log compaction)

Your consumer must handle `null` payloads:

```python
for msg in consumer:
    if msg.value is None:
        # tombstone — row was deleted, handle accordingly
        print(f"Tombstone for key: {msg.key}")
        continue
    process_event(msg.value)
```

---

## The Full Event Envelope (Without SMT)

If you remove the `transforms` config, you get the full Debezium envelope. This is more verbose but contains more information:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "user_id": 1,
    "product": "Laptop",
    "amount": 1299.99,
    "status": "pending"
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "dbserver1",
    "ts_ms": 1698123456789,
    "snapshot": "false",
    "db": "pipeline_db",
    "schema": "public",
    "table": "orders",
    "txId": 756,
    "lsn": 1752231,
    "xmin": null
  },
  "op": "c",
  "ts_ms": 1698123456900,
  "transaction": null
}
```

Use the full envelope when you need:
- The `before` values for updates/deletes
- Transaction ID for grouping related changes
- The `op` field to distinguish insert/update/delete in your consumer
- The LSN for debugging position in WAL

---

## Quick Reference — Useful Commands

```bash
# start the stack
docker compose up -d

# check all services are healthy
docker compose ps

# deploy connector
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connector.json

# check connector status
curl http://localhost:8083/connectors/postgres-cdc-connector/status

# list Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# consume from a topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic dbserver1.public.orders --from-beginning

# check replication slot lag
docker exec postgres psql -U admin -d pipeline_db -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;"

# view Kafka Connect logs
docker logs kafka-connect -f

# delete connector
curl -X DELETE http://localhost:8083/connectors/postgres-cdc-connector

# tear down everything (keeps volumes)
docker compose down

# tear down and wipe all data
docker compose down -v
```

---

## What Confused Me

- **The Debezium image `debezium/connect` vs `confluentinc/cp-kafka-connect`** — Debezium provides its own Kafka Connect image with all connectors pre-installed. The Confluent image requires manually installing connector JARs. For learning, use `debezium/connect`.
- **`database.hostname` must be the Docker service name**, not `localhost`. Inside Docker's network, `localhost` points to the Kafka Connect container itself, not Postgres. Use `postgres` (the service name in `docker-compose.yml`).
- **Kafka Connect takes a long time to start.** It needs to create its internal topics (`connect-configs`, `connect-offsets`, `connect-status`) and register with Kafka before it's ready. The healthcheck on port 8083 is essential — without `depends_on: condition: service_healthy`, the connector POST will fail.
- **Connector config is not a file.** You POST JSON to an HTTP endpoint. Save your connector JSON to a file (`connector.json`) and use `curl -d @connector.json` to make it easier to redeploy.
- **Schemas in `key.converter.schemas.enable: false`** — by default, Debezium wraps every message in a schema envelope. Setting this to `false` gives you clean JSON without the schema metadata. This is fine for development.

---

## Resources

- [Debezium getting started tutorial](https://debezium.io/documentation/reference/stable/tutorial.html)
- [Debezium PostgreSQL connector config reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties)
- [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Debezium Docker examples (GitHub)](https://github.com/debezium/debezium-examples)

---

*Back to [Postgres & CDC index](./README.md)*
