# 01 — PostgreSQL vs MySQL — Key Differences for Data Engineers

## The Simple Answer

If you know MySQL, you know about 80% of what you need for Postgres. The SQL syntax is very similar. But Postgres has deeper support for data types, better standards compliance, and — most importantly for data engineers — **built-in logical replication** that makes CDC possible without third-party plugins.

This note covers the differences that actually matter when working with Postgres as a data engineering source.

---

## The Big Mindset Shift

MySQL and Postgres have different philosophies:

| | MySQL | PostgreSQL |
|---|---|---|
| Philosophy | Fast reads, simple use cases | Standards-compliant, extensible, powerful |
| Owned by | Oracle | Open source community (PostgreSQL Global Development Group) |
| Best known for | Web apps (LAMP stack) | Complex queries, analytics, extensibility |
| CDC support | Requires binary log + plugins | Built-in logical replication (no plugins) |
| JSON support | Basic | Excellent (`jsonb`, indexable) |
| Custom types | Limited | Very powerful (enums, composite, arrays, custom) |

---

## Syntax Differences You'll Hit Immediately

### Auto-increment columns

```sql
-- MySQL
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    amount DECIMAL(10,2)
);

-- PostgreSQL — use SERIAL or IDENTITY
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,          -- older style
    amount DECIMAL(10,2)
);

-- Modern Postgres (v10+) — preferred
CREATE TABLE orders (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    amount DECIMAL(10,2)
);
```

### String quoting

```sql
-- MySQL accepts double quotes for strings
SELECT * FROM orders WHERE status = "active";   -- works in MySQL

-- PostgreSQL: double quotes = identifiers (table/column names), single quotes = strings
SELECT * FROM orders WHERE status = 'active';   -- correct in Postgres
SELECT * FROM "orders" WHERE "status" = 'active'; -- quoting identifiers
```

This trips up MySQL users constantly. In Postgres, `"active"` means a column or table named `active`.

### LIMIT / OFFSET

```sql
-- Both work the same
SELECT * FROM orders LIMIT 10 OFFSET 20;
```

### String concatenation

```sql
-- MySQL
SELECT CONCAT(first_name, ' ', last_name) FROM users;

-- PostgreSQL — both work, but || is idiomatic
SELECT first_name || ' ' || last_name FROM users;
SELECT CONCAT(first_name, ' ', last_name) FROM users;  -- also works
```

### `ILIKE` — case-insensitive LIKE

```sql
-- MySQL LIKE is case-insensitive by default (depends on collation)
SELECT * FROM users WHERE email LIKE '%gmail%';

-- PostgreSQL LIKE is case-sensitive
SELECT * FROM users WHERE email LIKE '%gmail%';    -- case-sensitive
SELECT * FROM users WHERE email ILIKE '%gmail%';   -- case-insensitive (Postgres-specific)
```

### `NOW()` and timestamps

```sql
-- Both work
SELECT NOW();            -- returns timestamp with timezone in Postgres
SELECT CURRENT_TIMESTAMP;

-- Postgres-specific: timezone-aware by default
SELECT NOW() AT TIME ZONE 'UTC';
```

---

## Data Types — What's Different and Better

### JSON

Postgres has two JSON types. This matters a lot when Debezium sends CDC events as JSON.

```sql
-- json: stores raw JSON text, validates on insert, no indexing
CREATE TABLE events (payload JSON);

-- jsonb: stores binary, faster queries, supports indexing ← use this
CREATE TABLE events (payload JSONB);

-- querying jsonb
SELECT payload->>'event_type' FROM events;            -- get as text
SELECT payload->'metadata'->>'source' FROM events;   -- nested
SELECT * FROM events WHERE payload @> '{"status": "active"}';  -- contains
```

### Arrays

```sql
-- Postgres supports native arrays
CREATE TABLE pipeline (
    id SERIAL PRIMARY KEY,
    tags TEXT[],
    metrics INTEGER[]
);

INSERT INTO pipeline (tags) VALUES (ARRAY['kafka', 'streaming', 'etl']);

-- query: find rows containing a tag
SELECT * FROM pipeline WHERE 'kafka' = ANY(tags);
```

### UUID

```sql
-- Postgres has a native UUID type (MySQL stores as VARCHAR)
CREATE TABLE events (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

UUIDs are common as primary keys in Postgres, especially for distributed systems and CDC — they don't conflict when merging data from multiple sources.

### TIMESTAMPTZ vs TIMESTAMP

```sql
-- TIMESTAMP: stores without timezone (dangerous across timezones)
created_at TIMESTAMP

-- TIMESTAMPTZ: stores with timezone (always use this)
created_at TIMESTAMPTZ DEFAULT NOW()
```

**Always use `TIMESTAMPTZ` in Postgres.** MySQL's DATETIME is closer to Postgres's `TIMESTAMP` — both lack timezone info. For CDC pipelines spanning regions and systems, timezone-aware timestamps prevent a lot of subtle bugs.

### NUMERIC — exact precision for money and measurements

```sql
-- NUMERIC(precision, scale) — exact, no floating point errors
price    NUMERIC(10, 2)   -- up to 10 digits total, 2 after decimal
quantity NUMERIC(6, 3)

-- vs FLOAT/DOUBLE — approximate, can have rounding errors
price    FLOAT            -- ❌ never use for money
```

**In CDC events, NUMERIC columns come through as strings, not numbers.** This is intentional — JSON numbers lose precision for large decimals. Debezium serializes `NUMERIC` as a string like `"1299.99"` to preserve exact precision.

```json
{
  "after": {
    "id": 1,
    "amount": "1299.99",   ← string, not float — parse with Decimal() in Python
    "price": "9.990"       ← trailing zeros preserved
  }
}
```

```python
from decimal import Decimal
amount = Decimal(event["after"]["amount"])  # safe, exact
```

### generate_series — generate test data

`generate_series` is a Postgres function that generates a sequence of values. It's invaluable for creating test data to feed into your CDC pipeline.

```sql
-- generate a sequence of integers
SELECT generate_series(1, 10);

-- generate timestamps — one per hour for the last 24 hours
SELECT generate_series(
    NOW() - INTERVAL '24 hours',
    NOW(),
    INTERVAL '1 hour'
) AS event_time;

-- generate test orders (most useful pattern)
INSERT INTO orders (user_id, product, amount, status)
SELECT
    (random() * 100 + 1)::INTEGER,                          -- random user_id 1-100
    'Product-' || generate_series,                           -- Product-1, Product-2...
    ROUND((random() * 500 + 10)::NUMERIC, 2),               -- random amount 10-510
    (ARRAY['pending','shipped','delivered'])[ceil(random()*3)::INT]
FROM generate_series(1, 1000);                               -- 1000 rows

-- verify
SELECT COUNT(*), MIN(amount), MAX(amount) FROM orders;
```

This is how you stress-test your CDC pipeline — insert thousands of rows, update them, delete them, and verify every event lands in Kafka correctly.

```sql
-- simulate updates on random rows (good for CDC testing)
UPDATE orders
SET status = 'shipped', updated_at = NOW()
WHERE id IN (
    SELECT id FROM orders
    ORDER BY random()
    LIMIT 100
);
```

---

## Schemas — A Postgres Concept MySQL Doesn't Have

In MySQL, "database" and "schema" are basically the same thing. In Postgres they are different.

```
PostgreSQL cluster
└── Database: my_database
    ├── Schema: public          ← default schema
    │   ├── Table: orders
    │   └── Table: users
    ├── Schema: analytics
    │   ├── Table: daily_summary
    │   └── Table: user_metrics
    └── Schema: staging
        └── Table: raw_events
```

```sql
-- create a schema
CREATE SCHEMA staging;

-- create a table in a specific schema
CREATE TABLE staging.raw_events (
    id SERIAL PRIMARY KEY,
    payload JSONB,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

-- query with schema prefix
SELECT * FROM staging.raw_events;

-- set default schema for session (like USE in MySQL)
SET search_path TO staging, public;
```

**Why this matters for CDC/Debezium:** Debezium topic names include the schema. A table `public.orders` gets a Kafka topic like `dbserver1.public.orders`. Understanding schemas helps you configure topic routing correctly.

---

## Roles and Permissions (Different from MySQL Users)

```sql
-- MySQL: CREATE USER then GRANT
CREATE USER 'replicator'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';

-- PostgreSQL: CREATE ROLE (users are roles with LOGIN)
CREATE ROLE debezium_user WITH LOGIN PASSWORD 'secret' REPLICATION;
GRANT CONNECT ON DATABASE mydb TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;
```

The `REPLICATION` attribute on a role is what allows it to read the WAL — essential for CDC. This is more explicit than MySQL's binary log approach.

---

## Transaction Behavior

Postgres is more strictly ACID-compliant than MySQL (InnoDB). A few things to know:

```sql
-- DDL is transactional in Postgres (not in MySQL!)
BEGIN;
ALTER TABLE orders ADD COLUMN processed_at TIMESTAMPTZ;
-- can ROLLBACK this in Postgres, cannot in MySQL
ROLLBACK;

-- Postgres uses MVCC (Multi-Version Concurrency Control)
-- Readers never block writers, writers never block readers
-- This is what makes logical replication efficient
```

---

## Key Postgres-Only Features You'll Use

### `RETURNING` clause

```sql
-- Get the inserted row back immediately (very useful in pipelines)
INSERT INTO orders (user_id, amount)
VALUES (42, 99.99)
RETURNING id, created_at;

-- Also works with UPDATE and DELETE
UPDATE orders SET status = 'processed' WHERE id = 1
RETURNING id, status, updated_at;
```

### `ON CONFLICT` (upsert)

```sql
-- Insert or update if conflict on primary key
INSERT INTO user_metrics (user_id, event_count)
VALUES (42, 1)
ON CONFLICT (user_id)
DO UPDATE SET event_count = user_metrics.event_count + 1;

-- Insert or ignore if conflict
INSERT INTO events (id, payload)
VALUES ('abc-123', '{"type": "click"}')
ON CONFLICT (id) DO NOTHING;
```

This is the Postgres equivalent of MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`. You'll use this a lot when consuming CDC events and writing to a target database.

### Window functions (both support, Postgres is richer)

```sql
-- Running total per user
SELECT
    user_id,
    event_time,
    amount,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY event_time) AS running_total
FROM transactions;
```

---

## psql — The Postgres CLI (equivalent to mysql CLI)

```bash
# connect to a database
psql -h localhost -p 5432 -U admin -d pipeline_db

# connect inside Docker container
docker exec -it postgres psql -U admin -d pipeline_db
```

### Useful psql commands

```sql
\l                    -- list databases (like SHOW DATABASES in MySQL)
\c pipeline_db        -- connect to a database (like USE in MySQL)
\dt                   -- list tables in current schema
\dt public.*          -- list tables in public schema
\d orders             -- describe table structure (like DESCRIBE in MySQL)
\dn                   -- list schemas
\du                   -- list roles/users
\x                    -- toggle expanded output (great for wide rows)
\timing               -- show query execution time
\q                    -- quit
```

---

## What Confused Me

- **Double quotes are NOT for strings.** Burned by this multiple times. `WHERE name = "John"` in Postgres is looking for a column named `John`, not the value `John`. Always use single quotes for string values.
- **Schemas are not databases.** In MySQL I used to create a new "database" for each project component. In Postgres, you create one database and use schemas to organize tables within it.
- **`SERIAL` vs `IDENTITY`** — `SERIAL` is the old way and still works, but `GENERATED ALWAYS AS IDENTITY` is the modern standard and behaves better (harder to accidentally insert explicit values).
- **`TIMESTAMPTZ` vs `TIMESTAMP`** — always use `TIMESTAMPTZ`. Found this out the hard way when CDC events showed wrong timestamps after a DST change.

---

## Resources

- [PostgreSQL vs MySQL comparison](https://www.postgresql.org/about/featurematrix/)
- [psql command reference](https://www.postgresql.org/docs/current/app-psql.html)
- [PostgreSQL data types](https://www.postgresql.org/docs/current/datatype.html)

---

*Next: [02 — WAL and Logical Replication](./02_wal_and_replication.md)*

*Back to [Postgres & CDC index](./README.md)*
