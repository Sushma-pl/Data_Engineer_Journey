# 07 — Healthchecks

## The Simple Answer

A healthcheck is a command Docker runs periodically inside a container to ask: **"is this service actually ready to do its job?"**

A container can be "running" but completely broken — Postgres is up but hasn't finished initializing, Kafka is started but not yet accepting connections. Without a healthcheck, Docker has no way to know the difference. With one, Docker knows the true state of your service and other containers can wait for it to be genuinely ready.

---

## The Problem It Solves

```
Without healthcheck:
  postgres container starts
  → status: "Up" ✅
  → consumer starts immediately
  → consumer tries to connect to postgres
  → postgres isn't ready yet
  → consumer crashes ❌

With healthcheck:
  postgres container starts
  → status: "starting" ⏳
  → healthcheck runs: pg_isready → fails
  → healthcheck runs again: pg_isready → fails
  → healthcheck runs again: pg_isready → passes ✅
  → status: "healthy"
  → consumer starts (depends_on: condition: service_healthy)
  → consumer connects successfully ✅
```

This is one of the most common sources of startup failures in multi-container data pipelines.

---

## Container Health States

At any point, a container with a healthcheck is in one of three states:

| State | Meaning |
|-------|---------|
| `starting` | Container just started, healthcheck hasn't passed yet |
| `healthy` | Last N healthchecks passed |
| `unhealthy` | Healthcheck has failed more than the allowed retries |

You can see the health state in `docker ps`:

```
CONTAINER ID   IMAGE        STATUS                   NAMES
a1b2c3d4e5f6   postgres:16  Up 30s (healthy)         postgres
9f8e7d6c5b4a   cp-kafka     Up 10s (health: starting) kafka
```

---

## Healthcheck in a Dockerfile

You can bake a healthcheck into the image itself:

```dockerfile
FROM postgres:16

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD pg_isready -U admin -d pipeline_db || exit 1
```

### Parameters Explained

```dockerfile
HEALTHCHECK \
  --interval=10s \       # how often to run the check (default: 30s)
  --timeout=5s \         # how long to wait for the check to respond (default: 30s)
  --start-period=30s \   # grace period before failures count (default: 0s)
  --retries=5 \          # how many consecutive failures = unhealthy (default: 3)
  CMD <your command>
```

**`--start-period` is important.** It gives the service time to initialize before the healthcheck starts counting failures. Kafka, for example, takes 10-20 seconds to fully start. Without a `start-period`, it would be marked unhealthy before it even has a chance.

```
Timeline with --start-period=30s:

0s    ─── container starts
      │
      │   healthcheck runs but failures DON'T count against retries
      │
30s   ─── start-period ends, healthcheck failures now count
      │
35s   ─── first real check: passes → status: healthy ✅
```

---

## Healthcheck in Docker Compose

In Compose, you define the healthcheck under the service. This overrides any healthcheck baked into the image.

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pipeline_db"]
      interval: 10s
      timeout: 5s
      start_period: 30s
      retries: 5
```

### `test` field formats

```yaml
# CMD-SHELL — runs through shell (/bin/sh -c), supports pipes and operators
test: ["CMD-SHELL", "pg_isready -U admin || exit 1"]

# CMD — exec form, no shell, more predictable
test: ["CMD", "pg_isready", "-U", "admin"]

# NONE — disables a healthcheck inherited from the image
test: ["NONE"]
```

Use `CMD-SHELL` when your check needs shell features (`||`, `&&`, pipes). Use `CMD` for simple single-command checks.

---

## Using Healthchecks with `depends_on`

This is where healthchecks pay off in Compose. The `condition` key controls exactly when a dependent service starts:

```yaml
services:

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pipeline_db"]
      interval: 10s
      timeout: 5s
      start_period: 30s
      retries: 5

  consumer:
    build: .
    depends_on:
      postgres:
        condition: service_healthy   # ← waits until postgres healthcheck passes
```

### Three `depends_on` conditions

```yaml
depends_on:
  service-a:
    condition: service_started    # container is running (default, no healthcheck needed)
  service-b:
    condition: service_healthy    # healthcheck has passed ✅ — use this for databases, brokers
  service-c:
    condition: service_completed_successfully  # for one-off init containers (exit 0)
```

**Rule of thumb for data engineering:**
- Databases → `service_healthy`
- Message brokers (Kafka) → `service_healthy`
- Init / migration containers → `service_completed_successfully`
- Sidecars with no healthcheck → `service_started`

---

## Real Healthchecks for Common Data Engineering Services

### PostgreSQL

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
  interval: 10s
  timeout: 5s
  start_period: 20s
  retries: 5
```

### Kafka (Confluent)

```yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
  interval: 15s
  timeout: 10s
  start_period: 60s    # kafka takes a while to fully start
  retries: 10
```

### Redis

```yaml
healthcheck:
  test: ["CMD", "redis-cli", "ping"]
  interval: 5s
  timeout: 3s
  start_period: 5s
  retries: 3
```

`redis-cli ping` returns `PONG` if Redis is healthy. Simple and reliable.

### A Custom Python HTTP Service

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"]
  interval: 15s
  timeout: 5s
  start_period: 10s
  retries: 3
```

Your `/health` endpoint should return HTTP 200 when the app is ready. `curl -f` exits with a non-zero code on HTTP errors — that's what signals an unhealthy state to Docker.

### A Custom Python Service (No curl)

If your image doesn't have `curl` (slim images often don't):

```yaml
healthcheck:
  test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8080/health')\" || exit 1"]
  interval: 15s
  timeout: 5s
  start_period: 10s
  retries: 3
```

Or install `curl` in your Dockerfile:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
```

---

## Full Example — Streaming Pipeline with Healthchecks

```yaml
version: "3.9"

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    healthcheck:
      test: ["CMD-SHELL", "echo ruok | nc localhost 2181 | grep imok"]
      interval: 10s
      timeout: 5s
      start_period: 20s
      retries: 5
    networks:
      - streaming-net

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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      start_period: 60s
      retries: 10
    networks:
      - streaming-net

  postgres:
    image: postgres:16
    container_name: postgres
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pipeline_db"]
      interval: 10s
      timeout: 5s
      start_period: 20s
      retries: 5
    networks:
      - streaming-net

  consumer:
    build: .
    container_name: consumer
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    environment:
      KAFKA_BROKER: kafka:29092
      DB_HOST: postgres
      DB_PORT: 5432
    networks:
      - streaming-net
    restart: on-failure

volumes:
  postgres-data:

networks:
  streaming-net:
    driver: bridge
```

The consumer only starts after both Kafka and Postgres have passed their healthchecks. No race conditions, no crash loops on startup.

---

## Debugging Healthchecks

```bash
# see health status
docker ps

# see healthcheck history — last 5 results with output
docker inspect --format='{{json .State.Health}}' postgres | python -m json.tool

# manually run the healthcheck command to debug it
docker exec postgres pg_isready -U admin -d pipeline_db

# view logs for a failing container
docker logs postgres
```

When a healthcheck fails, `docker inspect` shows you the last few results including the output of the failing command — this tells you exactly why it's failing.

---

## Disabling an Inherited Healthcheck

Sometimes an official image has a healthcheck baked in that you want to override or disable:

```yaml
# disable it entirely
healthcheck:
  disable: true

# or using test: NONE
healthcheck:
  test: ["NONE"]
```

---

## What Confused Me

- **Container "Up" does not mean "ready."** This took a while to internalize. A Postgres container is "Up" the moment the process starts, but it takes several more seconds before it accepts connections. Always use `service_healthy` with a proper healthcheck for anything your pipeline depends on.
- **`start_period` is not optional for slow services.** Kafka starts slowly. Without a `start_period` long enough, it gets marked unhealthy before it's had a chance to fully boot, and dependent services never start.
- **`depends_on` without a condition does nothing useful.** `depends_on: kafka` just waits for the Kafka container to start, not for Kafka to be ready. You need `condition: service_healthy` to get the actual guarantee.
- **Healthcheck commands run inside the container.** So `curl` needs to be installed in the image. On slim images, it often isn't. Either install it or use a Python/shell alternative.
- **Exit code is everything.** The healthcheck command must exit `0` for healthy and any non-zero for unhealthy. That's why commands end with `|| exit 1` — to ensure a non-zero exit on failure even if the shell swallows it.

---

## Resources

- [Dockerfile HEALTHCHECK reference](https://docs.docker.com/engine/reference/builder/#healthcheck)
- [Compose healthcheck reference](https://docs.docker.com/compose/compose-file/compose-file-v3/#healthcheck)
- [Compose depends_on reference](https://docs.docker.com/compose/compose-file/compose-file-v3/#depends_on)

---

*Back to [Docker index](./README.md)*
