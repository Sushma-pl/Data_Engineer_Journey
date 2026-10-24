# 06 — Docker Compose

## The Simple Answer

Docker Compose lets you define and run **multiple containers together** using a single YAML file called `docker-compose.yml`. Instead of running a dozen `docker run` commands with all their flags, you describe your entire stack in one file and bring it all up with one command.

For data engineers, this is how you run a full local stack — Kafka, Zookeeper, Postgres, your pipeline — with `docker compose up`.

---

## The Problem It Solves

Without Compose, running a streaming pipeline locally looks like this:

```bash
docker network create streaming-net
docker run -d --name zookeeper --network streaming-net ...
docker run -d --name kafka --network streaming-net ...
docker run -d --name postgres --network streaming-net -v pg-data:/var/lib/postgresql/data ...
docker run -d --name consumer --network streaming-net -e KAFKA_BROKER=kafka:9092 ...
```

With Compose, all of this becomes:

```bash
docker compose up -d
```

And to tear everything down:

```bash
docker compose down
```

One file. One command. Reproducible for every teammate.

---

## `docker-compose.yml` Structure

```yaml
version: "3.9"               # Compose file version

services:                    # each service = one container
  service-name:
    image: ...               # use an existing image
    build: ...               # or build from a Dockerfile
    ports: ...               # port mappings
    environment: ...         # env vars
    volumes: ...             # volume mounts
    networks: ...            # which networks to join
    depends_on: ...          # startup order
    healthcheck: ...         # health check config

volumes:                     # named volumes declaration
  volume-name:

networks:                    # custom networks declaration
  network-name:
```

---

## A Real Data Engineering Stack

Here's a production-realistic `docker-compose.yml` for a streaming pipeline with Kafka and Postgres:

```yaml
version: "3.9"

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - streaming-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - streaming-net

  postgres:
    image: postgres:16
    container_name: postgres
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql  # runs on first start
    networks:
      - streaming-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pipeline_db"]
      interval: 10s
      timeout: 5s
      retries: 5

  consumer:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: consumer
    depends_on:
      postgres:
        condition: service_healthy   # wait for postgres to be healthy
    environment:
      KAFKA_BROKER: kafka:29092       # internal broker address
      DB_HOST: postgres
      DB_PORT: 5432
      DB_USER: admin
      DB_PASSWORD: secret
      DB_NAME: pipeline_db
    volumes:
      - ./src:/app/src               # bind mount for dev
    networks:
      - streaming-net
    restart: on-failure

volumes:
  postgres-data:

networks:
  streaming-net:
    driver: bridge
```

---

## Key Concepts Explained

### `services`

Each service is a container. The service name (e.g., `kafka`, `postgres`) becomes the **hostname** on the network — other containers use it to connect.

```yaml
services:
  kafka:      # other containers reach kafka as "kafka:9092"
  postgres:   # other containers reach postgres as "postgres:5432"
```

### `build` vs `image`

```yaml
# use a pre-built image from a registry
service-a:
  image: postgres:16

# build from a local Dockerfile
service-b:
  build:
    context: .           # build context (where Dockerfile is)
    dockerfile: Dockerfile
```

### `depends_on`

Controls startup order. But it only waits for the container to start — not for the service inside to be ready. For truly waiting on readiness, combine with `healthcheck`:

```yaml
consumer:
  depends_on:
    postgres:
      condition: service_healthy   # waits until healthcheck passes
    kafka:
      condition: service_started   # just waits for container to start
```

### `environment`

Two ways to define environment variables:

```yaml
# map format
environment:
  POSTGRES_USER: admin
  POSTGRES_PASSWORD: secret

# list format
environment:
  - POSTGRES_USER=admin
  - POSTGRES_PASSWORD=secret

# pull from a .env file (best practice for secrets)
env_file:
  - .env
```

### Using `.env` for Secrets

Create a `.env` file in the same directory as `docker-compose.yml`:

```
# .env (add this to .gitignore!)
POSTGRES_USER=admin
POSTGRES_PASSWORD=supersecret
POSTGRES_DB=pipeline_db
```

Reference in Compose:

```yaml
postgres:
  image: postgres:16
  env_file:
    - .env
```

**Never commit `.env` to git.** Add it to `.gitignore`.

### `volumes` in Services

```yaml
services:
  postgres:
    volumes:
      # named volume (for data persistence)
      - postgres-data:/var/lib/postgresql/data

      # bind mount (for config/init files)
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql

      # bind mount for local dev (code hot-reload)
      - ./src:/app/src

# declare named volumes at the top level
volumes:
  postgres-data:
```

### `restart` Policy

```yaml
service:
  restart: "no"           # default — don't restart
  restart: always         # always restart (even on manual stop)
  restart: on-failure     # restart only if it crashes
  restart: unless-stopped # restart unless you explicitly stop it
```

For pipeline services, `on-failure` is usually the right choice in dev. In production, `unless-stopped` or orchestrators like Kubernetes handle this.

---

## Essential Compose Commands

### Starting and Stopping

```bash
# start all services in detached mode
docker compose up -d

# start and force rebuild images
docker compose up -d --build

# start a specific service only
docker compose up -d kafka

# stop all services (containers stopped, not removed)
docker compose stop

# stop and remove containers + networks (volumes kept)
docker compose down

# stop and remove EVERYTHING including volumes (wipes all data!)
docker compose down -v
```

> ⚠️ `docker compose down -v` deletes your named volumes — all database data gone. Only use when you want a full reset.

### Checking Status and Logs

```bash
# see status of all services
docker compose ps

# view logs for all services
docker compose logs

# follow logs in real time
docker compose logs -f

# logs for a specific service only
docker compose logs -f kafka

# last 50 lines
docker compose logs --tail 50 consumer
```

### Running Commands

```bash
# exec into a running service container
docker compose exec postgres bash
docker compose exec postgres psql -U admin -d pipeline_db

# run a one-off command in a new container (doesn't need the service running)
docker compose run --rm consumer python -c "from src.main import check_config; check_config()"
```

### Managing Individual Services

```bash
# restart one service
docker compose restart consumer

# stop one service
docker compose stop kafka

# rebuild and restart one service (after code changes)
docker compose up -d --build consumer

# scale a service (run multiple instances)
docker compose up -d --scale consumer=3
```

---

## Multiple Compose Files (Override Pattern)

A powerful pattern for handling dev vs production config:

```
docker-compose.yml          ← base config (shared)
docker-compose.dev.yml      ← dev overrides (bind mounts, debug ports)
docker-compose.prod.yml     ← prod overrides (resource limits, no bind mounts)
```

```bash
# dev: merges base + dev overrides
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# prod
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

`docker-compose.dev.yml` just overrides what's different:

```yaml
# docker-compose.dev.yml
services:
  consumer:
    volumes:
      - ./src:/app/src    # bind mount for hot reload in dev only
    environment:
      LOG_LEVEL: DEBUG
```

---

## Data Engineering Patterns with Compose

### Pattern 1: Infrastructure Only

Run just the infrastructure (Kafka, Postgres) in Docker, run your pipeline code locally for faster iteration:

```bash
docker compose up -d kafka postgres zookeeper
# now run your Python pipeline locally, pointing to localhost:9092
python src/consumer.py
```

### Pattern 2: Full Stack

Everything in Docker — good for sharing with teammates or testing the full system:

```bash
docker compose up -d --build
```

### Pattern 3: Init Scripts

Postgres runs any `.sql` files in `/docker-entrypoint-initdb.d/` on first start:

```yaml
postgres:
  volumes:
    - ./sql/init.sql:/docker-entrypoint-initdb.d/01_init.sql
    - ./sql/seed.sql:/docker-entrypoint-initdb.d/02_seed.sql
```

Files run in alphabetical order. Great for creating schemas and seeding test data automatically.

---

## Quick Reference

| Command | What it does |
|---------|-------------|
| `docker compose up -d` | Start all services in background |
| `docker compose up -d --build` | Rebuild images then start |
| `docker compose down` | Stop and remove containers |
| `docker compose down -v` | Stop, remove containers AND volumes |
| `docker compose ps` | Status of all services |
| `docker compose logs -f` | Follow all logs |
| `docker compose logs -f <service>` | Follow one service's logs |
| `docker compose exec <service> bash` | Shell into a service |
| `docker compose restart <service>` | Restart one service |
| `docker compose up -d --build <service>` | Rebuild and restart one service |

---

## What Confused Me

- **`depends_on` doesn't mean "wait until ready."** Kafka takes several seconds to be fully up after its container starts. If your consumer starts too fast and tries to connect before Kafka is ready, it crashes. Fix: add a healthcheck to Kafka and use `condition: service_healthy`, or add retry logic in your consumer code.
- **Two Kafka listener addresses.** Inside the Docker network, containers use `kafka:29092` (internal). From your host machine, you use `localhost:9092` (external). Mixing these up is the #1 Kafka-in-Docker headache.
- **`docker compose down` vs `docker compose down -v`** — forgot the `-v` flag when I wanted a clean slate and wondered why old data was showing up. Also got burned the other way — used `-v` and wiped data I needed.
- **Forgetting `--build`** — changed code, ran `docker compose up -d`, nothing changed. You have to pass `--build` to rebuild the image. Or use bind mounts in dev so changes reflect without rebuilding.

---

## Resources

- [Docker Compose docs](https://docs.docker.com/compose/)
- [Compose file reference](https://docs.docker.com/compose/compose-file/)
- [Awesome Compose](https://github.com/docker/awesome-compose) — real-world compose examples including Kafka, Spark, Airflow setups

---

*Back to [Docker index](./README.md)*
