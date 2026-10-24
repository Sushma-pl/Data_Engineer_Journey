# 05 — Volumes and Networks

---

# Part 1: Volumes

## The Simple Answer

Containers are ephemeral — when you remove a container, everything written inside it is gone. **Volumes** are how you persist data beyond the life of a container. They live on your host machine and get mounted into the container.

For data engineers this is critical. You don't want your Postgres data wiped every time you restart a container.

---

## The Problem Volumes Solve

```
Without volumes:
  Container starts → data written inside container → container removed → data gone ❌

With volumes:
  Container starts → data written to volume (on host) → container removed → data still there ✅
  New container starts → mounts same volume → data is back ✅
```

---

## Three Types of Storage Mounts

```
┌─────────────────────────────────────────────────────┐
│                    Host Machine                      │
│                                                      │
│  /var/lib/docker/volumes/  ← Named Volumes           │
│  /any/path/on/host         ← Bind Mounts             │
│  RAM                       ← tmpfs Mounts            │
│                                                      │
│         ┌──────────────────────┐                     │
│         │      Container       │                     │
│         │   /var/lib/postgresql│                     │
│         └──────────────────────┘                     │
└─────────────────────────────────────────────────────┘
```

| Type | What it is | When to use |
|------|-----------|-------------|
| **Named Volume** | Docker-managed storage | Persisting DB data, service state |
| **Bind Mount** | Maps a host path directly | Local dev, mounting your code |
| **tmpfs Mount** | In-memory, not persisted | Sensitive temp data, fast scratch space |

---

## Named Volumes

Docker manages where the data lives. You just give it a name.

```bash
# create a named volume
docker volume create postgres-data

# use it when running a container
docker run -d \
  --name my-postgres \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=secret \
  -v postgres-data:/var/lib/postgresql/data \
  postgres:16
```

Now if you stop and remove the container, the data in `postgres-data` survives. Start a new container with the same volume and the data is back.

### Volume Commands

```bash
# list all volumes
docker volume ls

# inspect a volume (see where it lives on disk)
docker volume inspect postgres-data

# remove a volume (only when you truly want to wipe the data)
docker volume rm postgres-data

# remove all unused volumes
docker volume prune
```

---

## Bind Mounts

A bind mount maps a specific path on your host directly into the container. Changes on either side are immediately reflected on the other.

```bash
# mount current directory into /app in the container
# Windows PowerShell
docker run -v ${PWD}:/app python:3.11-slim python pipeline.py

# Linux/Mac
docker run -v $(pwd):/app python:3.11-slim python pipeline.py

# explicit path
docker run -v C:\projects\my-pipeline:/app python:3.11-slim python pipeline.py
```

**This is how you develop locally.** You edit code on your machine, and the container picks up the changes immediately — no rebuilding the image.

### Bind Mount vs Named Volume

| | Named Volume | Bind Mount |
|---|---|---|
| Managed by | Docker | You |
| Path on host | Hidden (`/var/lib/docker/...`) | Wherever you specify |
| Best for | Persisting service data (DB, etc.) | Local development, config files |
| Portability | Portable across machines | Tied to specific host path |

---

## Read-Only Mounts

Prevent the container from modifying mounted files — good for config files and secrets.

```bash
docker run -v ${PWD}/config:/app/config:ro python:3.11-slim python pipeline.py
#                                       ^^
#                                  read-only flag
```

---

## Data Engineering Volume Patterns

### Persisting a Database

```bash
docker run -d \
  --name pipeline-db \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=events \
  -v pipeline-db-data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:16
```

### Mounting Code for Local Dev

```bash
docker run -d \
  --name my-consumer \
  -v ${PWD}/src:/app/src \
  -v ${PWD}/config:/app/config:ro \
  my-kafka-consumer:latest
```

### Sharing Data Between Containers

Both containers mount the same named volume — useful for passing files between pipeline stages.

```bash
docker volume create shared-data

docker run -d --name producer -v shared-data:/output my-producer:latest
docker run -d --name consumer -v shared-data:/input my-consumer:latest
```

---

---

# Part 2: Networks

## The Simple Answer

By default, containers are isolated — they can't talk to each other. Docker networks let containers communicate. For data engineering, this is essential: your pipeline code needs to reach Kafka, your Kafka needs to reach Zookeeper, your consumer needs to reach Postgres.

---

## Docker Network Types

| Type | What it does | When to use |
|------|-------------|-------------|
| **bridge** | Default. Containers on same bridge can talk to each other | Single-host setups, local dev |
| **host** | Container shares host's network stack | Performance-critical, Linux only |
| **none** | No network at all | Fully isolated containers |
| **overlay** | Multi-host networking | Docker Swarm / production clusters |

For data engineering on a single machine (local dev), you'll use **bridge networks** almost exclusively.

---

## The Default Bridge Network (and its Problem)

When you run a container without specifying a network, it joins the default bridge network. Containers on the default bridge can only communicate by IP address — not by name.

```bash
docker run -d --name kafka confluentinc/cp-kafka:7.5.0
docker run -d --name consumer my-consumer:latest

# consumer cannot reach kafka by name "kafka" on the default bridge
# it would need the IP: docker inspect kafka → find IP → hardcode it ❌
```

This is fragile. IPs change when containers restart. The solution: **create a custom bridge network**.

---

## Custom Bridge Networks — The Right Way

```bash
# create a custom network
docker network create pipeline-network

# run containers on that network
docker run -d --name zookeeper --network pipeline-network zookeeper:3.8
docker run -d --name kafka --network pipeline-network confluentinc/cp-kafka:7.5.0
docker run -d --name consumer --network pipeline-network my-consumer:latest
```

Now `consumer` can reach `kafka` simply by using the name `kafka` as the hostname. Docker's built-in DNS resolves container names automatically on custom networks.

```python
# inside the consumer container, this just works:
bootstrap_servers = "kafka:9092"
```

---

## Network Commands

```bash
# list networks
docker network ls

# create a network
docker network create my-network

# create with specific subnet
docker network create --subnet=172.20.0.0/16 my-network

# inspect a network (see which containers are connected)
docker network inspect pipeline-network

# connect a running container to a network
docker network connect pipeline-network my-container

# disconnect a container from a network
docker network disconnect pipeline-network my-container

# remove a network
docker network rm my-network

# remove all unused networks
docker network prune
```

---

## Container DNS — How Name Resolution Works

On a custom bridge network, Docker runs an internal DNS server. Container names and network aliases become hostnames.

```
pipeline-network
    │
    ├── zookeeper  (reachable as "zookeeper:2181")
    ├── kafka      (reachable as "kafka:9092")
    ├── postgres   (reachable as "postgres:5432")
    └── consumer   (can reach all of the above by name)
```

This is exactly how Docker Compose works under the hood — it creates a network and puts all services on it.

---

## Port Mapping vs Container Networking

This is a common point of confusion:

| | Port Mapping (`-p`) | Container Networking |
|---|---|---|
| Purpose | Access container from **your host machine** | Containers talking **to each other** |
| Syntax | `-p 5432:5432` | `--network my-net` |
| Who needs it | You, connecting from your laptop | Other containers in the same network |

```
Your Browser/Code                    Docker Network
(host machine)                       (internal)

localhost:9092  ──── port map ────►  kafka:9092
                                         │
                                   consumer:app  (no port map needed)
```

Your consumer container inside the Docker network talks to `kafka:9092` directly. You map port 9092 to your host only if you want to connect to Kafka from your laptop (e.g., with a Kafka client tool).

---

## Real-World Example — Streaming Pipeline Network

```bash
# create the network
docker network create streaming-net

# start zookeeper
docker run -d \
  --name zookeeper \
  --network streaming-net \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  confluentinc/cp-zookeeper:7.5.0

# start kafka (references zookeeper by name)
docker run -d \
  --name kafka \
  --network streaming-net \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.5.0

# start postgres
docker run -d \
  --name postgres \
  --network streaming-net \
  -e POSTGRES_PASSWORD=secret \
  -v pipeline-db:/var/lib/postgresql/data \
  postgres:16

# start your consumer (talks to kafka and postgres by name)
docker run -d \
  --name consumer \
  --network streaming-net \
  -e KAFKA_BROKER=kafka:9092 \
  -e DB_HOST=postgres \
  my-consumer:latest
```

In practice you'd use Docker Compose for this (next note) — but understanding that Compose is just automating exactly this is important.

---

## What Confused Me

**Volumes:**
- Forgetting that `docker rm` doesn't remove volumes. Volumes outlive containers by design. Use `docker volume prune` to clean up orphaned volumes.
- On Windows with bind mounts, path syntax matters. Use `${PWD}` in PowerShell, not `$(pwd)` (that's bash).
- Named volumes are better than bind mounts for database data — Docker handles permissions, and it works the same across operating systems.

**Networks:**
- The default bridge network not supporting DNS resolution tripped me up. A container named `kafka` is NOT reachable as `kafka` on the default network — only on custom networks.
- Confusing port mapping with container networking. Port mapping is for you (host access). Network names are for containers talking to each other.
- `KAFKA_ADVERTISED_LISTENERS` — Kafka needs to advertise the right hostname for external vs internal connections. This is a notorious source of confusion when setting up Kafka in Docker.

---

## Resources

- [Docker storage docs](https://docs.docker.com/storage/)
- [Docker networking docs](https://docs.docker.com/network/)
- [Docker networking deep dive (article)](https://www.docker.com/blog/understanding-docker-networking-drivers-use-cases/)

---

*Next: [06 — Docker Compose](./06_docker_compose.md)*

*Back to [Docker index](./README.md)*
