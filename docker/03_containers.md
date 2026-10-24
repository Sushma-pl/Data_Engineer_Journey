# 03 — Containers

## The Simple Answer

A container is a **running instance of an image**. It's an isolated process on your machine with its own filesystem, network, and process space. You can run many containers from the same image, each completely independent.

If an image is a recipe, a container is the meal you made from it.

---

## Container Lifecycle

Understanding the lifecycle helps you know which commands to use when.

```
           docker run
Image ──────────────────► Running Container
                                │
                    docker stop │ docker kill
                                ▼
                          Stopped Container
                                │
                    docker start│
                                ▼
                          Running Container
                                │
                    docker rm   │
                                ▼
                            (gone)
```

A stopped container still exists on disk — it keeps its filesystem state. You can restart it. Only `docker rm` actually deletes it.

---

## Running Containers — `docker run`

`docker run` is the most important Docker command. It creates and starts a container from an image.

```bash
# basic run
docker run python:3.11-slim

# run with an interactive terminal (essential for debugging)
docker run -it python:3.11-slim bash

# run in detached mode (background) — most common for services
docker run -d postgres:16

# give the container a name (much easier to manage than random names)
docker run -d --name my-postgres postgres:16

# run and automatically remove when it stops (great for one-off jobs)
docker run --rm python:3.11-slim python -c "print('hello')"
```

### Port Mapping `-p`

Containers have their own network. To access a service running inside a container from your host machine, you need to map ports.

```bash
# -p host_port:container_port
docker run -d -p 5432:5432 --name my-postgres postgres:16

# now you can connect to postgres at localhost:5432
```

```
Your Machine (host)          Container
   localhost:5432    ──────►  postgres:5432
```

You can map different host ports if 5432 is already taken:
```bash
docker run -d -p 5433:5432 --name my-postgres-2 postgres:16
```

### Environment Variables `-e`

Most official images are configured through environment variables.

```bash
docker run -d \
  --name my-postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=pipeline_db \
  postgres:16
```

Always check the image's Docker Hub page for what environment variables it accepts.

### Volume Mounting `-v`

Mount a folder from your host into the container (covered in detail in the volumes note).

```bash
# mount current directory into /app inside the container
docker run -v $(pwd):/app python:3.11-slim

# Windows PowerShell equivalent
docker run -v ${PWD}:/app python:3.11-slim
```

### Full Example — Running Kafka for Local Dev

```bash
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.5.0
```

---

## Managing Running Containers

### Listing Containers

```bash
# list running containers
docker ps

# list ALL containers (including stopped ones)
docker ps -a

# list only container IDs (useful in scripts)
docker ps -q
```

Output columns explained:
```
CONTAINER ID   IMAGE          COMMAND     CREATED        STATUS         PORTS                    NAMES
a1b2c3d4e5f6   postgres:16    "docker…"   2 hours ago    Up 2 hours     0.0.0.0:5432->5432/tcp   my-postgres
```

### Stopping and Starting

```bash
# gracefully stop a container (sends SIGTERM, waits, then SIGKILL)
docker stop my-postgres

# stop immediately (sends SIGKILL) — use when stop is hanging
docker kill my-postgres

# start a stopped container
docker start my-postgres

# restart a running container
docker restart my-postgres
```

### Removing Containers

```bash
# remove a stopped container
docker rm my-postgres

# force remove a running container (stop + remove in one)
docker rm -f my-postgres

# remove all stopped containers
docker container prune

# remove all stopped containers without confirmation prompt
docker container prune -f
```

---

## Interacting with Running Containers

### Execute Commands Inside a Container

```bash
# open an interactive bash shell in a running container
docker exec -it my-postgres bash

# run a one-off command
docker exec my-postgres psql -U admin -d pipeline_db -c "SELECT version();"

# open a Python shell inside a running Python container
docker exec -it my-python-app python
```

`docker exec` is your best friend for debugging. Got a container behaving unexpectedly? Exec into it and poke around.

### Viewing Logs

```bash
# view logs of a container
docker logs my-postgres

# follow logs in real time (like tail -f)
docker logs -f my-postgres

# last 50 lines only
docker logs --tail 50 my-postgres

# logs with timestamps
docker logs -t my-postgres

# combine: last 50 lines, follow, with timestamps
docker logs --tail 50 -f -t my-postgres
```

### Copying Files

```bash
# copy a file from your machine INTO a container
docker cp my_script.py my-postgres:/tmp/my_script.py

# copy a file OUT of a container
docker cp my-postgres:/var/log/postgresql/postgresql.log ./postgres.log
```

### Inspecting a Container

```bash
# full metadata — network, mounts, env vars, state
docker inspect my-postgres

# just the IP address of a container
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' my-postgres
```

---

## Container Resource Limits

In production (and even locally when running many services), limit what each container can consume.

```bash
# limit to 512MB RAM and 1 CPU
docker run -d \
  --name my-pipeline \
  --memory="512m" \
  --cpus="1.0" \
  my-pipeline:latest
```

For data engineering workloads — Spark, Kafka, Flink — these limits matter a lot. An unbounded Spark container will happily consume all your RAM.

---

## Cleanup — Keeping Your Machine Tidy

Running many containers and building images fills up disk fast.

```bash
# remove all stopped containers, unused networks, dangling images, build cache
docker system prune

# same but also removes unused images (not just dangling)
docker system prune -a

# check how much disk Docker is using
docker system df
```

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Run container (foreground) | `docker run <image>` |
| Run container (background) | `docker run -d <image>` |
| Run with interactive shell | `docker run -it <image> bash` |
| Name a container | `docker run --name <name> <image>` |
| Map port | `docker run -p host:container <image>` |
| Set env var | `docker run -e KEY=VALUE <image>` |
| List running | `docker ps` |
| List all | `docker ps -a` |
| Stop | `docker stop <name>` |
| Start | `docker start <name>` |
| Remove | `docker rm <name>` |
| Exec into container | `docker exec -it <name> bash` |
| View logs | `docker logs -f <name>` |
| Inspect | `docker inspect <name>` |
| Clean up stopped | `docker container prune` |

---

## What Confused Me

- **`docker run` vs `docker start`** — `run` creates a brand new container from an image. `start` restarts an already-created (stopped) container. Running `docker run` twice creates two separate containers.
- **Containers are ephemeral by default.** If you write a file inside a container and then remove it with `docker rm`, that file is gone. Use volumes to persist data.
- **`docker stop` vs `docker kill`** — `stop` is graceful (gives the app time to shut down cleanly). `kill` is immediate. Always prefer `stop` unless you're stuck.
- **Forgetting `-d`** — if you forget detached mode on a long-running service, your terminal gets hijacked. `Ctrl+C` to exit, then re-run with `-d`.

---

## Resources

- [docker run reference](https://docs.docker.com/engine/reference/commandline/run/)
- [docker exec reference](https://docs.docker.com/engine/reference/commandline/exec/)

---

*Next: [04 — Dockerfile](./04_dockerfile.md)*

*Back to [Docker index](./README.md)*
