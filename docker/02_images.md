# 02 — Docker Images

## The Simple Answer

A Docker image is a **read-only blueprint** for creating containers. It contains your application code, runtime, libraries, environment variables, and config — everything needed to run your app. You build an image once and run as many containers from it as you need.

If a container is a running process, an image is what that process was built from.

---

## How Images Work — Layers

This is the most important concept to understand about images. Docker images are built in **layers**, and each layer is cached.

```
┌─────────────────────────────┐
│  Layer 4: COPY your app     │  ← your code
├─────────────────────────────┤
│  Layer 3: RUN pip install   │  ← your dependencies
├─────────────────────────────┤
│  Layer 2: python:3.11-slim  │  ← base image layers
├─────────────────────────────┤
│  Layer 1: debian slim       │  ← OS layer
└─────────────────────────────┘
```

- Each instruction in a `Dockerfile` creates a new layer
- If a layer hasn't changed, Docker reuses the cached version — this makes builds fast
- Layers are shared between images — if two images use the same base, they share those layers on disk

**Why this matters for data engineers:** if you put `COPY requirements.txt` before `COPY . .` in your Dockerfile, Docker only re-installs dependencies when `requirements.txt` changes — not every time your code changes. This saves a lot of build time.

---

## Key Image Commands

### Pulling Images

```bash
# pull latest version of an image
docker pull postgres

# pull a specific version (always do this in production!)
docker pull postgres:16

# pull a slim variant (smaller, fewer tools — good for production)
docker pull python:3.11-slim
```

### Listing Images

```bash
# list all images on your machine
docker images

# same thing, more explicit
docker image ls

# list with full image IDs
docker images --no-trunc
```

Output looks like:

```
REPOSITORY   TAG       IMAGE ID       CREATED        SIZE
postgres     16        a1b2c3d4e5f6   2 weeks ago    412MB
python       3.11-slim 9f8e7d6c5b4a   3 weeks ago    130MB
```

### Inspecting Images

```bash
# see full metadata about an image (layers, env vars, entrypoint, etc.)
docker inspect postgres:16

# see the history of layers in an image
docker image history python:3.11-slim
```

`docker inspect` is extremely useful for debugging — it shows you exactly what environment variables, ports, and entrypoints are baked into an image.

### Removing Images

```bash
# remove a specific image
docker rmi postgres:16

# remove by image ID
docker rmi a1b2c3d4e5f6

# remove all unused images (not referenced by any container)
docker image prune

# remove ALL images (careful!)
docker image prune -a
```

> ⚠️ You can't remove an image that has a running or stopped container using it. Stop and remove the container first.

### Tagging Images

```bash
# tag an image with a new name (useful before pushing to a registry)
docker tag my-pipeline:latest myusername/my-pipeline:v1.0

# general syntax
docker tag <source_image>:<tag> <target_image>:<tag>
```

### Pushing Images

```bash
# login to Docker Hub
docker login

# push your image
docker push myusername/my-pipeline:v1.0
```

### Building Images

```bash
# build from Dockerfile in current directory
docker build -t my-pipeline:latest .

# build from a specific Dockerfile
docker build -f Dockerfile.prod -t my-pipeline:prod .

# build with build arguments
docker build --build-arg ENV=production -t my-pipeline:prod .
```

---

## Image Naming Convention

```
[registry/][username/]repository[:tag]

examples:
  python:3.11-slim                  → official image from Docker Hub
  postgres:16                       → official image, version 16
  myusername/my-kafka-consumer:v2   → your own image on Docker Hub
  ghcr.io/org/image:latest          → GitHub Container Registry
```

If no tag is specified, Docker defaults to `:latest`. **Avoid using `:latest` in production** — it changes and can break things.

---

## Official Images vs Custom Images

| | Official Images | Custom Images |
|---|---|---|
| Source | Docker Hub (verified) | You build them |
| Examples | `postgres`, `kafka`, `python`, `redis` | `my-pipeline`, `my-etl-job` |
| When to use | As base images or for standard services | When you have custom app code |

For data engineering, you'll mostly **pull official images** for infrastructure (Postgres, Kafka, Redis) and **build custom images** for your pipeline code.

---

## Useful Images Every Data Engineer Should Know

| Image | What it's for |
|-------|--------------|
| `postgres:16` | PostgreSQL database |
| `confluentinc/cp-kafka` | Apache Kafka (Confluent) |
| `apache/airflow` | Airflow orchestrator |
| `apache/spark` | Apache Spark |
| `python:3.11-slim` | Python base for custom pipelines |
| `redis:7` | Redis for caching / message queue |
| `dbt-core` | dbt for transformations |
| `bitnami/kafka` | Lightweight Kafka alternative |

---

## What Confused Me

- **`:latest` is not always the latest.** It's just a tag, and whoever published the image decides when to update it. Always pin to a specific version.
- **Image size matters.** A `python:3.11` image is ~900MB. `python:3.11-slim` is ~130MB. For containers you run many times, this adds up. Use slim or alpine variants when you don't need extra tools.
- **`docker image prune` vs `docker image prune -a`** — the first only removes dangling images (untagged, not referenced). The second removes everything not used by a container. Use `-a` carefully.

---

## Resources

- [Docker Hub](https://hub.docker.com/) — browse official images
- [Docker Images Docs](https://docs.docker.com/engine/reference/commandline/image/)
- [Best practices for writing Dockerfiles](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)

---

*Next: [03 — Containers](./03_containers.md)*

*Back to [Docker index](./README.md)*
