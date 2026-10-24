# 04 — Dockerfile

## The Simple Answer

A Dockerfile is a plain text file with step-by-step instructions for building a Docker image. Each instruction adds a layer to the image. You run `docker build` and Docker executes those instructions top to bottom, producing an image you can run anywhere.

---

## Anatomy of a Dockerfile

Here's a real Dockerfile for a Python-based data pipeline:

```dockerfile
# 1. Start from an official base image
FROM python:3.11-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy dependency file first (layer caching trick)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your application code
COPY . .

# 6. Set environment variables
ENV PYTHONUNBUFFERED=1

# 7. Define the command to run when the container starts
CMD ["python", "pipeline.py"]
```

---

## Every Instruction Explained

### `FROM` — Base Image

Every Dockerfile starts with `FROM`. You're building on top of an existing image.

```dockerfile
FROM python:3.11-slim       # slim = smaller, no extras
FROM python:3.11-alpine     # alpine = even smaller, musl libc (can cause issues)
FROM ubuntu:22.04           # full OS (use when you need system tools)
FROM scratch                # empty base (for ultra-minimal images, rare)
```

**Data engineering tip:** use `-slim` variants for Python-based pipelines. They have everything you need and are significantly smaller than the full image.

### `WORKDIR` — Set Working Directory

```dockerfile
WORKDIR /app
```

Sets the directory for all subsequent commands (`RUN`, `COPY`, `CMD`, etc.). Creates it if it doesn't exist. Think of it as `cd /app` but permanent for all following steps.

Always set a `WORKDIR`. Without it, files land in the root `/` which is messy.

### `COPY` — Copy Files from Host to Image

```dockerfile
COPY requirements.txt .          # copy one file to current WORKDIR
COPY src/ ./src/                 # copy a directory
COPY . .                         # copy everything from current directory
```

**The layer caching trick — very important:**

```dockerfile
# ✅ GOOD — dependencies only reinstall when requirements.txt changes
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

# ❌ BAD — dependencies reinstall every time ANY file changes
COPY . .
RUN pip install -r requirements.txt
```

Docker caches each layer. If a layer's input hasn't changed, it reuses the cache. By copying `requirements.txt` first and installing before copying your code, you avoid reinstalling all packages every time you change a single line of code.

### `ADD` — Like COPY but with Extra Powers

```dockerfile
ADD https://example.com/file.tar.gz /tmp/    # can download from URL
ADD archive.tar.gz /app/                     # auto-extracts tar archives
```

**Rule of thumb:** use `COPY` unless you specifically need `ADD`'s URL fetching or auto-extraction.

### `RUN` — Execute Commands During Build

```dockerfile
RUN pip install --no-cache-dir -r requirements.txt

# multiple commands — chain with && to keep it one layer
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*
```

Each `RUN` creates a new layer. Chaining commands with `&&` keeps related steps in one layer and keeps image size down.

**Always clean up in the same `RUN` layer:**
```dockerfile
# ✅ Correct — the cleanup is in the same layer
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# ❌ Wrong — cleanup is a separate layer, size is NOT reduced
RUN apt-get update && apt-get install -y gcc
RUN rm -rf /var/lib/apt/lists/*
```

### `ENV` — Environment Variables

```dockerfile
ENV PYTHONUNBUFFERED=1
ENV APP_ENV=production
ENV DB_HOST=localhost DB_PORT=5432
```

These are available inside the container at runtime. Use for config that doesn't change often. For secrets, **never hardcode them in ENV** — pass them at runtime with `docker run -e` or use Docker secrets / `.env` files.

### `ARG` — Build-Time Variables

```dockerfile
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

ARG ENV=development
RUN echo "Building for $ENV"
```

`ARG` is only available during the build process. Unlike `ENV`, it's not baked into the final image (doesn't appear in `docker inspect`). Use for things like version numbers or build flags.

### `EXPOSE` — Document Which Port the Container Listens On

```dockerfile
EXPOSE 8080
EXPOSE 5432
```

This is **documentation only** — it doesn't actually publish the port. You still need `-p` in `docker run`. It tells other developers (and Docker Compose) what port the app inside uses.

### `CMD` — Default Command to Run

```dockerfile
CMD ["python", "pipeline.py"]
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0"]
```

This is what runs when you do `docker run my-image`. Always use the JSON array format (exec form) — it handles signals correctly (important for graceful shutdowns).

Only one `CMD` is effective — if you have multiple, only the last one runs.

You can override it at runtime:
```bash
docker run my-image python other_script.py
```

### `ENTRYPOINT` — Fixed Command

```dockerfile
ENTRYPOINT ["python"]
CMD ["pipeline.py"]
```

`ENTRYPOINT` sets the fixed executable. `CMD` provides default arguments that can be overridden. Together they work like this:

```bash
docker run my-image                    # runs: python pipeline.py
docker run my-image other_script.py    # runs: python other_script.py
```

**For data pipelines:** use `ENTRYPOINT` when the container has one clear purpose (run this script). Use `CMD` alone when you want flexibility.

### `USER` — Run as Non-Root

```dockerfile
RUN useradd -m appuser
USER appuser
```

By default, containers run as root. This is a security risk. Create a non-root user and switch to it before `CMD`. Many production setups require this.

### `HEALTHCHECK` — Container Health Monitoring

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1
```

Docker periodically runs this command to check if the container is healthy. Docker Compose and orchestrators like Kubernetes use this to decide if a container is ready to receive traffic.

---

## Real-World Example — Data Pipeline Image

```dockerfile
FROM python:3.11-slim

# install system dependencies needed by some Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# layer cache optimization: install deps before copying code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY src/ ./src/
COPY config/ ./config/

# non-root user for security
RUN useradd -m pipelineuser
USER pipelineuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s \
  CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

CMD ["python", "-m", "src.main"]
```

---

## `.dockerignore` — Keep Your Images Clean

Just like `.gitignore`, this file tells Docker what NOT to copy into the image.

```
# .dockerignore
__pycache__/
*.pyc
*.pyo
.git/
.env
.venv/
venv/
*.log
.DS_Store
tests/
docs/
*.md
```

Without this, `COPY . .` copies your entire project including your `.git` folder, virtual environments, and local config. This bloats your image and can leak secrets.

---

## Building and Tagging

```bash
# build with a tag
docker build -t my-pipeline:latest .

# build with version tag
docker build -t my-pipeline:v1.2.0 .

# build for a specific platform (important for Apple Silicon / cloud deployment)
docker build --platform linux/amd64 -t my-pipeline:latest .

# see build output step by step
docker build --progress=plain -t my-pipeline:latest .

# no cache — force rebuild everything
docker build --no-cache -t my-pipeline:latest .
```

---

## What Confused Me

- **`CMD` vs `ENTRYPOINT`** — took a while to click. Simple rule: if your container does one thing, use `ENTRYPOINT`. If you want to be able to swap commands, use `CMD`.
- **Layer order matters enormously.** Always put things that change least at the top (base image, system packages) and things that change most at the bottom (your code). This maximizes cache hits.
- **`RUN` during build vs `CMD` at runtime.** `RUN` executes when you `docker build`. `CMD` executes when you `docker run`. Mixing these up causes hours of confusion.
- **Forgetting `.dockerignore`** — first time I copied my entire repo including `.venv` into an image. Took 10 minutes to build. `.dockerignore` fixed it instantly.

---

## Resources

- [Dockerfile reference](https://docs.docker.com/engine/reference/builder/)
- [Best practices for Dockerfiles](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)
- [Dive](https://github.com/wagoodman/dive) — a tool to inspect image layers and find what's bloating your image

---

*Next: [05 — Volumes and Networks](./05_volumes_and_networks.md)*

*Back to [Docker index](./README.md)*
