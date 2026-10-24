# 01 — What is Docker?

## The Simple Answer

Docker is a tool that lets you package an application and everything it needs to run — code, libraries, config — into a single unit called a **container**. You can then run that container anywhere.

Think of it like this: instead of saying *"it works on my machine"*, you ship the machine itself.

---

## Why Does It Exist?

Before Docker, deploying software was painful. You'd write code on your laptop, and when it ran on a server it would break because:

- The server had a different version of Python
- A library was missing or had a different version
- Environment variables were set differently
- The OS behaved slightly differently

Docker solves this by bundling everything together. The container runs the same way on every machine.

---

## Container vs Virtual Machine

This is a common source of confusion. Both containers and VMs isolate your application, but they do it differently.

```
Virtual Machine                      Container
┌──────────────────┐                ┌──────────────────┐
│   Your App       │                │   Your App       │
│   Libraries      │                │   Libraries      │
│   Guest OS       │  ← heavy!      │                  │
│   Hypervisor     │                │   Docker Engine  │  ← lightweight
│   Host OS        │                │   Host OS        │
│   Hardware       │                │   Hardware       │
└──────────────────┘                └──────────────────┘
```

| | VM | Container |
|---|---|---|
| Includes OS | Yes (full OS) | No (shares host OS kernel) |
| Startup time | Minutes | Seconds |
| Size | GBs | MBs |
| Isolation | Strong | Good enough for most cases |

**For data engineering:** containers are perfect. You can spin up a Kafka broker, a Postgres database, or a Spark worker in seconds, run your pipeline, and tear it all down.

---

## Key Concepts (The Vocabulary)

| Term | What it means |
|------|--------------|
| **Image** | A blueprint for a container. Read-only. Like a recipe. |
| **Container** | A running instance of an image. Like a meal made from the recipe. |
| **Dockerfile** | A text file with instructions to build an image. |
| **Docker Hub** | A registry where images are stored and shared (like GitHub but for images). |
| **Docker Compose** | A tool to define and run multiple containers together using a single file. |

---

## Why Data Engineers Use Docker

As a data engineer, you deal with a lot of moving parts — databases, message brokers, processing engines, orchestrators. Docker lets you:

- **Spin up a full stack locally** — run Kafka + Zookeeper + a consumer all at once with one command
- **Reproduce environments** — your pipeline runs the same in dev, staging, and production
- **Isolate services** — each component runs in its own container, no dependency conflicts
- **Collaborate easily** — share a `docker-compose.yml` and your teammate has the exact same setup in minutes

---

## A Taste of Docker in Action

Pull an official image and run a container:

```bash
# pull the official Python image
docker pull python:3.11-slim

# run a container from that image
docker run -it python:3.11-slim python
```

You now have a Python shell running inside a container. Close it and the container is gone — no cleanup needed.

---

## What Confused Me

- **Image vs Container** took a moment to click. Think of an image as a class and a container as an instance of that class.
- Docker doesn't replace your code — it wraps it. Your Python script is still just a Python script inside the container.
- The first time you run `docker pull`, it feels slow. That's just downloading layers — it's cached after that.

---

## Resources

- [Docker Official Docs — Get Started](https://docs.docker.com/get-started/)
- [Play with Docker](https://labs.play-with-docker.com/) — free browser-based Docker playground, no install needed

---

*Next: [02 — Dockerfile basics](./02_dockerfile_basics.md) (coming soon)*

*Back to [Docker index](./README.md)*
