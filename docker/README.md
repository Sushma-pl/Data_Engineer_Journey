# Docker 🐳

Docker is one of the most important tools in a data engineer's toolkit. It lets you package your code and all its dependencies into a container — so it runs the same way everywhere, whether that's your laptop, a teammate's machine, or a cloud server.

I'm learning Docker in the context of building a real streaming data pipeline, so every concept here connects back to practical use cases.

---

## Notes in Order

Start from the top and work your way down.

| # | Topic | What you'll learn |
|---|-------|-------------------|
| 01 | [What is Docker?](./01_what_is_docker.md) | The big picture — what Docker is, why it exists, and why data engineers use it |
| 02 | [Images](./02_images.md) | How images work, layers, pulling, tagging, pushing, and useful images for data engineers |
| 03 | [Containers](./03_containers.md) | Running containers, all essential commands, exec, logs, port mapping, cleanup |
| 04 | [Dockerfile](./04_dockerfile.md) | Writing Dockerfiles, every instruction explained, layer caching, real pipeline example |
| 05 | [Volumes and Networks](./05_volumes_and_networks.md) | Persisting data with volumes, container networking, DNS resolution between services |
| 06 | [Docker Compose](./06_docker_compose.md) | Running multi-service stacks, full streaming pipeline example, dev patterns |

---

## How I'm Using Docker

I'm using Docker to containerize services in a streaming project — things like message brokers, consumers, and producers. As I add more notes, they'll map directly to problems I solved in that project.

---

*Back to [main index](../README.md)*
