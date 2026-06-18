# lite-task-dispatcher

[![CI](https://github.com/stack3mpty/lite-task-dispatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/stack3mpty/lite-task-dispatcher/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java 21](https://img.shields.io/badge/Java-21-blue.svg)](https://openjdk.org/projects/jdk/21/)

A lightweight distributed task scheduling and execution engine. No ZooKeeper, no dedicated admin node, no heavy cluster protocol. Just Redis + PostgreSQL + Kafka, production-ready out of the box.

**Why another task dispatcher?**

| | XXL-Job | PowerJob | lite-task-dispatcher |
|---|---------|----------|---------------------|
| Admin node | Required | Required (Akka cluster) | None |
| Coordination | DB polling | Akka | Redis + distributed lock |
| Priority queue | No | No | P0-P4, 5 levels |
| Rate limiting | No | No | Token bucket (Lua) |
| Deduplication | No | No | MD5 parameter hashing |
| Infra dependencies | MySQL + Admin App | MySQL + Worker + Server | Redis + PostgreSQL |

Best suited for: mid-scale systems that need reliable task dispatch without operational overhead of a dedicated scheduler cluster.

### Benchmark (dual-node, 2026-02-07)

```
5000 tasks @ 200 RPS
Submit latency: p50=5ms, p95=22ms, p99=44ms
Execution: 100% success, zero pending stuck
HA: single node down mid-test, zero task loss
```

## Features

- **Producer-Dispatcher-Executor Architecture**: Clean separation of concerns for task lifecycle management
- **Multi-Priority Task Queue**: Redis-based task queues with P0-P4 priority levels
- **Delayed Task Execution**: Redis ZSet-based delay queue for scheduled tasks
- **Token Bucket Rate Limiting**: Redis + Lua atomic rate limiting
- **Distributed Lock**: Redisson-based distributed lock for task execution
- **Task Deduplication**: MD5-based parameter hashing for duplicate detection
- **Exponential Backoff Retry**: Configurable retry with jitter to prevent thundering herd
- **Event-Driven Architecture**: Kafka for async event publishing
- **Strategy Pattern Executors**: SPI-based extensible task executors

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Java 21 |
| Framework | Spring Boot 3.2 |
| Database | PostgreSQL 15 |
| Cache/Queue | Redis 7 |
| Message Queue | Apache Kafka |
| ORM | Spring Data JPA |
| Distributed Lock | Redisson |
| API Docs | SpringDoc OpenAPI |
| Build Tool | Maven |

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│   Producer   │────>│   Dispatcher    │────>│   Executor   │
│  (任务生产者) │     │   (任务分发器)   │     │  (任务执行器) │
└──────────────┘     └─────────────────┘     └──────────────┘
       │                     │                      │
       ▼                     ▼                      ▼
┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│    Redis     │     │   PostgreSQL    │     │    Kafka     │
│  Task Queue  │     │   Persistence   │     │   Events     │
└──────────────┘     └─────────────────┘     └──────────────┘
```

## Quick Start

### Prerequisites

- JDK 21+
- Maven 3.8+
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
make infra
# or: docker-compose up -d
```

### 2. Build Project

```bash
make build
# or: mvn clean package -DskipTests
```

### 3. Run Application

```bash
make run
# or: java -jar task-dispatcher-starter/target/task-dispatcher-starter-1.0.0-SNAPSHOT.jar
```

### 4. Access API

- Swagger UI: http://localhost:8080/swagger-ui.html
- API Docs: http://localhost:8080/api-docs

## API Examples

### Submit Task

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "taskType": "HTTP_CALLBACK",
    "priority": 2,
    "params": {
      "url": "https://httpbin.org/post",
      "method": "POST",
      "body": {"message": "Hello World"}
    }
  }'
```

### Query Task

```bash
curl http://localhost:8080/api/v1/tasks/{taskId}
```

### Cancel Task

```bash
curl -X PUT http://localhost:8080/api/v1/tasks/{taskId}/cancel
```

## Project Structure

```
lite-task-dispatcher/
├── task-dispatcher-common/          # Common utilities, enums, exceptions
├── task-dispatcher-domain/          # Domain entities, events, value objects
├── task-dispatcher-infrastructure/  # Redis, Kafka, JPA implementations
├── task-dispatcher-core/            # Core business logic
├── task-dispatcher-api/             # REST controllers, DTOs
├── task-dispatcher-starter/         # Spring Boot application
├── docker-compose.yml               # Infrastructure setup
└── init.sql                         # Database initialization
```

## Key Technical Highlights

### 1. Token Bucket Rate Limiting (Redis + Lua)

Atomic rate limiting using Lua script for thread-safe operations.

### 2. Exponential Backoff Retry

```java
delay = min(initialDelay * (multiplier ^ attempt) + jitter, maxDelay)
```

### 3. Strategy Pattern Executors

Implement `TaskExecutor` interface to add new task types:

```java
@Component
public class CustomExecutor extends AbstractTaskExecutor {
    @Override
    public String getType() { return "CUSTOM"; }

    @Override
    protected TaskResult doExecute(TaskContext context) {
        // Your logic here
        return TaskResult.success();
    }
}
```

### 4. Multi-Priority Queue

Tasks are dispatched based on priority (P0 > P1 > P2 > P3 > P4).

## Configuration

See `application.yml` for full configuration options:

```yaml
task:
  dispatcher:
    executor-id: ${HOSTNAME:localhost}
    poll-interval-ms: 1000
    batch-size: 10
```

## License

MIT License - see [LICENSE](LICENSE) for details.
