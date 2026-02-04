# lite-task-dispatcher

A lightweight distributed task scheduling and processing platform built with Spring Boot 3, PostgreSQL, Redis, and Kafka.

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

- JDK 17+
- Maven 3.8+
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
# Start PostgreSQL, Redis, Kafka
docker-compose up -d
```

### 2. Build Project

```bash
mvn clean package -DskipTests
```

### 3. Run Application

```bash
java -jar task-dispatcher-starter/target/task-dispatcher-starter-1.0.0-SNAPSHOT.jar
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

MIT License
