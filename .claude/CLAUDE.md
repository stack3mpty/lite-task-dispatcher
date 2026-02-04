# CLAUDE.md - Project Memory

## Project Overview

**Project Name**: lite-task-dispatcher
**Project Type**: Distributed Task Scheduling and Processing Platform
**Created**: 2026-02-04
**Author**: Java Developer (2.5 years experience, background at Ctrip Octopus Core Server)

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Java | 21 | Language |
| Spring Boot | 3.2.2 | Framework |
| PostgreSQL | 15 | Database |
| Redis | 7 | Task Queue / Cache / Lock |
| Kafka | 3.x | Event Messaging |
| Redisson | 3.25.2 | Distributed Lock |
| Spring Data JPA | 3.x | ORM |

## Architecture

```
Producer (任务生产者) -> Dispatcher (任务分发器) -> Executor (任务执行器)
     |                        |                        |
     v                        v                        v
   Redis                 PostgreSQL                 Kafka
 (Task Queue)           (Persistence)             (Events)
```

## Module Structure

```
lite-task-dispatcher/
├── task-dispatcher-common/          # Enums, Exceptions, Utils
├── task-dispatcher-domain/          # Entities, Events, Value Objects
├── task-dispatcher-infrastructure/  # Redis, Kafka, JPA implementations
├── task-dispatcher-core/            # Producer, Dispatcher, Executor
├── task-dispatcher-api/             # REST Controllers, DTOs
├── task-dispatcher-starter/         # Spring Boot Application
├── docker-compose.yml               # Infrastructure
├── init.sql                         # Database init script
└── README.md                        # Documentation
```

## Key Technical Highlights

### 1. Redis Task Queue
- Multi-priority queues (P0-P4) using Redis List
- Key pattern: `task:queue:p{0-4}`
- Operations: LPUSH (enqueue), RPOP (dequeue)

### 2. Delay Queue
- Redis ZSet with score = execution timestamp
- Key: `task:delay:queue`
- Polling for ready tasks

### 3. Token Bucket Rate Limiting
- Redis + Lua atomic script
- File: `RateLimiter.java`
- Key pattern: `task:rate:limit:{taskType}`

### 4. Distributed Lock
- Redisson-based implementation
- File: `DistributedLock.java`
- Key pattern: `task:lock:{key}`

### 5. Task Deduplication
- MD5 hash of task parameters
- File: `DeduplicationChecker.java`
- Key pattern: `task:dedup:{taskType}:{md5}`

### 6. Strategy Pattern Executors
- Interface: `TaskExecutor.java`
- Abstract base: `AbstractTaskExecutor.java` (Template Method)
- Built-in: `HttpCallbackExecutor.java`

### 7. Exponential Backoff Retry
- File: `ExponentialBackoffRetry.java`
- Formula: `delay = min(initialDelay * (multiplier ^ attempt) + jitter, maxDelay)`

### 8. Snowflake ID Generator
- File: `IdGenerator.java`
- 64-bit: 1(sign) + 41(timestamp) + 10(worker) + 12(sequence)

## Database Schema

### Tables
1. `task_definition` - Task type definitions
2. `task_instance` - Task execution instances
3. `task_execution_log` - Execution logs

### Key Indexes
- `idx_task_instance_status_priority` - For task polling
- `idx_task_instance_execute_at` - For delay queue

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/tasks` | Submit task |
| GET | `/api/v1/tasks/{taskId}` | Get task |
| GET | `/api/v1/tasks` | List tasks |
| PUT | `/api/v1/tasks/{taskId}/cancel` | Cancel task |
| PUT | `/api/v1/tasks/{taskId}/retry` | Retry task |

## Quick Commands

```bash
# Start infrastructure
docker-compose up -d

# Build project
mvn clean package -DskipTests

# Run application
java -jar task-dispatcher-starter/target/task-dispatcher-starter-1.0.0-SNAPSHOT.jar

# Access Swagger UI
open http://localhost:8080/swagger-ui.html
```

## Related Company Project Reference

This project is inspired by **Ctrip Octopus Core Server**:
- Builder-Crawler-Writer architecture → Producer-Dispatcher-Executor
- QMQ → Kafka
- MySQL sharding → PostgreSQL (single instance for simplicity)
- Redis clusters → Redis (single instance)

## TODO / Future Enhancements

- [ ] Add unit tests (target: >80% coverage)
- [ ] Add integration tests with Testcontainers
- [ ] Implement EmailExecutor
- [ ] Implement DataSyncExecutor
- [ ] Add Prometheus metrics
- [ ] Implement DelayTaskScheduler (background polling)
- [ ] Implement TimeoutTaskScanner
- [ ] Add graceful shutdown handling
- [ ] Create SDK module for client integration

## File Quick Reference

| Feature | File Path |
|---------|-----------|
| Rate Limiter | `infrastructure/redis/RateLimiter.java` |
| Distributed Lock | `infrastructure/redis/DistributedLock.java` |
| Task Queue | `infrastructure/redis/TaskQueueOperator.java` |
| Delay Queue | `infrastructure/redis/DelayQueueOperator.java` |
| Deduplication | `infrastructure/redis/DeduplicationChecker.java` |
| Task Executor Interface | `core/executor/TaskExecutor.java` |
| Abstract Executor | `core/executor/AbstractTaskExecutor.java` |
| Retry Policy | `core/executor/retry/ExponentialBackoffRetry.java` |
| Task Producer | `core/producer/TaskProducer.java` |
| Task Dispatcher | `core/dispatcher/TaskDispatcher.java` |
| ID Generator | `common/util/IdGenerator.java` |
| Task Entity | `domain/task/entity/TaskInstance.java` |

## Session Notes

- Project created as a mini version of Ctrip's Octopus system
- Designed to showcase enterprise-level skills for resume/portfolio
- Core focus: high concurrency, high availability, extensibility
- All code is in English with Chinese comments where needed
