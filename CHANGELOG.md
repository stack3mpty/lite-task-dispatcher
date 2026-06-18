# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-02-07

### Added
- P1 reliability and observability features
- Outbox pattern for reliable event publishing
- Structured logging with traceId propagation
- Prometheus metrics endpoint
- HA failover support (verified via load test)

### Fixed
- Task state transition bug (`PENDING -> RUNNING` race condition)
- Data consistency issues in concurrent execution

### Verified
- Load test: 5000 tasks @ 200 RPS, dual-node, zero task loss
- HA test: single node failure with zero impact on remaining node

## [0.1.0] - 2026-01-10

### Added
- Producer-Dispatcher-Executor architecture
- Multi-priority Redis task queue (P0-P4)
- Delayed task execution via Redis ZSet
- Token bucket rate limiting (Redis + Lua)
- Distributed lock via Redisson
- Task deduplication (MD5 parameter hashing)
- Exponential backoff retry with jitter
- Kafka event publishing
- Strategy pattern executors (HTTP_CALLBACK, LOG)
- REST API with SpringDoc OpenAPI
- PostgreSQL persistence with JSONB support
- Docker Compose infrastructure setup
