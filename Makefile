.PHONY: infra build run test clean all stop

# Start infrastructure (PostgreSQL, Redis, Kafka)
infra:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@echo "Infrastructure ready."

# Stop infrastructure
stop:
	docker-compose down

# Build the project (skip tests for speed)
build:
	mvn clean package -DskipTests -q

# Run the application
run:
	java -jar task-dispatcher-starter/target/task-dispatcher-starter-1.0.0-SNAPSHOT.jar

# Run tests
test:
	mvn verify

# Full workflow: infra + build + run
all: infra build run

# Clean build artifacts
clean:
	mvn clean
