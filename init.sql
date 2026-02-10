-- Task Definition Table
CREATE TABLE IF NOT EXISTS task_definition (
    id BIGSERIAL PRIMARY KEY,
    task_type VARCHAR(64) NOT NULL UNIQUE,
    task_name VARCHAR(128) NOT NULL,
    executor_type VARCHAR(64) NOT NULL,
    executor_config JSONB DEFAULT '{}',
    retry_policy JSONB DEFAULT '{"maxAttempts": 3, "initialDelay": 1000, "multiplier": 2.0, "maxDelay": 60000}',
    timeout_seconds INT DEFAULT 60,
    rate_limit INT DEFAULT 100,
    description TEXT,
    status VARCHAR(16) DEFAULT 'ENABLED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Task Instance Table
CREATE TABLE IF NOT EXISTS task_instance (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(32) NOT NULL UNIQUE,
    task_def_id BIGINT NOT NULL REFERENCES task_definition(id),
    task_type VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'CREATED',
    priority INT NOT NULL DEFAULT 2,
    params JSONB DEFAULT '{}',
    result JSONB,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    max_retry INT DEFAULT 3,
    execute_at TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    callback_url VARCHAR(512),
    created_by VARCHAR(64),
    executor_id VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Task Execution Log Table
CREATE TABLE IF NOT EXISTS task_execution_log (
    id BIGSERIAL PRIMARY KEY,
    task_instance_id BIGINT NOT NULL REFERENCES task_instance(id),
    task_id VARCHAR(32) NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(16) NOT NULL,
    message TEXT,
    error_detail TEXT,
    duration_ms BIGINT,
    executor_ip VARCHAR(45),
    executor_id VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Task Outbox Event Table (reliable dispatch)
CREATE TABLE IF NOT EXISTS task_outbox_event (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    task_id VARCHAR(32) NOT NULL UNIQUE,
    payload JSONB DEFAULT '{}',
    status VARCHAR(16) NOT NULL DEFAULT 'NEW',
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_task_def_type ON task_definition(task_type);
CREATE INDEX IF NOT EXISTS idx_task_def_status ON task_definition(status);

CREATE INDEX IF NOT EXISTS idx_task_instance_task_id ON task_instance(task_id);
CREATE INDEX IF NOT EXISTS idx_task_instance_status ON task_instance(status);
CREATE INDEX IF NOT EXISTS idx_task_instance_priority ON task_instance(priority);
CREATE INDEX IF NOT EXISTS idx_task_instance_execute_at ON task_instance(execute_at);
CREATE INDEX IF NOT EXISTS idx_task_instance_created_at ON task_instance(created_at);
CREATE INDEX IF NOT EXISTS idx_task_instance_status_priority ON task_instance(status, priority);
CREATE INDEX IF NOT EXISTS idx_task_instance_def_status ON task_instance(task_def_id, status);

CREATE INDEX IF NOT EXISTS idx_task_log_instance_id ON task_execution_log(task_instance_id);
CREATE INDEX IF NOT EXISTS idx_task_log_created_at ON task_execution_log(created_at);
CREATE INDEX IF NOT EXISTS idx_task_outbox_status_next_retry ON task_outbox_event(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_task_outbox_task_id ON task_outbox_event(task_id);

-- Insert sample task definitions
INSERT INTO task_definition (task_type, task_name, executor_type, description) VALUES
    ('HTTP_CALLBACK', 'HTTP Callback Task', 'HTTP_CALLBACK', 'Execute HTTP callback to external systems'),
    ('EMAIL_SEND', 'Email Send Task', 'EMAIL', 'Send emails'),
    ('DATA_SYNC', 'Data Sync Task', 'DATA_SYNC', 'Synchronize data between systems')
ON CONFLICT (task_type) DO NOTHING;

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_task_definition_updated_at ON task_definition;
CREATE TRIGGER update_task_definition_updated_at
    BEFORE UPDATE ON task_definition
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_task_instance_updated_at ON task_instance;
CREATE TRIGGER update_task_instance_updated_at
    BEFORE UPDATE ON task_instance
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_task_outbox_event_updated_at ON task_outbox_event;
CREATE TRIGGER update_task_outbox_event_updated_at
    BEFORE UPDATE ON task_outbox_event
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
