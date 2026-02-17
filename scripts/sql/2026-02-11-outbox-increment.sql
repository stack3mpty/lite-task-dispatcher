-- Incremental migration for cf57ef4: task_outbox_event table and related objects
-- Safe to run multiple times.

BEGIN;

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

ALTER TABLE task_outbox_event
    ADD COLUMN IF NOT EXISTS event_type VARCHAR(64),
    ADD COLUMN IF NOT EXISTS task_id VARCHAR(32),
    ADD COLUMN IF NOT EXISTS payload JSONB DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS retry_count INT,
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE task_outbox_event
    ALTER COLUMN event_type SET NOT NULL,
    ALTER COLUMN task_id SET NOT NULL,
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN retry_count SET NOT NULL;

ALTER TABLE task_outbox_event
    ALTER COLUMN payload SET DEFAULT '{}',
    ALTER COLUMN status SET DEFAULT 'NEW',
    ALTER COLUMN retry_count SET DEFAULT 0,
    ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

UPDATE task_outbox_event
SET status = 'NEW'
WHERE status IS NULL;

UPDATE task_outbox_event
SET retry_count = 0
WHERE retry_count IS NULL;

UPDATE task_outbox_event
SET created_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;

UPDATE task_outbox_event
SET updated_at = CURRENT_TIMESTAMP
WHERE updated_at IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uk_task_outbox_event_task_id'
          AND conrelid = 'task_outbox_event'::regclass
    ) THEN
        ALTER TABLE task_outbox_event
            ADD CONSTRAINT uk_task_outbox_event_task_id UNIQUE (task_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_task_outbox_status_next_retry
    ON task_outbox_event(status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_task_outbox_task_id
    ON task_outbox_event(task_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_task_outbox_event_updated_at ON task_outbox_event;
CREATE TRIGGER update_task_outbox_event_updated_at
    BEFORE UPDATE ON task_outbox_event
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMIT;
