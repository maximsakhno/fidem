CREATE TYPE fidem.outbox_status AS ENUM ('pending', 'done', 'failed');

CREATE TABLE fidem.outbox (
    id text PRIMARY KEY,
    event_id text NOT NULL,
    source_message_id text NOT NULL,
    handler text NOT NULL,
    event_name text NOT NULL,
    event_version integer NOT NULL,
    payload jsonb NOT NULL,
    status fidem.outbox_status NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);

CREATE INDEX outbox_pending ON fidem.outbox (available_at) WHERE status = 'pending';
