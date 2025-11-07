CREATE DATABASE txoutbox;

\c txoutbox

CREATE TABLE txoutbox
(
    id            BIGSERIAL PRIMARY KEY,
    topic         TEXT        NOT NULL,
    key           TEXT,
    payload       JSONB       NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    retry_count   INT         NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by    TEXT,
    claimed_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at       TIMESTAMPTZ
);
