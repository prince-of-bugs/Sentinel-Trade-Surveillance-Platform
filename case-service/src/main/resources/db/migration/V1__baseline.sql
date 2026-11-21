-- Sentinel case-service baseline schema.
-- Never edit a migration after it is merged: add V2__..., V3__... instead.

CREATE TABLE alert (
    id              VARCHAR(64)  PRIMARY KEY,
    rule_type       VARCHAR(32)  NOT NULL,
    severity        VARCHAR(16)  NOT NULL,
    account_id      VARCHAR(64)  NOT NULL,
    symbol          VARCHAR(16)  NOT NULL,
    summary         TEXT         NOT NULL,
    risk_score      SMALLINT     CHECK (risk_score BETWEEN 0 AND 100),
    status          VARCHAR(16)  NOT NULL DEFAULT 'OPEN',
    evidence        JSONB        NOT NULL,
    raised_at       TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- The analyst queue: open alerts, newest first.
CREATE INDEX idx_alert_status_raised ON alert (status, raised_at DESC);
CREATE INDEX idx_alert_account ON alert (account_id);

CREATE TABLE investigation_case (
    id              BIGSERIAL    PRIMARY KEY,
    alert_id        VARCHAR(64)  NOT NULL UNIQUE REFERENCES alert (id),
    assignee        VARCHAR(64),
    status          VARCHAR(16)  NOT NULL DEFAULT 'OPEN',
    disposition     VARCHAR(32),
    ai_summary      TEXT,
    opened_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    -- Optimistic locking: two analysts cannot silently overwrite each other.
    version         BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX idx_case_assignee_status ON investigation_case (assignee, status);

CREATE TABLE case_note (
    id              BIGSERIAL    PRIMARY KEY,
    case_id         BIGINT       NOT NULL REFERENCES investigation_case (id),
    author          VARCHAR(64)  NOT NULL,
    body            TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_note_case ON case_note (case_id);

CREATE TABLE audit_event (
    id              BIGSERIAL    PRIMARY KEY,
    actor           VARCHAR(64)  NOT NULL,
    action          VARCHAR(32)  NOT NULL,
    entity          VARCHAR(32)  NOT NULL,
    entity_id       VARCHAR(64)  NOT NULL,
    before_state    JSONB,
    after_state     JSONB,
    at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_entity ON audit_event (entity, entity_id);

-- Idempotent consumer: insert the Kafka eventId here in the same transaction as the alert.
-- INSERT ... ON CONFLICT DO NOTHING returning 0 rows means "already processed, skip".
CREATE TABLE processed_event (
    event_id        VARCHAR(64)  PRIMARY KEY,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE rule_config (
    rule_type       VARCHAR(32)  PRIMARY KEY,
    thresholds      JSONB        NOT NULL,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Starting thresholds from the project plan. Tuned later from the admin page.
INSERT INTO rule_config (rule_type, thresholds) VALUES
    ('EXCESSIVE_CANCELS', '{"windowSeconds": 900, "minOrders": 20, "maxCancelToFillRatio": 10}'),
    ('SPOOFING',          '{"windowSeconds": 60, "sizeMultipleOfAverage": 5, "cancelWithinSeconds": 10}'),
    ('LAYERING',          '{"windowSeconds": 30, "minPriceLevels": 4, "minCancelPercent": 75}'),
    ('WASH_TRADING',      '{"windowSeconds": 300, "quantityTolerancePercent": 2, "priceTolerancePercent": 0.1}');
