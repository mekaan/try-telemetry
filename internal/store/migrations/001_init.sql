-- History: every distinct event we have accepted. Append-only.
-- Dedup key is (charger_id, event_id): ids only have to be unique per charger,
-- so two producers can't swallow each other's events.
-- In production this table is partitioned by day on occurred_at, which means
-- occurred_at joins the primary key. That still dedupes, because a retry
-- carries the same occurred_at.
-- COLLATE "C" makes the event_id tie-break a byte comparison, independent of
-- the database locale.
CREATE TABLE IF NOT EXISTS telemetry_events (
    charger_id     text        NOT NULL,
    event_id       text        COLLATE "C" NOT NULL,
    connector_id   integer     NOT NULL,
    event_type     text        NOT NULL,
    schema_version integer     NOT NULL,
    occurred_at    timestamptz NOT NULL,  -- charger clock
    received_at    timestamptz NOT NULL DEFAULT now(),
    payload        jsonb       NOT NULL,
    PRIMARY KEY (charger_id, event_id)
);

CREATE INDEX IF NOT EXISTS telemetry_events_charger_time
    ON telemetry_events (charger_id, occurred_at DESC);

-- Latest state: one row per charger, connector and event type.
-- Keyed per event type so a late meter reading cannot be blocked by a newer
-- status change (and the reverse), and so a new event type needs no migration.
CREATE TABLE IF NOT EXISTS latest_state (
    charger_id     text        NOT NULL,
    connector_id   integer     NOT NULL,
    event_type     text        NOT NULL,
    schema_version integer     NOT NULL,
    occurred_at    timestamptz NOT NULL,
    event_id       text        COLLATE "C" NOT NULL,
    payload        jsonb       NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (charger_id, connector_id, event_type)
);
