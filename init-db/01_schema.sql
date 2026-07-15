-- Feature store schema for the accident-severity-prediction backend.
-- Executed automatically by the postgres container on first startup
-- (mounted into /docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS aggregated_features (
    id                     SERIAL PRIMARY KEY,
    "State"                VARCHAR(8)       NOT NULL,
    time_bucket            VARCHAR(16)      NOT NULL,
    is_weekend             BOOLEAN          NOT NULL,
    accident_count         BIGINT           NOT NULL,
    avg_severity           DOUBLE PRECISION,
    junction_rate          DOUBLE PRECISION,
    crossing_rate          DOUBLE PRECISION,
    traffic_signal_rate    DOUBLE PRECISION,
    run_timestamp          TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_aggregated_features_state ON aggregated_features ("State");
CREATE INDEX IF NOT EXISTS idx_aggregated_features_run ON aggregated_features (run_timestamp);

-- Principle of least privilege: a read-only role for the future delivery
-- API (FastAPI service), so the data-serving layer can never modify the
-- training feature set. Planned to be wired up in the Finalization phase.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'accidents_readonly') THEN
        CREATE ROLE accidents_readonly LOGIN PASSWORD 'changeme_readonly';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE accidents TO accidents_readonly;
GRANT SELECT ON aggregated_features TO accidents_readonly;
