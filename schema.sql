-- PostgreSQL schema (used by the running pipeline)

CREATE TABLE IF NOT EXISTS raw_readings (
  id BIGSERIAL PRIMARY KEY,
  sensor_id        TEXT NOT NULL,
  ts               TIMESTAMPTZ NOT NULL,
  source           TEXT NOT NULL,
  location         TEXT,
  reading_type     TEXT NOT NULL,     -- e.g., temperature, humidity, pressure
  reading_value    DOUBLE PRECISION NOT NULL,
  unit             TEXT,              -- e.g., C, %, hPa
  file_name        TEXT NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_ts         ON raw_readings(ts);
CREATE INDEX IF NOT EXISTS idx_raw_sensor_ts  ON raw_readings(sensor_id, ts);
CREATE INDEX IF NOT EXISTS idx_raw_type       ON raw_readings(reading_type);

CREATE TABLE IF NOT EXISTS file_aggregates (
  id BIGSERIAL PRIMARY KEY,
  file_name       TEXT NOT NULL,
  source          TEXT NOT NULL,
  reading_type    TEXT NOT NULL,
  count           BIGINT NOT NULL,
  min_value       DOUBLE PRECISION,
  max_value       DOUBLE PRECISION,
  avg_value       DOUBLE PRECISION,
  stddev_value    DOUBLE PRECISION,
  window_start    TIMESTAMPTZ,
  window_end      TIMESTAMPTZ,
  computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agg_file_type ON file_aggregates(file_name, reading_type);
-- Idempotency for raw_readings
ALTER TABLE raw_readings
  ADD COLUMN IF NOT EXISTS dedupe_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_dedupe
  ON raw_readings(dedupe_key);

-- Upsert key for aggregates (per file + type)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_file_agg'
  ) THEN
    ALTER TABLE file_aggregates
      ADD CONSTRAINT uq_file_agg UNIQUE (file_name, reading_type);
  END IF;
END$$;

