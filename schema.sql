CREATE TABLE IF NOT EXISTS raw_readings (
  id BIGSERIAL PRIMARY KEY,
  sensor_id        TEXT NOT NULL,
  ts               TIMESTAMPTZ NOT NULL,
  source           TEXT NOT NULL,
  location         TEXT,
  reading_type     TEXT NOT NULL,
  reading_value    DOUBLE PRECISION NOT NULL,
  unit             TEXT,
  file_name        TEXT NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_readings(ts);
CREATE INDEX IF NOT EXISTS idx_raw_sensor_ts ON raw_readings(sensor_id, ts);
CREATE INDEX IF NOT EXISTS idx_raw_type ON raw_readings(reading_type);

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