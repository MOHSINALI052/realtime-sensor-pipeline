-- MySQL schema (deliverable) for MySQL 8.x
-- Mirrors the PostgreSQL tables used by the pipeline
-- Note: Column names differ slightly (count_val) to avoid reserved word conflicts.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS raw_readings (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  sensor_id     VARCHAR(255) NOT NULL,
  ts            TIMESTAMP(6) NOT NULL,
  source        VARCHAR(255) NOT NULL,
  location      VARCHAR(255),
  reading_type  VARCHAR(100) NOT NULL,    -- e.g., temperature, humidity, gas names
  reading_value DOUBLE NOT NULL,
  unit          VARCHAR(32),              -- e.g., C, %, hPa
  file_name     VARCHAR(255) NOT NULL,
  ingested_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_raw_ts (ts),
  KEY idx_raw_sensor_ts (sensor_id, ts),
  KEY idx_raw_type (reading_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS file_aggregates (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  file_name     VARCHAR(255) NOT NULL,
  source        VARCHAR(255) NOT NULL,
  reading_type  VARCHAR(100) NOT NULL,
  count_val     BIGINT NOT NULL,          -- use count_val (avoid COUNT keyword)
  min_value     DOUBLE,
  max_value     DOUBLE,
  avg_value     DOUBLE,
  stddev_value  DOUBLE,
  window_start  TIMESTAMP(6) NULL,
  window_end    TIMESTAMP(6) NULL,
  computed_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_agg_file_type (file_name, reading_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;
