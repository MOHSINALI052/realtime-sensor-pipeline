# Real-Time Sensor Pipeline — Architecture

## Overview
This project continuously watches a local folder for CSV files, validates/transforms sensor readings, and stores both raw rows and per-file aggregates into PostgreSQL. Invalid files are quarantined with error details.

## Components
- **Directory Poller (`src/main.py`)**  
  Polls `data/incoming/` every `POLL_INTERVAL_SECONDS` (default 5s). For each new CSV:
  1) Load file → 2) Validate/transform → 3) Insert to DB → 4) Route to `processed/` or `quarantine/`.

- **Processor (`src/processor.py`)**  
  - `load_csv_airquality`: reads AirQuality CSV (sep `;`, decimal `,`), drops `Unnamed*`, converts sentinel `-200` → NaN.
  - `build_timestamp`: merges `Date + Time` → UTC (explicit format `%d/%m/%Y %H.%M.%S`).
  - `validate_transform`: ensures key fields (`sensor_id`, `ts`, `reading_value`) exist, normalizes to **long** form, maps `T`→`temperature (C)` and `RH`→`humidity (%)` with range checks; lets gas sensors pass-through.
  - `compute_aggregates`: per-file stats (count, min, max, mean, std).
  - `to_raw_rows`: converts DF → list[dict] for DB inserts.

- **DB Layer (`src/db.py`)**  
  - SQLAlchemy engine with pooling and `pool_pre_ping`.
  - `insert_raw`, `insert_aggregates` wrapped with **tenacity** (exponential backoff retries) for transient DB failures.

- **Config (`src/config.py`)**  
  Loads `.env` (runtime values) with safe defaults.

- **Utilities (`src/utils.py`)**  
  Safe file move (no overwrite; creates folders). Routes files to `processed/`/`quarantine/`.

- **Quarantine & Error Artifacts**  
  - Validation errors → entire file moved to `quarantine/` and an `__errors.csv` is written with `error_reason`.
  - Fatal (unexpected) exceptions → file moved to `quarantine/` with `__fatal.txt` explaining the error.

## Data Model (PostgreSQL)
**Tables** (see `schema.sql`):
- `raw_readings`  
  `(id, sensor_id, ts, source, location, reading_type, reading_value, unit, file_name, ingested_at)`  
  **Indexes**: `ts`, `(sensor_id, ts)`, `reading_type`.

- `file_aggregates`  
  `(id, file_name, source, reading_type, count, min_value, max_value, avg_value, stddev_value, window_start, window_end, computed_at)`  
  **Indexes**: `(file_name, reading_type)`.

> Optional deliverable: `schema.mysql.sql` mirrors the tables for MySQL 8.x (repo artifact only).

## Processing Flow (Mermaid)
```mermaid
flowchart LR
    W[Directory Poller] --> L[Load CSV]
    L --> V[Validate & Transform]
    V -->|invalid rows exist| Q[Move file to quarantine + write __errors.csv]
    V -->|valid only| I[Insert raw rows + aggregates]
    I --> P[Move file to processed]
    V -->|unexpected error| F[Move file to quarantine + __fatal.txt]
