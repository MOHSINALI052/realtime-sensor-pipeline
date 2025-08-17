# Scaling the Real-Time Sensor Pipeline

This document explains how to evolve the single-process, folder-polling prototype into a **horizontally scalable**, **fault-tolerant**, and **cost-efficient** platform.

---

## 1) High-Level Reference Architectures

### A. Cloud-Native (serverless-ish, S3-centric, AWS example)
- **Ingress**: Sensors → S3 (object storage). Optional edge buffer via API Gateway + Lambda, or direct S3 PUT with signed URLs.
- **Eventing**: S3 → Event notifications → SQS (queue) / SNS.
- **Workers**: Lambda or Fargate/ECS/EKS consumers that pull SQS messages (one per file), read the S3 object, run validation + transform.
- **Storage**:
  - **Raw & curated**: S3 in **Parquet** (partitioned by `ingest_date=YYYY-MM-DD/sensor_type=...`).
  - **Hot analytics**: PostgreSQL/TimescaleDB or Amazon Aurora Postgres for **aggregates, dashboards, APIs**.
  - **Warehouse**: Redshift or Athena-on-S3 for BI & long term.
- **DLQ**: SQS Dead-Letter Queue + S3 `quarantine/` bucket for bad files + error manifests.
- **Observability**: CloudWatch metrics/logs, X-Ray traces, alarms.

### B. Kafka + Spark/Flink (stream-native, on k8s or managed)
- **Ingress**: Files land in object storage (S3/GCS/ADLS). A small dispatcher publishes a Kafka message per file (path + metadata).
- **Processing**: Spark Structured Streaming or Flink jobs read the file path, load data, validate/normalize, write:
  - **Bronze** (raw) → S3 (Parquet)
  - **Silver** (clean) → S3 (Parquet)
  - **Gold** (aggregates) → Postgres/warehouse
- **Exactly-once**: Use transactional sinks (Delta/Iceberg/Hudi) for lake tables + idempotent `UPSERT` into Postgres.
- **Scale**: Horizontal executors; autoscaling by lag.

### C. GCP analog
- **Ingress**: GCS
- **Eventing**: Pub/Sub
- **Processing**: Dataflow (Apache Beam) or Cloud Run batch workers
- **Store**: BigQuery (analytics) + Cloud SQL (Postgres) for hot aggregates.

> Keep the pipeline **stateless per worker**; coordination via queue offsets + transactional writes.

---

## 2) Ingestion & File Fan-out

- **Replace polling**: use **object-store events → queue** (S3→SQS or GCS→Pub/Sub).
- **One message per object**: payload includes `{bucket, key, size, md5, source, received_at}`.
- **Backpressure**: the queue depth becomes your throttle; scale consumer count based on lag.

**Local/dev**: if you must stay on filesystems, switch from polling to **watchdog/inotify** for lower latency; still publish a message to a local queue (e.g., Redis, RabbitMQ) to simulate production flow.

---

## 3) Data Model & Idempotency

- **Natural id (dedupe key)**: `hash(sensor_id, ts, reading_type, file_name)` (or SHA-256 over the concatenation).
- **Postgres**:
  ```sql
  -- Add this to schema for exactly-once inserts:
  ALTER TABLE raw_readings ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
  CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_dedupe ON raw_readings(dedupe_key);

  -- INSERT ... ON CONFLICT DO NOTHING (or UPDATE if needed)

