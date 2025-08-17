# src/main.py
import argparse
import logging
import time
from pathlib import Path

from src.config import Config
from src.db import make_engine, insert_raw, insert_aggregates
from src.processor import (
    load_csv_airquality,
    validate_transform,
    compute_aggregates,
    to_raw_rows,
)
from src.utils import move_file

# === Prometheus metrics (Step 17B) ===
from prometheus_client import Counter, start_http_server

FILES_PROCESSED = Counter("files_processed_total", "Files successfully processed")
FILES_QUARANTINED = Counter("files_quarantined_total", "Files moved to quarantine")
RAW_ROWS_INSERTED = Counter("raw_rows_inserted_total", "Raw rows inserted into DB")
AGG_ROWS_INSERTED = Counter("agg_rows_inserted_total", "Aggregate rows inserted/updated")
PROCESS_ERRORS = Counter("process_errors_total", "Unhandled processing errors")


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def process_file(fp: Path, cfg: Config, data_dir: Path, engine):
    """
    Load -> validate/transform -> write to DB -> move to processed/
    On validation failure: move to quarantine/ and write __errors.csv
    On unexpected error:   move to quarantine/ and write __fatal.txt
    """
    try:
        logging.info(f"Processing {fp.name}")
        df = load_csv_airquality(str(fp))

        valid, invalid = validate_transform(df, cfg, fp.name)

        # Any invalid rows? quarantine the whole file + log details
        if not invalid.empty:
            qdir = data_dir / "quarantine"
            qpath = move_file(fp, qdir)
            invalid_path = qpath.with_suffix("").with_name(qpath.stem + "__errors.csv")
            invalid.to_csv(invalid_path, index=False)
            FILES_QUARANTINED.inc()
            logging.warning(
                f"Validation failed for some rows. File moved to quarantine: {qpath.name}"
            )
            return

        # Build payloads
        raw_rows = to_raw_rows(valid, fp.name, cfg.source_name)
        aggs = compute_aggregates(valid, fp.name, cfg.source_name)

        # Write to DB (insert_* have retry/backoff)
        insert_raw(engine, raw_rows)
        insert_aggregates(engine, aggs)

        RAW_ROWS_INSERTED.inc(len(raw_rows))
        AGG_ROWS_INSERTED.inc(len(aggs))

        # Success → processed/
        move_file(fp, data_dir / "processed")
        FILES_PROCESSED.inc()
        logging.info(f"Done: {fp.name} -> processed/")

    except Exception as e:
        logging.exception(f"Failed to process {fp.name}: {e}")
        qdir = data_dir / "quarantine"
        qpath = move_file(fp, qdir)
        (qpath.with_suffix("").with_name(qpath.stem + "__fatal.txt")).write_text(str(e))
        FILES_QUARANTINED.inc()
        PROCESS_ERRORS.inc()


def run():
    parser = argparse.ArgumentParser(description="Real-time sensor pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process current files in data/incoming/ once and exit",
    )
    args = parser.parse_args()

    cfg = Config()
    data_dir = Path("data")
    inc_dir = data_dir / "incoming"
    for sub in ("incoming", "processed", "quarantine"):
        (data_dir / sub).mkdir(parents=True, exist_ok=True)

    engine = make_engine(
        cfg.pg_user, cfg.pg_password, cfg.pg_host, cfg.pg_port, cfg.pg_db
    )

    # Start Prometheus metrics endpoint
    try:
        start_http_server(8000)
        logging.info("Metrics exposed at http://localhost:8000/metrics")
    except Exception as e:
        logging.warning(f"Metrics server not started: {e}")

    logging.info(
        "Polling... drop CSVs into data/incoming/ "
        + ("(one-shot mode)" if args.once else "")
    )

    seen = set()  # basic in-memory idempotence for this run

    def scan_and_process():
        for fp in sorted(inc_dir.glob("*.csv")):
            # skip zero-byte or locked files
            try:
                if fp.stat().st_size == 0:
                    continue
            except Exception:
                continue

            if fp.name in seen:
                continue
            process_file(fp, cfg, data_dir, engine)
            seen.add(fp.name)

    if args.once:
        scan_and_process()
        return

    while True:
        scan_and_process()
        time.sleep(cfg.poll_interval_s)


if __name__ == "__main__":
    run()
