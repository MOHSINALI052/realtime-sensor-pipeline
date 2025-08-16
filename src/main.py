import time
import logging
from pathlib import Path
import pandas as pd
from src.config import Config
from src.db import make_engine, insert_raw, insert_aggregates
from src.processor import load_csv_airquality, validate_transform, compute_aggregates, to_raw_rows
from src.utils import move_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def process_file(fp: Path, cfg: Config, data_dir: Path, engine):
    try:
        logging.info(f"Processing {fp.name}")
        # AirQuality format
        df = load_csv_airquality(str(fp))

        valid, invalid = validate_transform(df, cfg, fp.name)

        if not invalid.empty:
            qdir = data_dir / 'quarantine'
            qpath = move_file(fp, qdir)
            invalid_path = qpath.with_suffix('').with_name(qpath.stem + '__errors.csv')
            invalid.to_csv(invalid_path, index=False)
            logging.warning(f"Validation failed for some rows. File moved to quarantine: {qpath.name}")
            return

        raw_rows = to_raw_rows(valid, fp.name, cfg.source_name)
        aggs = compute_aggregates(valid, fp.name, cfg.source_name)

        insert_raw(engine, raw_rows)
        insert_aggregates(engine, aggs)

        move_file(fp, data_dir / 'processed')
        logging.info(f"Done: {fp.name} -> processed/")

    except Exception as e:
        logging.exception(f"Failed to process {fp.name}: {e}")
        qdir = data_dir / 'quarantine'
        qpath = move_file(fp, qdir)
        (qpath.with_suffix('').with_name(qpath.stem + '__fatal.txt')).write_text(str(e))


def run():
    cfg = Config()
    data_dir = Path('data')
    inc_dir = data_dir / 'incoming'
    for sub in ['incoming','processed','quarantine']:
        (data_dir / sub).mkdir(parents=True, exist_ok=True)

    engine = make_engine(cfg.pg_user, cfg.pg_password, cfg.pg_host, cfg.pg_port, cfg.pg_db)

    logging.info('Polling... drop CSVs into data/incoming/')
    seen = set()
    while True:
        for fp in sorted(inc_dir.glob('*.csv')):
            if fp.stat().st_size == 0:
                continue
            if fp.name in seen:
                continue
            process_file(fp, cfg, data_dir, engine)
            seen.add(fp.name)
        time.sleep(cfg.poll_interval_s)


if __name__ == '__main__':
    run()