from typing import Iterable, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def make_engine(user: str, password: str, host: str, port: int, db: str) -> Engine:
    """
    Create a pooled SQLAlchemy engine for PostgreSQL.
    """
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    # Tune pool sizes conservatively for local dev; pool_pre_ping guards stale conns.
    engine = create_engine(url, pool_size=10, max_overflow=20, pool_pre_ping=True)
    return engine


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, max=10),
    retry=retry_if_exception_type(OperationalError),
)
def insert_raw(engine: Engine, rows: Iterable[Dict[str, Any]]):
    """
    Insert raw rows. Step 15 will switch this to idempotent UPSERTs.
    """
    rows = list(rows)
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_readings
                  (sensor_id, ts, source, location, reading_type, reading_value, unit, file_name)
                VALUES
                  (:sensor_id, :ts, :source, :location, :reading_type, :reading_value, :unit, :file_name)
                """
            ),
            rows,
        )


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, max=10),
    retry=retry_if_exception_type(OperationalError),
)
def insert_aggregates(engine: Engine, rows: Iterable[Dict[str, Any]]):
    """
    Insert per-file aggregates. Step 15 will switch this to UPSERT on (file_name, reading_type).
    """
    rows = list(rows)
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO file_aggregates
                  (file_name, source, reading_type, count, min_value, max_value, avg_value, stddev_value, window_start, window_end)
                VALUES
                  (:file_name, :source, :reading_type, :count, :min_value, :max_value, :avg_value, :stddev_value, :window_start, :window_end)
                """
            ),
            rows,
        )
