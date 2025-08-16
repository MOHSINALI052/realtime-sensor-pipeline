from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Config:
    pg_host: str = os.getenv("PGHOST", "localhost")
    pg_port: int = int(os.getenv("PGPORT", 5432))
    pg_user: str = os.getenv("PGUSER", "sensor")
    pg_password: str = os.getenv("PGPASSWORD", "sensorpw")
    pg_db: str = os.getenv("PGDATABASE", "sensordb")

    poll_interval_s: int = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
    source_name: str = os.getenv("SOURCE_NAME", "kaggle/airquality")
    default_sensor_id: str = os.getenv("DEFAULT_SENSOR_ID", "Station_1")
    default_location: str = os.getenv("DEFAULT_LOCATION", "Milan_AirQuality")

    temp_min_c: float = float(os.getenv("TEMP_MIN_C", -50))
    temp_max_c: float = float(os.getenv("TEMP_MAX_C", 50))
    rh_min: float = float(os.getenv("RH_MIN", 0))
    rh_max: float = float(os.getenv("RH_MAX", 100))