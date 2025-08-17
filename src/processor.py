# src/processor.py

from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

from .config import Config


# ------------------------------------------------------------
# Loader for AirQuality.csv
#  - separator ';', decimal ','
#  - drops "Unnamed" empty columns
#  - converts sentinel -200 to NaN (common in this dataset)
# ------------------------------------------------------------
def load_csv_airquality(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", low_memory=False)
    df = df[[c for c in df.columns if not c.lower().startswith("unnamed")]]
    # replace sentinel -200 with NaN for numeric cols
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].replace(-200, np.nan)
    return df


# ------------------------------------------------------------
# Build UTC timestamp from Date (dd/mm/yyyy) + Time (HH.MM.SS)
# Explicit format removes pandas warnings
# ------------------------------------------------------------
def build_timestamp(df: pd.DataFrame) -> pd.Series:
    ts = pd.to_datetime(
        df["Date"] + " " + df["Time"],
        format="%d/%m/%Y %H.%M.%S",
        errors="coerce",
    )
    return ts.dt.tz_localize("UTC")


# ------------------------------------------------------------
# Validation + transformation to normalized long format
# Returns (valid_df, invalid_df)
#   valid:   sensor_id, ts, reading_type, reading_value, unit, location
#   invalid: same + error_reason
# ------------------------------------------------------------
def validate_transform(
    df: pd.DataFrame, cfg: Config, file_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    # metadata / mandatory fields
    df["ts"] = build_timestamp(df)
    df["sensor_id"] = cfg.default_sensor_id
    df["location"] = cfg.default_location

    # make sure T/RH are numeric (sometimes parsed as object)
    for col in ("T", "RH"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # numeric columns treated as measurements (gas sensors, T, RH, etc.)
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    # key fields must exist
    key_ok = df[["sensor_id", "ts"]].notna().all(axis=1)

    # melt wide → long
    value_cols = [c for c in numeric_cols if c not in {"sensor_id", "ts"}]
    long = df[["sensor_id", "ts", "location"] + value_cols].melt(
        id_vars=["sensor_id", "ts", "location"],
        var_name="reading_type",
        value_name="reading_value",
    )

    # drop rows missing keys / measurement
    long = long[key_ok.repeat(len(value_cols)).values]
    long = long.dropna(subset=["reading_value"])

    # Map units + basic range checks for temperature/humidity
    def unit_and_check(rt: str, val: float) -> tuple[str | None, bool, str]:
        rtl = rt.lower()
        if rtl in ("t", "temp", "temperature"):
            unit = "C"
            ok = cfg.temp_min_c <= val <= cfg.temp_max_c
            return unit, ok, "temperature"
        if rtl in ("rh", "humidity"):
            unit = "%"
            ok = cfg.rh_min <= val <= cfg.rh_max
            return unit, ok, "humidity"
        # passthrough for other sensors (e.g., gas)
        return None, True, rtl

    units: list[str | None] = []
    oks: list[bool] = []
    canonical_types: list[str] = []
    for rt, val in zip(long["reading_type"].astype(str), long["reading_value"].astype(float)):
        u, ok, cname = unit_and_check(rt, val)
        units.append(u)
        oks.append(ok)
        canonical_types.append(cname)

    long["unit"] = units
    long["reading_type"] = canonical_types
    valid_mask = pd.Series(oks, index=long.index)

    invalid = long[
        ~valid_mask | long[["sensor_id", "ts", "reading_value"]].isna().any(axis=1)
    ].copy()
    invalid["error_reason"] = np.where(~valid_mask, "out_of_range", "missing_key_field")

    valid = long[
        valid_mask & long[["sensor_id", "ts", "reading_value"]].notna().all(axis=1)
    ].copy()
    valid = valid[["sensor_id", "ts", "reading_type", "reading_value", "unit", "location"]]

    return valid, invalid


# ------------------------------------------------------------
# Aggregates per reading_type within a single file
# ------------------------------------------------------------
def compute_aggregates(
    valid: pd.DataFrame, file_name: str, source: str
) -> list[dict[str, Any]]:
    if valid.empty:
        return []
    window_start = valid["ts"].min()
    window_end = valid["ts"].max()

    stats = (
        valid.groupby("reading_type")["reading_value"]
        .agg(["count", "min", "max", "mean", "std"])
        .reset_index()
    )

    out: list[dict[str, Any]] = []
    for _, r in stats.iterrows():
        out.append(
            {
                "file_name": file_name,
                "source": source,
                "reading_type": r["reading_type"],
                "count": int(r["count"]),
                "min_value": float(r["min"]),
                "max_value": float(r["max"]),
                "avg_value": float(r["mean"]),
                "stddev_value": float(r["std"]) if pd.notna(r["std"]) else 0.0,
                "window_start": window_start,
                "window_end": window_end,
            }
        )
    return out


# ------------------------------------------------------------
# Idempotency key (Step 15): stable SHA-256 over key fields
# ------------------------------------------------------------
def _dedupe_key(sensor_id, ts, reading_type, file_name) -> str:
    key = f"{sensor_id}|{pd.Timestamp(ts).isoformat()}|{reading_type}|{file_name}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# Convert valid DF → list[dict] for DB inserts (includes dedupe_key)
# ------------------------------------------------------------
def to_raw_rows(valid: pd.DataFrame, file_name: str, source: str) -> list[dict[str, Any]]:
    if valid.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in valid.itertuples(index=False):
        sensor_id = getattr(row, "sensor_id")
        ts = getattr(row, "ts")
        reading_type = getattr(row, "reading_type")
        rows.append(
            {
                "sensor_id": sensor_id,
                "ts": ts,
                "source": source,
                "location": getattr(row, "location"),
                "reading_type": reading_type,
                "reading_value": float(getattr(row, "reading_value")),
                "unit": getattr(row, "unit"),
                "file_name": file_name,
                "dedupe_key": _dedupe_key(sensor_id, ts, reading_type, file_name),
            }
        )
    return rows
