from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from .config import Config

# Columns present in AirQuality.csv
# Date (dd/mm/yyyy), Time (HH.MM.SS), T (°C), RH (%), باقی گیس سینسرز بھی ہیں


def load_csv_airquality(path: str) -> pd.DataFrame:
    # AirQuality: sep=';' and decimal=','
    df = pd.read_csv(path, sep=';', decimal=',', low_memory=False)
    # Drop empty unnamed columns if exist
    df = df[[c for c in df.columns if not c.lower().startswith('unnamed')]]
    return df


def build_timestamp(df: pd.DataFrame) -> pd.Series:
    # Merge Date + Time → UTC-aware timestamp (assume local naive → UTC)
    ts = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True, errors='coerce')
    # Make UTC (naive → UTC)
    ts = ts.dt.tz_localize('UTC')
    return ts


def validate_transform(df: pd.DataFrame, cfg: Config, file_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    # Build mandatory fields
    df['ts'] = build_timestamp(df)
    df['sensor_id'] = cfg.default_sensor_id
    df['location'] = cfg.default_location

    # Keep numeric columns as measurements
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

    # Additionally include known numeric columns even if parsed as object
    for col in ['T', 'RH']:
        if col in df.columns and col not in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            numeric_cols.append(col)

    # Key fields must not be null
    key_ok = df[['sensor_id', 'ts']].notna().all(axis=1)

    # Melt to long format
    value_cols = [c for c in numeric_cols if c not in {'sensor_id', 'ts'}]
    long = df[['sensor_id', 'ts', 'location'] + value_cols].melt(
        id_vars=['sensor_id', 'ts', 'location'], var_name='reading_type', value_name='reading_value'
    )

    # Drop null measurement rows
    long = long[key_ok.repeat(len(value_cols)).values]
    long = long.dropna(subset=['reading_value'])

    # Units + range checks for temperature & humidity (others pass-through)
    def unit_and_check(rt: str, val: float) -> tuple[str|None, bool]:
        rtl = rt.lower()
        if rtl in ('t', 'temp', 'temperature'):
            unit = 'C'
            ok = (cfg.temp_min_c <= val <= cfg.temp_max_c)
            return unit, ok
        if rtl in ('rh', 'humidity'):
            unit = '%'
            ok = (cfg.rh_min <= val <= cfg.rh_max)
            return unit, ok
        # other gas sensors → no strict range here
        return None, True

    units = []
    valid_mask = []
    for rt, val in zip(long['reading_type'].astype(str), long['reading_value'].astype(float)):
        u, ok = unit_and_check(rt, val)
        units.append(u)
        valid_mask.append(ok)
    long['unit'] = units

    valid_mask = pd.Series(valid_mask, index=long.index)

    invalid = long[~valid_mask | long[['sensor_id','ts','reading_value']].isna().any(axis=1)].copy()
    invalid['error_reason'] = np.where(~valid_mask, 'out_of_range', 'missing_key_field')

    valid = long[valid_mask & long[['sensor_id','ts','reading_value']].notna().all(axis=1)].copy()

    # Normalize reading_type names for T/RH only
    rt_map = {'t': 'temperature', 'rh': 'humidity'}
    valid['reading_type'] = valid['reading_type'].str.lower().replace(rt_map)

    valid = valid[['sensor_id','ts','reading_type','reading_value','unit','location']]
    return valid, invalid


def compute_aggregates(valid: pd.DataFrame, file_name: str, source: str) -> list[dict[str, Any]]:
    if valid.empty:
        return []
    window_start = valid['ts'].min()
    window_end = valid['ts'].max()

    stats = valid.groupby('reading_type')['reading_value'].agg(['count','min','max','mean','std']).reset_index()
    out: list[dict[str, Any]] = []
    for _, r in stats.iterrows():
        out.append({
            'file_name': file_name,
            'source': source,
            'reading_type': r['reading_type'],
            'count': int(r['count']),
            'min_value': float(r['min']),
            'max_value': float(r['max']),
            'avg_value': float(r['mean']),
            'stddev_value': float(r['std']) if pd.notna(r['std']) else 0.0,
            'window_start': window_start,
            'window_end': window_end,
        })
    return out


def to_raw_rows(valid: pd.DataFrame, file_name: str, source: str) -> list[dict[str, Any]]:
    if valid.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in valid.itertuples(index=False):
        rows.append({
            'sensor_id': getattr(row, 'sensor_id'),
            'ts': getattr(row, 'ts'),
            'source': source,
            'location': getattr(row, 'location'),
            'reading_type': getattr(row, 'reading_type'),
            'reading_value': float(getattr(row, 'reading_value')),
            'unit': getattr(row, 'unit'),
            'file_name': file_name,
        })
    return rows