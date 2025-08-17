"""
Corrupt an AirQuality-style CSV by injecting missing and out-of-range values.

Usage (from project root):
    py -3.11 scripts/corrupt_data.py --input AirQuality.csv --output data/incoming/AirQuality_corrupt.csv
    # optional tuning:
    # py -3.11 scripts/corrupt_data.py --input AirQuality.csv --output data/incoming/AQ_bad.csv --null-frac 0.02 --oor-frac 0.01 --seed 123

Notes:
- Reads with sep=';' and decimal=',' (AirQuality format).
- Writes back with the same format.
- Adds NaNs randomly to numeric columns.
- Pushes obvious out-of-range values into T (°C) and RH (%) if those columns exist.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


def load_airquality(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", low_memory=False)
    # drop unnamed empty columns
    df = df[[c for c in df.columns if not c.lower().startswith("unnamed")]]
    return df


def save_airquality(df: pd.DataFrame, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=";", decimal=",", index=False)


def corrupt_df(df: pd.DataFrame, null_frac: float, oor_frac: float, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = df.copy()

    # Randomly null out entries in numeric columns
    num_cols = out.select_dtypes(include=["number"]).columns.tolist()
    for col in num_cols:
        if len(out) == 0:
            continue
        mask = rng.random(len(out)) < null_frac
        out.loc[mask, col] = np.nan

    # Inject out-of-range values for T and RH if present
    if "T" in out.columns and len(out) > 0:
        idx = out.sample(frac=min(oor_frac, 1.0), random_state=seed).index
        out.loc[idx, "T"] = 200.0  # invalid temperature in °C

    if "RH" in out.columns and len(out) > 0:
        idx = out.sample(frac=min(oor_frac, 1.0), random_state=seed + 1).index
        out.loc[idx, "RH"] = 150.0  # invalid humidity in %

    return out


def main():
    ap = argparse.ArgumentParser(description="Corrupt AirQuality CSV for pipeline testing.")
    ap.add_argument("--input", required=True, help="Path to clean AirQuality CSV")
    ap.add_argument("--output", required=True, help="Destination path for corrupted CSV (e.g., data/incoming/xyz.csv)")
    ap.add_argument("--null-frac", type=float, default=0.01, help="Fraction of numeric cells to null (default: 0.01)")
    ap.add_argument("--oor-frac", type=float, default=0.005, help="Fraction of rows to push out-of-range for T/RH (default: 0.005)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    args = ap.parse_args()

    df = load_airquality(args.input)
    bad = corrupt_df(df, null_frac=args.null_frac, oor_frac=args.oor_frac, seed=args.seed)
    save_airquality(bad, args.output)
    print(f"Wrote corrupted CSV to: {args.output}")


if __name__ == "__main__":
    main()
