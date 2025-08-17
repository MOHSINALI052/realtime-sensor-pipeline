def validate_transform(
    df: pd.DataFrame, cfg: Config, file_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    # mandatory fields
    df["ts"] = build_timestamp(df)
    df["sensor_id"] = cfg.default_sensor_id
    df["location"] = cfg.default_location

    # ensure numeric
    for col in ("T", "RH"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    value_cols = [c for c in numeric_cols if c not in {"sensor_id", "ts"}]

    long = df[["sensor_id", "ts", "location"] + value_cols].melt(
        id_vars=["sensor_id", "ts", "location"],
        var_name="reading_type",
        value_name="reading_value",
    )

    def unit_and_check(rt: str, val: float) -> tuple[str | None, bool, str]:
        rtl = rt.lower()
        if rtl in ("t", "temp", "temperature"):
            unit = "C"
            ok = (cfg.temp_min_c <= val <= cfg.temp_max_c) if pd.notna(val) else False
            return unit, ok, "temperature"
        if rtl in ("rh", "humidity"):
            unit = "%"
            ok = (cfg.rh_min <= val <= cfg.rh_max) if pd.notna(val) else False
            return unit, ok, "humidity"
        return None, True, rtl  # pass-through for other sensors

    units, oks, canon = [], [], []
    for rt, val in zip(long["reading_type"].astype(str), long["reading_value"]):
        v = float(val) if pd.notna(val) else np.nan
        u, ok, name = unit_and_check(rt, v)
        units.append(u)
        oks.append(ok)
        canon.append(name)

    long["unit"] = units
    long["reading_type"] = canon

    # --- masks built on 'long' and aligned ---
    key_missing_mask = long[["sensor_id", "ts"]].isna().any(axis=1)
    value_missing_mask = long["reading_value"].isna()            # we will IGNORE these rows
    range_bad_mask = (~pd.Series(oks, index=long.index)) & (~value_missing_mask)

    # invalid rows are: missing key OR out-of-range (but NOT just NaN reading)
    invalid_mask = key_missing_mask | range_bad_mask
    invalid = long[invalid_mask].copy()
    invalid["error_reason"] = np.where(
        key_missing_mask.loc[invalid.index], "missing_key_field", "out_of_range"
    )

    # valid rows: must have keys, a reading value, and pass range checks
    valid = long[~key_missing_mask & ~value_missing_mask & ~range_bad_mask].copy()
    valid = valid[["sensor_id", "ts", "reading_type", "reading_value", "unit", "location"]]

    return valid, invalid
