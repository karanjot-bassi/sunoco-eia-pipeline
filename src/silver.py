# Author: Karanjot Bassi

import json
import pandas as pd
from pathlib import Path
from src.config import BRONZE_DIR, SILVER_DIR, ALL_SERIES


def load_bronze_series(name: str) -> pd.DataFrame:
    """Load one bronze JSON file and extract period and value columns."""
    safe_name = name.replace(" ", "_").replace("/", "_").lower()
    path = BRONZE_DIR / f"{safe_name}.json"

    with open(path) as f:
        raw = json.load(f)

    records = raw.get("response", {}).get("data", [])

    if not records:
        raise ValueError(f"No data found for series: {name}")

    df = pd.DataFrame(records)
    df = df[["period", "value"]].copy()
    df.columns = ["period", "value_mbbl_d"]
    df["series_name"] = name
    return df


def clean_series(df: pd.DataFrame) -> pd.DataFrame:
    """Parse period to datetime, cast value to float, drop nulls, sort ascending."""
    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], format="%Y-%m")
    df["value_mbbl_d"] = pd.to_numeric(df["value_mbbl_d"], errors="coerce")
    df = df.dropna(subset=["value_mbbl_d"])
    df = df[["period", "series_name", "value_mbbl_d"]].sort_values("period")
    df = df.reset_index(drop=True)
    return df


def run_silver() -> pd.DataFrame:
    """Clean all bronze series and concatenate into a long-format CSV at the silver layer."""
    frames = []

    for name in ALL_SERIES.keys():
        print(f"Processing: {name}")
        raw_df = load_bronze_series(name)
        clean_df = clean_series(raw_df)
        frames.append(clean_df)
        print(f"  {len(clean_df)} rows")

    combined = pd.concat(frames, ignore_index=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SILVER_DIR / "distillate_padd3_long.csv"
    combined.to_csv(out_path, index=False)

    print(f"\nSilver layer complete. {len(combined)} total rows saved to {out_path}")
    return combined


if __name__ == "__main__":
    run_silver()