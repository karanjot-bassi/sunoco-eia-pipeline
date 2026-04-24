# Author: Karanjot Bassi

import pandas as pd
from src.config import SILVER_DIR, GOLD_DIR


def run_gold() -> pd.DataFrame:
    """Aggregate silver data to annual averages and pivot to wide format. Saves to gold layer."""
    silver_path = SILVER_DIR / "distillate_padd3_long.csv"
    df = pd.read_csv(silver_path, parse_dates=["period"])

    df["year"] = df["period"].dt.year

    annual = (
        df.groupby(["year", "series_name"])["value_mbbl_d"]
        .mean()
        .reset_index()
        .rename(columns={"value_mbbl_d": "annual_avg_mbbl_d"})
    )

    gold = annual.pivot(
        index="year",
        columns="series_name",
        values="annual_avg_mbbl_d"
    )
    gold.columns.name = None
    gold = gold.reset_index()
    gold = gold.sort_values("year").reset_index(drop=True)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = GOLD_DIR / "distillate_padd3_annual.csv"
    gold.to_csv(out_path, index=False)

    print(f"Gold layer complete. {len(gold)} years saved to {out_path}")
    print("\nColumns:", list(gold.columns))
    print("\nFirst 3 rows:")
    print(gold.head(3).to_string())

    return gold


if __name__ == "__main__":
    run_gold()