# Author: Karanjot Bassi

import os 
import json 
import requests
from pathlib import Path
from dotenv import load_dotenv
from src.config import EIA_BASE_URL, BRONZE_DIR, ALL_SERIES, START_DATE

load_dotenv()

def fetch_series(api_key: str, series_id: str) -> dict:
    """Call EIA v2 API for one series. Raises on non-200 response."""
    url = f"{EIA_BASE_URL}/{series_id}"
    params = {
        "api_key": api_key,
        "length": 5000,
        "offset": 0,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def filter_by_start_date(raw: dict, start: str) -> dict:
    """Drop records before start date and sort oldest first. EIA returns newest first by default."""
    records = raw.get("response", {}).get("data", [])
    filtered = [r for r in records if r["period"] >= start]
    filtered.sort(key=lambda x: x["period"])
    raw["response"]["data"] = filtered
    return raw


def save_bronze(name: str, payload: dict) -> Path:
    """Write raw API response to data/bronze/ as JSON with no transformations."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace(" ", "_").replace("/", "_").lower()
    path = BRONZE_DIR / f"{safe_name}.json"
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    return path


def run_bronze():
    """Fetch all 7 EIA series and save to bronze layer. Skips any series returning 0 records."""
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise EnvironmentError("EIA_API_KEY not set in environment.")

    for name, series_id in ALL_SERIES.items():
        print(f"Fetching: {name} ({series_id})")
        payload = fetch_series(api_key, series_id)
        payload = filter_by_start_date(payload, START_DATE)
        record_count = len(payload["response"]["data"])

        if record_count == 0:
            print(f"  WARNING: 0 records returned for {name} — skipping save")
            continue

        path = save_bronze(name, payload)
        print(f"  {record_count} records saved to {path}")

    print("\nBronze layer complete.")


if __name__ == "__main__":
    run_bronze()
