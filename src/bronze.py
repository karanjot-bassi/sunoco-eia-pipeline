import os 
import json 
import requests
from pathlib import Path
from dotenv import load_dotenv
from src.config import EIA_BASE_URL, BRONZE_DIR, ALL_SERIES, START_DATE

load_dotenv()

# Fetch one EIA series 
# Returns full JSON response as a dict
# Raises in non-200 response.
def fetch_series(api_key: str, series_id: str) -> dict:
    url = f"{EIA_BASE_URL}/{series_id}"
    params = {
        "api_key": api_key,
        "length": 5000,  # request up to 5000 records
        "offset": 0,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# Filter response data, include periods >= start date
# api returns news first
def filter_by_start_date(raw: dict, start: str) -> dict:
    records = raw.get("response", {}).get("data", [])
    filtered = [r for r in records if r["period"] >= start]
    filtered.sort(key=lambda x: x["period"])
    raw["response"]["data"] = filtered
    return raw

# save raw API response to bronze layer as JSON
# No transformation
def save_bronze(name: str, payload: dict) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace(" ", "_").replace("/", "_").lower()
    path = BRONZE_DIR / f"{safe_name}.json"
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    return path


def run_bronze():
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
