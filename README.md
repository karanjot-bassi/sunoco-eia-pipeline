# sunoco-eia-pipeline

ETL pipeline that ingests monthly EIA petroleum supply and disposition data for PADD 3 Distillate Fuel Oil, cleans it into a long-format CSV, and aggregates to annual averages in wide format for analysis.

**Data source:** U.S. Energy Information Administration (EIA) Open Data API  
**Product:** Distillate Fuel Oil (diesel + heating oil)  
**Region:** PADD 3 — Gulf Coast (Texas, Louisiana, Mississippi, Alabama, Arkansas)  
**Unit:** Thousand Barrels per Day (MBBL/D)  
**Coverage:** January 2015 — most recent available month

---

## Setup

**Requirements:** Python 3.9, virtual environment recommended.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**API key:** Copy `.env.example` to `.env` and add your EIA API key. Keys are free at [eia.gov/opendata](https://www.eia.gov/opendata/).

```
EIA_API_KEY=your_key_here
```

`.env` is gitignored and never committed.

---

## Running the Pipeline

Each layer is run as a module from the project root. They must be run in order — each layer depends on the previous one.

```bash
python3 -m src.bronze   # fetch from EIA API, save raw JSON
python3 -m src.silver   # clean and normalize to long-format CSV
python3 -m src.gold     # aggregate to annual averages, pivot to wide format
```

---

## Project Structure

```
sunoco-eia-pipeline/
├── .env                          # API key — gitignored, never committed
├── .env.example                  # template showing required variable name
├── requirements.txt
├── data/
│   ├── bronze/                   # gitignored — raw JSON from EIA API, one file per series
│   ├── silver/
│   │   └── distillate_padd3_long.csv     # 810 rows x 3 cols
│   └── gold/
│       ├── distillate_padd3_annual.csv   # 12 rows x 8 cols
│       └── padd3_distillate_chart.png    # chart output from notebook
├── src/
│   ├── config.py       # series IDs, file paths, constants
│   ├── bronze.py       # API ingestion
│   ├── silver.py       # cleaning and normalization
│   └── gold.py         # aggregation and pivot
├── tests/
│   ├── test_silver.py      # unit tests for clean_series() — no API calls, no file I/O
│   └── test_validation.py  # data reasonableness checks against gold layer output
└── notebooks/
    └── pipeline_report.ipynb   # data mapping, architecture notes, market analysis
```

---

## Data Layers

**Bronze** — Raw API responses saved as JSON with no transformations. Records are filtered to `>= 2015-01` before saving. If a series returns 0 records, it is skipped with a warning.

**Silver** — Each bronze file is loaded, periods are parsed to datetime, values are cast to float, nulls are dropped, and all 7 series are concatenated into one long-format CSV.

Schema: `period (datetime)`, `series_name (str)`, `value_mbbl_d (float)`

**Gold** — Silver data is grouped by year and series, annual averages are computed, and the result is pivoted to wide format with one row per year.

Schema: `year`, `Adjustments`, `Exports`, `Imports`, `Net Receipts`, `Products Supplied`, `Refinery and Blender Net Production`, `Stock Change`

---

## EIA Series

| Series Name | EIA Series ID | Side |
|---|---|---|
| Refinery and Blender Net Production | PET.MDIRPP32.M | Supply |
| Imports | PET.MDIIMP32.M | Supply |
| Net Receipts | PET.MDINRP32.M | Supply |
| Adjustments | PET.M_EPD0_VUA_R30_2.M | Supply |
| Stock Change | PET.MDISCP32.M | Disposition |
| Exports | PET.MDIEXP32.M | Disposition |
| Products Supplied | PET.MDIUPP32.M | Disposition |

**Data availability notes:**

- **Adjustments** — available January 2015 through December 2020 only. EIA discontinued this series after 2020. Gold shows `NaN` for 2021 onward.
- **Imports** — sparse (73 of 133 months). PADD 3 rarely imports distillate. Missing months represent zero or unreported imports.
- **Biofuels Plant Net Production** — excluded. PADD 3 reports zero biofuel-derived distillate across all periods; no dedicated API series exists for this product/region combination.
- **2026** — only January is available. Treated as an incomplete year and excluded from the chart and market analysis.

---

## Tests

```bash
python3 -m pytest tests/ -v
```

**test_silver.py** — 6 unit tests for `clean_series()` using in-memory data. Covers: period parsing, float casting, null dropping, schema shape, sort order, non-numeric coercion.

**test_validation.py** — 9 validation tests against the actual gold layer CSV. Covers: file existence, year coverage, no nulls in core columns, production always positive, exports always positive, net receipts always negative, production exceeds exports, production within expected range, supply/disposition balance within 20% tolerance for years 2015–2020.

All 15 tests pass.

---

## Notebook

`notebooks/pipeline_report.ipynb` runs the full pipeline, prints test results, and includes a market analysis section with a line chart of the four main series (production, exports, products supplied, net receipts) from 2015 to 2025.

The notebook uses `subprocess` to run pytest inline and `matplotlib` to render and save the chart to `data/gold/padd3_distillate_chart.png`.
