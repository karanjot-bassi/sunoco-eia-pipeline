from pathlib import Path

# Project root
ROOT = Path(__file__).parent.parent

# Data paths
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR = ROOT / "data" / "gold"

# EIA API
EIA_BASE_URL = "https://api.eia.gov/series/"
START_DATE = "2025-01"

# Series IDs
# Supply Side
SUPPLY_SERIES = {
    "Refinery and Blender Net Production": "PET.MDIRPP32.M",
    "Imports":                             "PET.MDIIMP32.M",
    "Net Receipts":                        "PET.MDINRP32.M",
    "Adjustments":                         "PET.M_EPD0_VUA_R30_2.M",
    "Stock Change":                        "PET.MDISCP32.M",
}

# Disposition Side
DISPOSITION_SERIES = {
    "Exports":           "PET.MDIEXP32.M",
    "Products Supplied": "PET.MDIUPP32.M",
}

ALL_SERIES = {**SUPPLY_SERIES, **DISPOSITION_SERIES}
SUPPLY_COLS = list(SUPPLY_SERIES.keys())
DISPOSITION_COLS = list(DISPOSITION_SERIES.keys())


