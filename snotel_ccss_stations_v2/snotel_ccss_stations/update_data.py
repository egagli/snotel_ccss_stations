#!/usr/bin/env python
"""
Daily update script for SNOTEL and CCSS station CSV files.

Key improvements over v1:
  - AWDB REST API instead of CUAHSI SOAP (faster, returns quality flags)
  - Quality flags stored inline alongside each variable (_flag columns)
  - Per-station audit logs tracking retroactive value and flag changes
  - update_stats.json tracks last-pull time and change velocity per station
  - Adaptive lookback: stations with frequent retroactive changes get re-examined further back
  - Structured logging instead of bare except / silent failures
"""

import glob
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import awdb_client
import ccss_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
AUDIT_DIR = DATA_DIR / "audit"
STATS_FILE = DATA_DIR / "update_stats.json"
GEOJSON_PATH = REPO_ROOT / "all_stations.geojson"

DEFAULT_LOOKBACK_DAYS = 30          # re-examine this many days on every run
MAX_LOOKBACK_DAYS = 365             # cap for stations with high change rates
MIN_RETROACTIVE_CHANGES_FOR_EXTEND = 3  # if a station had ≥ this many retroactive
                                        # changes in last run, double the lookback

DATA_COLS = ["TAVG", "TMIN", "TMAX", "SNWD", "WTEQ", "PRCPSA"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def load_stats() -> dict:
    if STATS_FILE.exists():
        with open(STATS_FILE) as f:
            return json.load(f)
    return {}


def save_stats(stats: dict) -> None:
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2, default=str)


def lookback_days_for(station_code: str, stats: dict) -> int:
    """Return how many days back to re-fetch for change detection."""
    retro = stats.get(station_code, {}).get("retroactive_changes_last_run", 0)
    if retro >= MIN_RETROACTIVE_CHANGES_FOR_EXTEND:
        days = min(DEFAULT_LOOKBACK_DAYS * 2, MAX_LOOKBACK_DAYS)
    else:
        days = DEFAULT_LOOKBACK_DAYS
    return days


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------

def _audit_path(station_code: str) -> Path:
    return AUDIT_DIR / f"{station_code}_audit.csv"


AUDIT_COLS = ["detected_on", "date", "variable", "old_value", "new_value",
              "old_flag", "new_flag", "change_type"]


def detect_and_log_changes(
    station_code: str,
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
) -> int:
    """
    Compare the overlapping date range between existing CSV data and freshly
    fetched data.  Log any value or flag changes to the per-station audit CSV.

    Returns the number of changes detected.
    """
    overlap = existing.index.intersection(incoming.index)
    if overlap.empty:
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    changes = []

    for col in DATA_COLS:
        if col not in existing.columns or col not in incoming.columns:
            continue

        old_vals = existing.loc[overlap, col]
        new_vals = incoming.loc[overlap, col]
        flag_col = f"{col}_flag"
        old_flags = existing.loc[overlap, flag_col] if flag_col in existing.columns else pd.Series("", index=overlap)
        new_flags = incoming.loc[overlap, flag_col] if flag_col in incoming.columns else pd.Series("", index=overlap)

        # Detect value changes (NaN != NaN is False in pandas, so handle explicitly)
        both_nan = old_vals.isna() & new_vals.isna()
        value_changed = ~both_nan & (old_vals.fillna("__nan__") != new_vals.fillna("__nan__"))

        for date in overlap[value_changed]:
            changes.append({
                "detected_on": now,
                "date": date.strftime("%Y-%m-%d"),
                "variable": col,
                "old_value": "" if pd.isna(old_vals[date]) else old_vals[date],
                "new_value": "" if pd.isna(new_vals[date]) else new_vals[date],
                "old_flag": old_flags.get(date, ""),
                "new_flag": new_flags.get(date, ""),
                "change_type": "value",
            })

        # Detect flag-only changes (value unchanged)
        flag_changed = ~value_changed & (old_flags.fillna("") != new_flags.fillna(""))
        for date in overlap[flag_changed]:
            changes.append({
                "detected_on": now,
                "date": date.strftime("%Y-%m-%d"),
                "variable": col,
                "old_value": "" if pd.isna(old_vals[date]) else old_vals[date],
                "new_value": "" if pd.isna(new_vals[date]) else new_vals[date],
                "old_flag": old_flags.get(date, ""),
                "new_flag": new_flags.get(date, ""),
                "change_type": "flag",
            })

    if changes:
        audit_df = pd.DataFrame(changes, columns=AUDIT_COLS)
        path = _audit_path(station_code)
        audit_df.to_csv(
            path,
            mode="a",
            header=not path.exists(),
            index=False,
        )
        logger.info(f"{station_code}: {len(changes)} retroactive change(s) logged to audit")

    return len(changes)


# ---------------------------------------------------------------------------
# Per-station update
# ---------------------------------------------------------------------------

def update_station(station_code: str, csv_path: Path, stats: dict) -> bool:
    """
    Fetch new data for a station, merge with existing CSV, detect changes.

    Returns True on success, False on failure.
    """
    is_ccss = len(station_code) == 3  # CCSS codes are 3 chars (e.g. '49M')

    try:
        existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    except Exception as e:
        logger.error(f"{station_code}: failed to read existing CSV: {e}")
        return False

    if existing.empty:
        logger.warning(f"{station_code}: existing CSV is empty, skipping")
        return False

    last_date = existing.index[-1]
    lookback = lookback_days_for(station_code, stats)
    fetch_from = (last_date - timedelta(days=lookback)).strftime("%Y-%m-%d")
    today = datetime.today().strftime("%Y-%m-%d")

    logger.info(f"{station_code}: fetching {fetch_from} → {today} (lookback={lookback}d)")

    try:
        if is_ccss:
            incoming = ccss_client.fetch_station_data(
                station_code, begin_date=fetch_from, end_date=today
            )
        else:
            incoming = awdb_client.fetch_station_data(
                station_code, begin_date=fetch_from, end_date=today
            )
    except Exception as e:
        logger.error(f"{station_code}: fetch failed: {e}")
        return False

    if incoming.empty:
        logger.warning(f"{station_code}: no data returned")
        return False

    # Audit: check what changed in the overlapping window
    n_changes = detect_and_log_changes(station_code, existing, incoming)

    # Align columns: incoming may have flag columns that existing lacks (first
    # run after schema migration), or vice versa.
    all_cols = list(dict.fromkeys(list(existing.columns) + list(incoming.columns)))
    existing = existing.reindex(columns=all_cols)
    incoming = incoming.reindex(columns=all_cols)

    combined = pd.concat([existing, incoming])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()

    combined.to_csv(csv_path, index=True)

    # Update stats for this station
    stats[station_code] = {
        "last_pull": datetime.now(timezone.utc).isoformat(),
        "last_data_date": combined.index[-1].strftime("%Y-%m-%d"),
        "retroactive_changes_last_run": n_changes,
    }

    return True


# ---------------------------------------------------------------------------
# GeoJSON update
# ---------------------------------------------------------------------------

def update_geojson(stats: dict) -> None:
    if not GEOJSON_PATH.exists():
        logger.warning("all_stations.geojson not found, skipping GeoJSON update")
        return

    gdf = gpd.read_file(GEOJSON_PATH)

    for station_code, s in stats.items():
        if "last_data_date" in s:
            mask = gdf["code"] == station_code
            if mask.any():
                gdf.loc[mask, "endDate"] = s["last_data_date"]

    gdf.to_file(GEOJSON_PATH, driver="GeoJSON")
    logger.info("all_stations.geojson updated")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    stats = load_stats()
    csv_files = sorted(glob.glob(str(DATA_DIR / "*.csv")))

    if not csv_files:
        logger.error(f"No CSV files found in {DATA_DIR}")
        sys.exit(1)

    logger.info(f"Updating {len(csv_files)} station(s)")

    succeeded = 0
    failed = 0
    failed_stations = []

    # Regex to extract station code from filename
    code_re = re.compile(r"/(?P<code>[^/]+)\.csv$")

    for csv_path in csv_files:
        m = code_re.search(csv_path)
        if not m:
            continue
        station_code = m.group("code")

        if update_station(station_code, Path(csv_path), stats):
            succeeded += 1
        else:
            failed += 1
            failed_stations.append(station_code)

    save_stats(stats)
    update_geojson(stats)

    logger.info(f"Done. {succeeded} succeeded, {failed} failed.")
    if failed_stations:
        logger.warning(f"Failed stations: {', '.join(failed_stations)}")


if __name__ == "__main__":
    main()
