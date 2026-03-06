"""
CDEC (California Data Exchange Center) client for CCSS station data.

API: https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet
Sensor list: https://cdec.water.ca.gov/misc/senslist.html

Improvements over v1:
- Captures DATA_FLAG column from CDEC response
- Returns interleaved value/flag columns matching the SNOTEL schema
"""

import logging
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

logger = logging.getLogger(__name__)

CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet"

# sensor_number -> (variable_name, unit, conversion fn, precision)
SENSORS = {
    30: ("TAVG",   "degF", lambda x: round((x - 32) * 5 / 9, 1), 1),
    32: ("TMIN",   "degF", lambda x: round((x - 32) * 5 / 9, 1), 1),
    31: ("TMAX",   "degF", lambda x: round((x - 32) * 5 / 9, 1), 1),
    18: ("SNWD",   "in",   lambda x: round(x / 39.3701, 4),       4),
    82: ("WTEQ",   "in",   lambda x: round(x / 39.3701, 4),       4),
    45: ("PRCPSA", "in",   lambda x: round(x / 39.3701, 4),       4),
}

DATA_COLS = [meta[0] for meta in SENSORS.values()]


def fetch_station_data(
    station_code: str,
    begin_date: str = "1900-01-01",
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Fetch all CCSS sensor data for a station from CDEC.

    Returns a DataFrame with interleaved value/flag columns:
        TAVG, TAVG_flag, TMIN, TMIN_flag, TMAX, TMAX_flag,
        SNWD, SNWD_flag, WTEQ, WTEQ_flag, PRCPSA, PRCPSA_flag

    Rows where all data columns are NaN are dropped.
    Units: temperatures in Celsius, distances in meters.

    CDEC flag meanings (DATA_FLAG column):
        e  = estimated
        F  = forecasted
        m  = missing
        (blank) = no flag / valid
    Full list: https://cdec.water.ca.gov/misc/QCflags.html
    """
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    params = {
        "Stations": station_code,
        "SensorNums": ",".join(str(s) for s in SENSORS),
        "dur_code": "D",
        "Start": begin_date,
        "End": end_date,
    }

    resp = requests.get(CDEC_URL, params=params, timeout=60)
    resp.raise_for_status()

    raw = pd.read_csv(
        StringIO(resp.content.decode("utf-8")),
        on_bad_lines="skip",
        index_col=False,
    )

    if raw.empty or "DATE TIME" not in raw.columns:
        return pd.DataFrame()

    raw["datetime"] = pd.to_datetime(raw["DATE TIME"], errors="coerce").dt.normalize()
    raw = raw.dropna(subset=["datetime"])
    raw = raw.dropna(subset=["SENSOR_NUMBER"])
    raw["SENSOR_NUMBER"] = raw["SENSOR_NUMBER"].astype(int)

    # Normalize flag column — CDEC may call it DATA_FLAG or similar
    flag_col = next((c for c in raw.columns if "FLAG" in c.upper()), None)
    if flag_col:
        raw["_flag"] = raw[flag_col].fillna("").astype(str).str.strip()
    else:
        raw["_flag"] = ""

    raw["VALUE"] = pd.to_numeric(raw["VALUE"], errors="coerce")

    raw = raw.set_index(["datetime", "SENSOR_NUMBER"])

    # Build one wide row per date
    all_dates = raw.index.get_level_values("datetime").unique().sort_values()
    result = pd.DataFrame(index=all_dates)
    result.index.name = "datetime"

    var_order = [meta[0] for meta in SENSORS.values()]

    for sensor_num, (var_name, _, convert, _) in SENSORS.items():
        if sensor_num not in raw.index.get_level_values("SENSOR_NUMBER"):
            result[var_name] = np.nan
            result[f"{var_name}_flag"] = ""
            continue

        subset = raw.xs(sensor_num, level="SENSOR_NUMBER")

        values = subset["VALUE"].reindex(all_dates).apply(
            lambda x: convert(x) if pd.notna(x) else np.nan
        )
        flags = subset["_flag"].reindex(all_dates).fillna("")

        result[var_name] = values
        result[f"{var_name}_flag"] = flags

    # Interleave columns: TAVG, TAVG_flag, TMIN, TMIN_flag, ...
    ordered = []
    for var_name in var_order:
        ordered.append(var_name)
        ordered.append(f"{var_name}_flag")

    result = result[ordered]
    result = result.dropna(subset=DATA_COLS, how="all")
    return result
