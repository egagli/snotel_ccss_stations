"""
AWDB REST API client for SNOTEL station data.

Replaces the legacy ulmo/CUAHSI SOAP approach with the NRCS AWDB REST API.

API docs: https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html
Demo:     https://github.com/nrcs-nwcc/iow_awdb_rest_api_demo
"""

import logging
import numpy as np
import pandas as pd
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

AWDB_BASE = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"

# Element code -> (unit from API, conversion fn to SI, output precision)
ELEMENTS = {
    "TAVG":   ("degF", lambda x: round((x - 32) * 5 / 9, 1),  1),
    "TMIN":   ("degF", lambda x: round((x - 32) * 5 / 9, 1),  1),
    "TMAX":   ("degF", lambda x: round((x - 32) * 5 / 9, 1),  1),
    "SNWD":   ("in",   lambda x: round(x / 39.3701, 4),        4),
    "WTEQ":   ("in",   lambda x: round(x / 39.3701, 4),        4),
    "PRCPSA": ("in",   lambda x: round(x / 39.3701, 4),        4),
}

DATA_COLS = list(ELEMENTS.keys())


def station_code_to_triplet(station_code: str) -> str:
    """'1000_OR_SNTL' -> '1000:OR:SNTL'"""
    parts = station_code.split("_")
    # station code format: ID_STATE_NETWORK (e.g. 1000_OR_SNTL)
    return ":".join(parts)


def triplet_to_station_code(triplet: str) -> str:
    """'1000:OR:SNTL' -> '1000_OR_SNTL'"""
    return triplet.replace(":", "_")


def _get(endpoint: str, params: dict) -> list | dict:
    url = f"{AWDB_BASE}/{endpoint}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_element(
    station_triplet: str,
    element: str,
    begin_date: str,
    end_date: str,
    get_flags: bool = True,
) -> pd.DataFrame:
    """
    Fetch a single element for a single station from the AWDB REST API.

    Returns a DataFrame with columns ['value', 'flag'] (flag only if get_flags=True),
    indexed by DatetimeIndex at daily frequency.

    Values are converted to SI units (Celsius, meters).
    """
    params = {
        "stationTriplets": station_triplet,
        "elements": element,
        "duration": "DAILY",
        "beginDate": begin_date,
        "endDate": end_date,
        "getFlags": "true" if get_flags else "false",
        "periodRef": "START",
    }

    result = _get("data", params)

    if not result:
        return pd.DataFrame()

    station_data = result[0]
    if not station_data.get("data"):
        return pd.DataFrame()

    block = station_data["data"][0]
    raw_values = block.get("values", [])

    if not raw_values:
        return pd.DataFrame()

    # Response values can be dicts {"date": "...", "value": ..., "flag": "..."}
    # or plain scalars depending on the endpoint variant — handle both.
    if isinstance(raw_values[0], dict):
        dates = [v["date"] for v in raw_values]
        values = [v.get("value") for v in raw_values]
        flags = [v.get("flag", "") for v in raw_values] if get_flags else [None] * len(raw_values)
    else:
        # Compact form: parallel arrays beginDate + values + flags
        start = pd.to_datetime(block["beginDate"])
        dates = pd.date_range(start=start, periods=len(raw_values), freq="D").strftime("%Y-%m-%d")
        values = raw_values
        flags = block.get("flags", [None] * len(raw_values)) if get_flags else [None] * len(raw_values)

    _, convert, _ = ELEMENTS[element]

    converted_values = []
    for v in values:
        if v is None or v == -9999:
            converted_values.append(np.nan)
        else:
            try:
                converted_values.append(convert(float(v)))
            except (TypeError, ValueError):
                converted_values.append(np.nan)

    df = pd.DataFrame(
        {"value": converted_values, "flag": flags},
        index=pd.to_datetime(dates),
    )
    df.index.name = "datetime"
    return df


def fetch_station_data(
    station_code: str,
    begin_date: str = "1900-01-01",
    end_date: str | None = None,
    get_flags: bool = True,
) -> pd.DataFrame:
    """
    Fetch all elements for a SNOTEL station.

    Returns a DataFrame with interleaved value/flag columns:
        TAVG, TAVG_flag, TMIN, TMIN_flag, TMAX, TMAX_flag,
        SNWD, SNWD_flag, WTEQ, WTEQ_flag, PRCPSA, PRCPSA_flag

    Rows where all data columns are NaN are dropped.
    Units: temperatures in Celsius, distances in meters.
    """
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    triplet = station_code_to_triplet(station_code)
    frames: dict[str, pd.DataFrame] = {}

    for element in ELEMENTS:
        try:
            df = fetch_element(triplet, element, begin_date, end_date, get_flags)
            if not df.empty:
                frames[element] = df
        except requests.HTTPError as e:
            logger.warning(f"{station_code} {element}: HTTP {e.response.status_code}")
        except Exception as e:
            logger.warning(f"{station_code} {element}: {e}")

    if not frames:
        return pd.DataFrame()

    # Build a unified date index spanning all elements
    all_dates = pd.date_range(
        start=min(f.index.min() for f in frames.values()),
        end=max(f.index.max() for f in frames.values()),
        freq="D",
        name="datetime",
    )
    combined = pd.DataFrame(index=all_dates)

    for element, df in frames.items():
        combined[element] = df["value"]
        if get_flags:
            combined[f"{element}_flag"] = df["flag"].where(df["flag"].notna(), "")

    # Ensure all expected columns exist
    for element in ELEMENTS:
        if element not in combined.columns:
            combined[element] = np.nan
        if get_flags and f"{element}_flag" not in combined.columns:
            combined[f"{element}_flag"] = ""

    # Interleave: TAVG, TAVG_flag, TMIN, TMIN_flag, ...
    ordered = []
    for element in ELEMENTS:
        ordered.append(element)
        if get_flags:
            ordered.append(f"{element}_flag")

    combined = combined[ordered]
    combined = combined.dropna(subset=DATA_COLS, how="all")
    return combined


def fetch_all_station_metadata(network: str = "SNTL") -> pd.DataFrame:
    """
    Fetch station metadata for all active stations in a network.

    Returns a DataFrame indexed by station code (e.g. '1000_OR_SNTL').
    """
    params = {
        "stationTriplets": f"*:*:{network}",
        "activeOnly": "false",
    }
    result = _get("stations", params)

    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result)
    df["code"] = df["stationTriplet"].apply(triplet_to_station_code)
    return df.set_index("code")
