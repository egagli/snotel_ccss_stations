"""
Air temperature bias correction utility for SNOTEL stations.

Background
----------
SNOTEL sites switched to the YSI 44019A extended-range thermistor in the late
1990s.  Temperature was computed from sensor voltage via a linear least-squares
regression, which introduced a systematic non-linear bias.  NRCS is debiasing
historical records (ongoing ~2023–2028).

This module lets you:
  1. Fetch the NRCS HATBC metadata to know which station-date ranges are biased.
  2. Apply the recommended NOAA9 polynomial correction to a temperature series.
  3. Check whether a given station/date is in a known-biased period.

References
----------
- NRCS bias correction page:
    https://www.nrcs.usda.gov/resources/guides-and-instructions/air-temperature-bias-correction
- NRCS unbias table (station-level HTML):
    https://www.wcc.nrcs.usda.gov/ftpref/support/air_temp_bias/nrcs_air_temp_unbias.html
- Correction equation study (PDF):
    https://www.nrcs.usda.gov/sites/default/files/2023-05/Final_Temperature_Correction_Study05262023.pdf
- Transformation of SNOTEL Temperature Record (PDF):
    https://www.nrcs.usda.gov/sites/default/files/2023-04/Transformation%20of%20SNOTEL%20Temperature%20-%20Methodology%20and%20Implications.pdf

Usage
-----
    from bias_correction import apply_noaa9, load_hatbc_metadata, correct_station_temps

    # Check / apply correction to a DataFrame column
    df["TAVG"] = correct_station_temps("1000_OR_SNTL", df.index, df["TAVG"])

    # Or apply the polynomial directly (temperature in Celsius)
    corrected = apply_noaa9(biased_celsius)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NOAA9 polynomial coefficients
# ---------------------------------------------------------------------------
# Source: Atwood (2022) "Evaluation of NOAA vs SNOW YSI Temperature Correction
# Equations", NRCS NWCC.
# The polynomial maps biased temperature (°C) -> corrected temperature (°C).
# Valid range: -55 °C to 60 °C  (outside this range the non-linear response
# is too large to correct reliably).
#
# NOTE: These coefficients are published by NRCS; verify against the current
# version of the correction study PDF before relying on them in production.
# The coefficients below are for the CONUS equation.  Alaska uses a separate
# equation (not included here).

NOAA9_COEFS_CONUS = np.array([
    -2.03150e-1,   #  a0  (constant)
     1.01023e+0,   #  a1  (linear)
     1.89219e-3,   #  a2
    -2.26372e-5,   #  a3
    -6.97498e-7,   #  a4
     2.77526e-9,   #  a5
     2.37048e-10,  #  a6
    -9.21220e-13,  #  a7
    -3.72050e-14,  #  a8
     1.52840e-16,  #  a9
])

CORRECTION_VALID_RANGE = (-55.0, 60.0)  # °C

# ---------------------------------------------------------------------------
# HATBC metadata
# ---------------------------------------------------------------------------

HATBC_URL = "https://www.wcc.nrcs.usda.gov/ftpref/support/air_temp_bias/nrcs_air_temp_unbias.html"

# Local cache path (relative to this file)
_CACHE_PATH = Path(__file__).parent / "_hatbc_cache.csv"


def load_hatbc_metadata(force_refresh: bool = False) -> pd.DataFrame:
    """
    Fetch (or load cached) HATBC metadata table from NRCS.

    Returns a DataFrame with at least these columns:
        station_code    – e.g. '1000_OR_SNTL'
        bias_start      – first date of known-biased data (or NaT)
        bias_end        – last date of known-biased data (or NaT / NaN = still biased)
        corrected       – bool: True if NRCS has already applied the correction

    Falls back to an empty DataFrame with the correct columns if the fetch fails.
    """
    if _CACHE_PATH.exists() and not force_refresh:
        return pd.read_csv(_CACHE_PATH, parse_dates=["bias_start", "bias_end"])

    try:
        resp = requests.get(HATBC_URL, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        if not tables:
            raise ValueError("No tables found in HATBC page")

        # The unbias table is expected to be the first/largest table.
        # Column names vary — we do best-effort parsing here.
        df = tables[0].copy()
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # Attempt to map common column name variants
        rename = {}
        for col in df.columns:
            if "station" in col and "triplet" not in col:
                rename[col] = "station_triplet"
            elif "start" in col or "begin" in col:
                rename[col] = "bias_start"
            elif "end" in col or "stop" in col:
                rename[col] = "bias_end"
            elif "correct" in col or "debias" in col:
                rename[col] = "corrected"
        df = df.rename(columns=rename)

        if "station_triplet" not in df.columns:
            logger.warning("HATBC table format unexpected; returning empty metadata")
            return _empty_hatbc()

        # Convert triplet '1000:OR:SNTL' -> station code '1000_OR_SNTL'
        df["station_code"] = df["station_triplet"].astype(str).str.replace(":", "_")

        for date_col in ("bias_start", "bias_end"):
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        if "corrected" not in df.columns:
            df["corrected"] = False

        df.to_csv(_CACHE_PATH, index=False)
        logger.info(f"HATBC metadata cached to {_CACHE_PATH} ({len(df)} rows)")
        return df

    except Exception as e:
        logger.warning(f"Could not fetch HATBC metadata: {e}")
        return _empty_hatbc()


def _empty_hatbc() -> pd.DataFrame:
    return pd.DataFrame(columns=["station_code", "bias_start", "bias_end", "corrected"])


def is_bias_period(
    station_code: str,
    dates: pd.DatetimeIndex,
    hatbc: pd.DataFrame | None = None,
) -> pd.Series:
    """
    Return a boolean Series indicating which dates are in the known-biased
    temperature period for this station.

    If HATBC metadata is unavailable or the station is not listed, assumes
    that data from 1997-10-01 onwards may be biased (conservative heuristic
    based on network-wide YSI deployment timeline).
    """
    if hatbc is None:
        hatbc = load_hatbc_metadata()

    row = hatbc[hatbc["station_code"] == station_code]

    if row.empty:
        # Heuristic: YSI extended-range sensors deployed broadly ~late 1997
        bias_start = pd.Timestamp("1997-10-01")
        bias_end = pd.NaT
        already_corrected = False
    else:
        row = row.iloc[0]
        bias_start = row.get("bias_start", pd.NaT)
        bias_end = row.get("bias_end", pd.NaT)
        already_corrected = bool(row.get("corrected", False))

    if already_corrected:
        return pd.Series(False, index=dates)

    in_period = pd.Series(True, index=dates)
    if pd.notna(bias_start):
        in_period &= dates >= bias_start
    if pd.notna(bias_end):
        in_period &= dates <= bias_end

    return in_period


# ---------------------------------------------------------------------------
# NOAA9 polynomial correction
# ---------------------------------------------------------------------------

def apply_noaa9(temp_celsius: float | np.ndarray) -> float | np.ndarray:
    """
    Apply the NOAA9 9th-order polynomial correction to biased SNOTEL
    temperature(s) in Celsius.

    Values outside [-55, 60] °C are returned as NaN (unreliable correction).
    This is the CONUS equation; Alaska requires a different set of coefficients.
    """
    scalar = np.isscalar(temp_celsius)
    arr = np.atleast_1d(np.asarray(temp_celsius, dtype=float))

    out_of_range = (arr < CORRECTION_VALID_RANGE[0]) | (arr > CORRECTION_VALID_RANGE[1])
    corrected = np.polyval(NOAA9_COEFS_CONUS[::-1], arr)  # np.polyval wants highest degree first
    corrected[out_of_range] = np.nan

    return float(corrected[0]) if scalar else corrected


def correct_station_temps(
    station_code: str,
    dates: pd.DatetimeIndex,
    temp_series: pd.Series,
    hatbc: pd.DataFrame | None = None,
) -> pd.Series:
    """
    Apply NOAA9 correction to temperature values that fall in the biased
    period for this station.  Values outside the biased period are unchanged.

    Parameters
    ----------
    station_code : str
        e.g. '1000_OR_SNTL'
    dates : pd.DatetimeIndex
        Date index aligned with temp_series.
    temp_series : pd.Series
        Temperatures in Celsius (already converted from Fahrenheit).
    hatbc : pd.DataFrame, optional
        Pre-loaded HATBC metadata (avoids repeated HTTP fetches in loops).

    Returns
    -------
    pd.Series
        Corrected temperatures in Celsius, rounded to 1 decimal place.
    """
    biased = is_bias_period(station_code, dates, hatbc)
    corrected = temp_series.copy()

    mask = biased & temp_series.notna()
    if mask.any():
        corrected[mask] = apply_noaa9(temp_series[mask].values).round(1)

    return corrected


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Apply bias correction to a station CSV and write a corrected copy.

    Usage:
        python bias_correction.py <path/to/STATION.csv> [--inplace]
    """
    import argparse

    parser = argparse.ArgumentParser(description="Apply NOAA9 air temperature bias correction")
    parser.add_argument("csv", help="Path to station CSV file")
    parser.add_argument("--inplace", action="store_true",
                        help="Overwrite the input file (default: write _corrected.csv)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    station_code = csv_path.stem  # e.g. '1000_OR_SNTL'

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    dates = df.index

    hatbc = load_hatbc_metadata()

    for col in ("TAVG", "TMIN", "TMAX"):
        if col in df.columns:
            original = df[col].copy()
            df[col] = correct_station_temps(station_code, dates, df[col], hatbc)
            n_corrected = (df[col] != original).sum()
            logger.info(f"{col}: corrected {n_corrected} value(s)")

    out_path = csv_path if args.inplace else csv_path.with_name(csv_path.stem + "_corrected.csv")
    df.to_csv(out_path)
    print(f"Written to {out_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
