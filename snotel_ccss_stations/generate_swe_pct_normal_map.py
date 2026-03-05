#!/usr/bin/env python

print('Generating live SWE % normal map...')

import pandas as pd
import geopandas as gpd
import datetime
import glob
import re
import os
import folium
import branca.colormap as cm
import numpy as np
from tqdm import tqdm


def datetime_to_DOWY(date):
    """Convert a datetime to Day of Water Year (Oct 1 = Day 1)."""
    if date.month >= 10:
        start_of_water_year = pd.Timestamp(year=date.year, month=10, day=1)
    else:
        start_of_water_year = pd.Timestamp(year=date.year - 1, month=10, day=1)
    return (date - start_of_water_year).days + 1


def compute_pct_normal(df, today, current_dowy, min_years=5):
    """
    Compute % of normal SWE for the most recent reading.

    Returns (pct_normal, current_swe_m, historical_median_m, data_date) or None if invalid.
    """
    if 'WTEQ' not in df.columns:
        return None

    # Get most recent valid WTEQ reading within last 7 days
    recent = df.loc[df.index >= today - pd.Timedelta(days=7), 'WTEQ'].dropna()
    if recent.empty:
        return None
    current_swe = recent.iloc[-1]
    data_date = recent.index[-1]

    # Historical baseline: exclude current water year
    if today.month >= 10:
        current_wy_start = pd.Timestamp(year=today.year, month=10, day=1)
    else:
        current_wy_start = pd.Timestamp(year=today.year - 1, month=10, day=1)

    historical = df.loc[df.index < current_wy_start, 'WTEQ'].dropna()
    if historical.empty:
        return None

    # Add DOWY to historical data
    historical_with_dowy = historical.to_frame()
    historical_with_dowy['DOWY'] = historical_with_dowy.index.map(datetime_to_DOWY)

    dowy_vals = historical_with_dowy.loc[
        historical_with_dowy['DOWY'] == current_dowy, 'WTEQ'
    ].dropna()

    if dowy_vals.count() < min_years:
        return None

    historical_median = dowy_vals.median()

    # Skip if historical median is zero (off-season or no-snow station at this time of year)
    if historical_median <= 0:
        return None

    pct_normal = 100.0 * current_swe / historical_median

    if not (0 <= pct_normal <= 2000):
        return None

    return pct_normal, current_swe, historical_median, data_date


# ── Setup ──────────────────────────────────────────────────────────────────────

today = pd.Timestamp.today().normalize()
current_dowy = datetime_to_DOWY(today)

print(f'Today: {today.date()}  |  DOWY: {current_dowy}')

# Load station metadata
all_stations_gdf = gpd.read_file('all_stations.geojson').set_index('code')

# ── Compute % normal for each station ─────────────────────────────────────────

results = {}
fns = glob.glob('data/*.csv')
pattern = re.compile(r'/(?P<code>[^/.]+)\.csv$')

for fn in tqdm(fns, desc='Processing stations'):
    m = pattern.search(fn)
    if not m:
        continue
    stationcode = m.group('code')

    try:
        df = pd.read_csv(fn, index_col=0, parse_dates=True)
        result = compute_pct_normal(df, today, current_dowy)
        if result is not None:
            pct_normal, current_swe, historical_median, data_date = result
            results[stationcode] = {
                'pct_normal': pct_normal,
                'current_swe_m': current_swe,
                'historical_median_m': historical_median,
                'data_date': data_date,
            }
    except Exception as e:
        print(f'  {stationcode} failed: {e}')

print(f'Successfully computed % normal for {len(results)} stations.')

# ── Build Folium map ───────────────────────────────────────────────────────────

colormap = cm.LinearColormap(
    colors=['#8B0000', '#FF4500', '#FFA500', '#FFFF80', '#FFFFFF', '#ADD8E6', '#4169E1', '#00008B'],
    vmin=0,
    vmax=200,
    caption='SWE % of Normal  (values above 200% shown in darkest blue)',
)

# Center on western US
m = folium.Map(location=[44, -113], zoom_start=5, tiles='CartoDB positron')

# Title overlay
title_html = f'''
<div style="position: fixed; top: 12px; left: 50%; transform: translateX(-50%);
            z-index: 1000; background: white; padding: 8px 16px;
            border: 1px solid #ccc; border-radius: 6px;
            font-family: Arial, sans-serif; font-size: 14px; font-weight: bold;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.2);">
    SNOTEL &amp; CCSS Stations — Current SWE % of Normal
    <span style="font-weight: normal; color: #666;">&nbsp;(generated {today.strftime("%Y-%m-%d")})</span>
</div>
'''
m.get_root().html.add_child(folium.Element(title_html))

stations_added = 0
for stationcode, data in results.items():
    if stationcode not in all_stations_gdf.index:
        continue

    station = all_stations_gdf.loc[stationcode]
    lat = station.geometry.y
    lon = station.geometry.x

    pct = data['pct_normal']
    color_val = min(pct, 200)  # cap for colormap
    color = colormap(color_val)

    current_swe_cm = data['current_swe_m'] * 100
    median_swe_cm = data['historical_median_m'] * 100
    data_date_str = data['data_date'].strftime('%Y-%m-%d')

    popup_html = (
        f"<b>{station['name']}</b><br>"
        f"Code: {stationcode}<br>"
        f"Network: {station['network']}<br>"
        f"State: {station.get('state', 'N/A')}<br>"
        f"Elevation: {station['elevation_m']:.0f} m<br>"
        f"Mountain Range: {station.get('mountainRange', 'N/A')}<br>"
        f"<hr style='margin:4px 0'>"
        f"Data date: {data_date_str}<br>"
        f"Current SWE: {current_swe_cm:.1f} cm<br>"
        f"Historical median SWE: {median_swe_cm:.1f} cm<br>"
        f"<b>% of Normal: {pct:.0f}%</b>"
    )

    folium.CircleMarker(
        location=[lat, lon],
        radius=7,
        color='black',
        weight=1,
        fill=True,
        fill_color=color,
        fill_opacity=0.85,
        popup=folium.Popup(popup_html, max_width=270),
        tooltip=f"{station['name']}: {pct:.0f}% of normal",
    ).add_to(m)
    stations_added += 1

colormap.add_to(m)

print(f'Added {stations_added} station markers to map.')

# ── Save ───────────────────────────────────────────────────────────────────────

output_path = 'live_swe_map.html'
m.save(output_path)
print(f'Map saved to {output_path}')
