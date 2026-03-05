#!/usr/bin/env python

print('Generating live SWE % normal map...')

import html as html_lib
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

# Center on western US — tiles=None so we can add named switchable basemaps
m = folium.Map(location=[44, -113], zoom_start=5, tiles=None)

# ── Basemap layers ─────────────────────────────────────────────────────────────

folium.TileLayer(
    tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
    attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    name='CartoDB Light',
    subdomains='abcd',
    max_zoom=20,
    show=True,
).add_to(m)

folium.TileLayer(
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attr='Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
    name='ESRI Satellite',
    show=False,
).add_to(m)

folium.TileLayer(
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
    attr='Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community',
    name='ESRI Topo',
    show=False,
).add_to(m)

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

# Add Plotly.js and PapaParse to the page head
m.get_root().header.add_child(folium.Element(
    '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>\n'
    '<script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>'
))

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

    station_name_safe = html_lib.escape(str(station['name']), quote=True)

    popup_html = (
        f'<div data-station="{stationcode}" '
        f'data-csvpath="data/{stationcode}.csv" '
        f'data-stationname="{station_name_safe}">'
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
        f'<div class="swe-chart" style="width:480px;height:280px;margin-top:8px;"></div>'
        f'</div>'
    )

    folium.CircleMarker(
        location=[lat, lon],
        radius=7,
        color='black',
        weight=1,
        fill=True,
        fill_color=color,
        fill_opacity=0.85,
        popup=folium.Popup(popup_html, max_width=520),
        tooltip=f"{station['name']}: {pct:.0f}% of normal",
    ).add_to(m)
    stations_added += 1

colormap.add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

print(f'Added {stations_added} station markers to map.')

# ── Inject chart rendering JavaScript ─────────────────────────────────────────

map_name = m.get_name()

chart_js = '''<script>
window.addEventListener('load', function() {
  var mapObj = window['__MAP_NAME__'];
  if (!mapObj) return;

  var chartCache = {};

  function dateToDoWY(dateStr) {
    var d = new Date(dateStr + 'T00:00:00');
    var month = d.getMonth() + 1;
    var year = d.getFullYear();
    var wyStart = month >= 10
      ? new Date(year, 9, 1)
      : new Date(year - 1, 9, 1);
    return Math.floor((d - wyStart) / 86400000) + 1;
  }

  function computeStats(values) {
    if (!values.length) return null;
    var n = values.length;
    var mean = values.reduce(function(a, b) { return a + b; }, 0) / n;
    var variance = values.reduce(function(a, b) { return a + (b - mean) * (b - mean); }, 0) / n;
    var std = Math.sqrt(variance);
    var sorted = values.slice().sort(function(a, b) { return a - b; });
    var mid = Math.floor(sorted.length / 2);
    var median = sorted.length % 2 === 1
      ? sorted[mid]
      : (sorted[mid - 1] + sorted[mid]) / 2;
    return { mean: mean, std: std, median: median, min: sorted[0], max: sorted[sorted.length - 1] };
  }

  function renderSWEChart(stationCode, csvPath, stationName, chartDiv) {
    if (chartCache[stationCode]) {
      Plotly.newPlot(chartDiv, chartCache[stationCode].traces, chartCache[stationCode].layout,
        {responsive: true, displayModeBar: false});
      return;
    }

    chartDiv.innerHTML = '<div style="padding:20px;text-align:center;color:#666;">Loading chart...</div>';

    fetch(csvPath).then(function(resp) {
      if (!resp.ok) throw new Error('CSV not available (HTTP ' + resp.status + ')');
      return resp.text();
    }).then(function(text) {
      var parsed = Papa.parse(text, {header: true, dynamicTyping: true, skipEmptyLines: true});
      var rows = parsed.data;

      // Determine current water year start
      var today = new Date();
      var currentWYStart = today.getMonth() >= 9
        ? new Date(today.getFullYear(), 9, 1)
        : new Date(today.getFullYear() - 1, 9, 1);

      var historical = {};
      var currentWYX = [], currentWYY = [];

      rows.forEach(function(row) {
        var dateStr = row.datetime || row.date;
        if (!dateStr || row.WTEQ === null || row.WTEQ === undefined || isNaN(row.WTEQ)) return;
        var d = new Date(dateStr + 'T00:00:00');
        if (isNaN(d.getTime())) return;

        var dowy = dateToDoWY(dateStr);
        var sweVal = row.WTEQ * 100; // m → cm

        if (d >= currentWYStart) {
          currentWYX.push(dowy);
          currentWYY.push(sweVal);
        } else {
          if (!historical[dowy]) historical[dowy] = [];
          historical[dowy].push(sweVal);
        }
      });

      var dowys = [], meanArr = [], stdHighArr = [], stdLowArr = [];
      var medianArr = [], minArr = [], maxArr = [];

      for (var i = 1; i <= 366; i++) {
        var vals = historical[i] || [];
        var stats = vals.length >= 3 ? computeStats(vals) : null;
        dowys.push(i);
        meanArr.push(stats ? parseFloat(stats.mean.toFixed(3)) : null);
        stdHighArr.push(stats ? parseFloat((stats.mean + stats.std).toFixed(3)) : null);
        stdLowArr.push(stats ? parseFloat(Math.max(0, stats.mean - stats.std).toFixed(3)) : null);
        medianArr.push(stats ? parseFloat(stats.median.toFixed(3)) : null);
        minArr.push(stats ? parseFloat(stats.min.toFixed(3)) : null);
        maxArr.push(stats ? parseFloat(stats.max.toFixed(3)) : null);
      }

      var traces = [
        // mean ±1 std shaded band (filled polygon)
        {
          x: dowys.concat(dowys.slice().reverse()),
          y: stdHighArr.concat(stdLowArr.slice().reverse()),
          fill: 'toself',
          fillcolor: 'rgba(147,112,219,0.25)',
          line: {color: 'transparent'},
          name: 'mean \u00b11 std',
          type: 'scatter',
          hoverinfo: 'skip',
          showlegend: true
        },
        // min
        {
          x: dowys, y: minArr,
          mode: 'lines',
          line: {color: 'red', width: 1.5},
          name: 'min',
          type: 'scatter',
          connectgaps: false
        },
        // max
        {
          x: dowys, y: maxArr,
          mode: 'lines',
          line: {color: 'blue', width: 1.5},
          name: 'max',
          type: 'scatter',
          connectgaps: false
        },
        // mean
        {
          x: dowys, y: meanArr,
          mode: 'lines',
          line: {color: 'purple', width: 1.5},
          name: 'mean',
          type: 'scatter',
          connectgaps: false
        },
        // median
        {
          x: dowys, y: medianArr,
          mode: 'lines',
          line: {color: 'green', width: 1.5},
          name: 'median',
          type: 'scatter',
          connectgaps: false
        },
        // current water year dots
        {
          x: currentWYX,
          y: currentWYY,
          mode: 'markers',
          marker: {color: 'black', size: 5, symbol: 'circle'},
          name: 'Current WY',
          type: 'scatter'
        }
      ];

      var layout = {
        title: {text: stationName + ' \u2014 SWE by Day of Water Year', font: {size: 12}},
        xaxis: {
          title: 'Day of Water Year (Oct 1 = Day 1)',
          range: [0, 366],
          showgrid: true,
          gridcolor: '#eee'
        },
        yaxis: {
          title: 'SWE [cm]',
          rangemode: 'tozero',
          showgrid: true,
          gridcolor: '#eee'
        },
        legend: {x: 0.01, y: 0.99, bgcolor: 'rgba(255,255,255,0.8)', font: {size: 11}},
        margin: {l: 55, r: 10, t: 35, b: 45},
        height: 280,
        plot_bgcolor: 'white',
        paper_bgcolor: 'white'
      };

      chartCache[stationCode] = {traces: traces, layout: layout};
      chartDiv.innerHTML = '';
      Plotly.newPlot(chartDiv, traces, layout, {responsive: true, displayModeBar: false});

    }).catch(function(err) {
      chartDiv.innerHTML = '<div style="padding:10px;color:#c00;font-size:12px;">'
        + 'Chart unavailable: ' + err.message
        + '<br><small>Charts load when served over HTTP (e.g. GitHub Pages)</small></div>';
    });
  }

  mapObj.on('popupopen', function(e) {
    var el = e.popup.getElement();
    if (!el) return;
    var stationDiv = el.querySelector('[data-station]');
    if (!stationDiv) return;

    var stationCode = stationDiv.getAttribute('data-station');
    var csvPath = stationDiv.getAttribute('data-csvpath');
    var stationName = stationDiv.getAttribute('data-stationname');
    var chartDiv = stationDiv.querySelector('.swe-chart');

    if (!chartDiv || !stationCode || !csvPath) return;
    renderSWEChart(stationCode, csvPath, stationName, chartDiv);
  });
});
</script>'''.replace('__MAP_NAME__', map_name)

m.get_root().html.add_child(folium.Element(chart_js))

# ── Save ───────────────────────────────────────────────────────────────────────

output_path = 'live_swe_map.html'
m.save(output_path)
print(f'Map saved to {output_path}')
