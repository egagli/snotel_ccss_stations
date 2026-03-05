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
import matplotlib.cm as mplcm
import matplotlib.colors as mcolors
from tqdm import tqdm


def datetime_to_DOWY(date):
    """Convert a datetime to Day of Water Year (Oct 1 = Day 1)."""
    if date.month >= 10:
        start_of_water_year = pd.Timestamp(year=date.year, month=10, day=1)
    else:
        start_of_water_year = pd.Timestamp(year=date.year - 1, month=10, day=1)
    return (date - start_of_water_year).days + 1


def compute_pct_normal(df, today, current_dowy, column='WTEQ', min_years=5):
    """
    Compute % of normal for the most recent reading of the given column.

    Returns (pct_normal, current_val, historical_median, data_date) or None if invalid.
    """
    if column not in df.columns:
        return None

    recent = df.loc[df.index >= today - pd.Timedelta(days=7), column].dropna()
    if recent.empty:
        return None
    current_val = recent.iloc[-1]
    data_date = recent.index[-1]

    if today.month >= 10:
        current_wy_start = pd.Timestamp(year=today.year, month=10, day=1)
    else:
        current_wy_start = pd.Timestamp(year=today.year - 1, month=10, day=1)

    historical = df.loc[df.index < current_wy_start, column].dropna()
    if historical.empty:
        return None

    historical_with_dowy = historical.to_frame()
    historical_with_dowy['DOWY'] = historical_with_dowy.index.map(datetime_to_DOWY)

    dowy_vals = historical_with_dowy.loc[
        historical_with_dowy['DOWY'] == current_dowy, column
    ].dropna()

    if dowy_vals.count() < min_years:
        return None

    historical_median = dowy_vals.median()

    if historical_median <= 0:
        return None

    pct_normal = 100.0 * current_val / historical_median

    if not (0 <= pct_normal <= 2000):
        return None

    return pct_normal, current_val, historical_median, data_date


def pct_to_color(pct):
    """Map % of normal (0–200+) to a hex color using RdBu, centered at 100%."""
    normed = min(pct, 200) / 200.0  # 0→0.0, 100→0.5, 200→1.0
    return mcolors.to_hex(mplcm.RdBu(normed))


# ── Setup ──────────────────────────────────────────────────────────────────────

today = pd.Timestamp.today().normalize()
current_dowy = datetime_to_DOWY(today)

print(f'Today: {today.date()}  |  DOWY: {current_dowy}')

all_stations_gdf = gpd.read_file('all_stations.geojson').set_index('code')

# ── Process all stations ───────────────────────────────────────────────────────

results = {}       # stationcode → dict with SWE + SNWD stats
station_info = {}  # stationcode → date range info (all CSVs)

fns = glob.glob('data/*.csv')
pattern = re.compile(r'/(?P<code>[^/.]+)\.csv$')

for fn in tqdm(fns, desc='Processing stations'):
    m = pattern.search(fn)
    if not m:
        continue
    stationcode = m.group('code')

    try:
        df = pd.read_csv(fn, index_col=0, parse_dates=True)

        # Date range for all stations that have a CSV
        valid_dates = df.index[df[['WTEQ', 'SNWD']].notna().any(axis=1)]
        date_start = valid_dates.min().strftime('%Y-%m-%d') if len(valid_dates) else None
        date_end   = valid_dates.max().strftime('%Y-%m-%d') if len(valid_dates) else None
        station_info[stationcode] = {'date_start': date_start, 'date_end': date_end}

        swe_result  = compute_pct_normal(df, today, current_dowy, column='WTEQ')
        snwd_result = compute_pct_normal(df, today, current_dowy, column='SNWD')

        entry = {}
        if swe_result:
            pct, val, med, ddate = swe_result
            entry['swe'] = {
                'pct_normal': pct,
                'current_cm': val * 100,
                'median_cm': med * 100,
                'data_date': ddate.strftime('%Y-%m-%d'),
                'color': pct_to_color(pct),
            }
        if snwd_result:
            pct, val, med, ddate = snwd_result
            entry['snwd'] = {
                'pct_normal': pct,
                'current_cm': val * 100,
                'median_cm': med * 100,
                'data_date': ddate.strftime('%Y-%m-%d'),
                'color': pct_to_color(pct),
            }
        if entry:
            results[stationcode] = entry

    except Exception as e:
        print(f'  {stationcode} failed: {e}')

print(f'Processed {len(fns)} CSVs; {len(results)} with valid data.')

# ── Build Folium map ───────────────────────────────────────────────────────────

# RdBu colormap for legend (20 samples, vmin=0, vmax=200)
_n = 20
_rdbu_colors = [mcolors.to_hex(mplcm.RdBu(i / (_n - 1))) for i in range(_n)]
colormap = cm.LinearColormap(
    colors=_rdbu_colors,
    vmin=0,
    vmax=200,
    caption='SWE % of Normal  (values above 200% shown in darkest blue)',
)

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

# ── Title overlay ──────────────────────────────────────────────────────────────

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

# Variable selector toggle (bottom-center)
toggle_html = '''
<div id="variable-selector"
     style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%);
            z-index: 1000; background: white; padding: 7px 18px;
            border: 1px solid #ccc; border-radius: 6px;
            font-family: Arial, sans-serif; font-size: 13px;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.2);">
  <b>Show:</b>&nbsp;
  <label style="cursor:pointer;">
    <input type="radio" name="mapVariable" value="swe" checked> SWE
  </label>
  &nbsp;&nbsp;
  <label style="cursor:pointer;">
    <input type="radio" name="mapVariable" value="snwd"> Snow Depth
  </label>
</div>
'''
m.get_root().html.add_child(folium.Element(toggle_html))

# CDN dependencies
m.get_root().header.add_child(folium.Element(
    '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>\n'
    '<script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>'
))

# ── Add station markers ────────────────────────────────────────────────────────

# Build JS data object in parallel with adding markers
js_station_data_parts = []

stations_with_data = 0
stations_gray = 0

for stationcode, station in all_stations_gdf.iterrows():
    lat = station.geometry.y
    lon = station.geometry.x
    station_name_safe = html_lib.escape(str(station['name']), quote=True)

    has_data = stationcode in results
    swe_data  = results[stationcode].get('swe')  if has_data else None
    snwd_data = results[stationcode].get('snwd') if has_data else None

    info = station_info.get(stationcode, {})
    date_start = info.get('date_start', 'N/A')
    date_end   = info.get('date_end', 'N/A')
    date_range_str = f"{date_start} to {date_end}" if date_start else 'No data'

    # Marker color: SWE by default, gray if no data
    fill_color = swe_data['color'] if swe_data else '#808080'
    fill_opacity = 0.85 if has_data else 0.55

    # Popup: show both SWE and SNWD info
    def fmt_row(label, d):
        if d:
            return (
                f"<b>{label}:</b> {d['current_cm']:.1f} cm "
                f"(median {d['median_cm']:.1f} cm, "
                f"<b>{d['pct_normal']:.0f}% of normal</b>)<br>"
                f"&nbsp;&nbsp;<small>data date: {d['data_date']}</small>"
            )
        return f"<b>{label}:</b> <i>insufficient data</i>"

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
        f"Record: {date_range_str}<br>"
        f"<hr style='margin:4px 0'>"
        f"{fmt_row('SWE', swe_data)}<br>"
        f"{fmt_row('Snow Depth', snwd_data)}"
        f'<div class="swe-chart" style="width:480px;height:280px;margin-top:8px;"></div>'
        f'</div>'
    )

    tooltip_text = (
        f"{station['name']}: {swe_data['pct_normal']:.0f}% of normal (SWE)"
        if swe_data else f"{station['name']}: no data"
    )

    folium.CircleMarker(
        location=[lat, lon],
        radius=7,
        color='black',
        weight=1,
        fill=True,
        fill_color=fill_color,
        fill_opacity=fill_opacity,
        popup=folium.Popup(popup_html, max_width=520),
        tooltip=tooltip_text,
    ).add_to(m)

    if has_data:
        stations_with_data += 1
    else:
        stations_gray += 1

    # Accumulate JS data for this station
    swe_js = (
        f'{{"pct":{swe_data["pct_normal"]:.1f},"color":"{swe_data["color"]}",'
        f'"tooltip":"{html_lib.escape(station["name"], quote=True)}: '
        f'{swe_data["pct_normal"]:.0f}% of normal (SWE)"}}'
        if swe_data else 'null'
    )
    snwd_js = (
        f'{{"pct":{snwd_data["pct_normal"]:.1f},"color":"{snwd_data["color"]}",'
        f'"tooltip":"{html_lib.escape(station["name"], quote=True)}: '
        f'{snwd_data["pct_normal"]:.0f}% of normal (Snow Depth)"}}'
        if snwd_data else 'null'
    )
    js_station_data_parts.append(
        f'"{stationcode}":{{"swe":{swe_js},"snwd":{snwd_js}}}'
    )

colormap.add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

print(f'Added {stations_with_data} colored + {stations_gray} gray station markers.')

# ── Inject JavaScript ──────────────────────────────────────────────────────────

map_name = m.get_name()
station_data_json = '{' + ','.join(js_station_data_parts) + '}'

chart_js = (
    '<script>\n'
    'window.stationData = ' + station_data_json + ';\n'
    '</script>\n'
    '<script>\n'
    'window.addEventListener("load", function() {\n'
    '  var mapObj = window["__MAP_NAME__"];\n'
    '  if (!mapObj) return;\n'
    '\n'
    '  // ── Build stationCode → marker map ──────────────────────────────────\n'
    '  var markerMap = {};\n'
    '  mapObj.eachLayer(function(layer) {\n'
    '    if (!(layer instanceof L.CircleMarker)) return;\n'
    '    var popup = layer.getPopup();\n'
    '    if (!popup) return;\n'
    '    var match = (popup.getContent() || "").match(/data-station="([^"]+)"/);\n'
    '    if (match) markerMap[match[1]] = layer;\n'
    '  });\n'
    '\n'
    '  // ── Variable toggle ──────────────────────────────────────────────────\n'
    '  function updateMarkers(variable) {\n'
    '    Object.keys(markerMap).forEach(function(code) {\n'
    '      var d = window.stationData[code];\n'
    '      var varData = d && d[variable];\n'
    '      var color   = varData ? varData.color   : "#808080";\n'
    '      var opacity = varData ? 0.85 : 0.55;\n'
    '      markerMap[code].setStyle({fillColor: color, fillOpacity: opacity});\n'
    '      if (varData) {\n'
    '        markerMap[code].setTooltipContent(\n'
    '          "<div>" + varData.tooltip + "</div>"\n'
    '        );\n'
    '      }\n'
    '    });\n'
    '    // Update colorbar caption\n'
    '    var caption = document.querySelector(".caption");\n'
    '    if (caption) {\n'
    '      caption.textContent = variable === "swe"\n'
    '        ? "SWE % of Normal  (values above 200% shown in darkest blue)"\n'
    '        : "Snow Depth % of Normal  (values above 200% shown in darkest blue)";\n'
    '    }\n'
    '  }\n'
    '\n'
    '  document.querySelectorAll("input[name=mapVariable]").forEach(function(radio) {\n'
    '    radio.addEventListener("change", function() {\n'
    '      if (this.checked) updateMarkers(this.value);\n'
    '    });\n'
    '  });\n'
    '\n'
    '  // ── Chart rendering ──────────────────────────────────────────────────\n'
    '  var chartCache = {};\n'
    '\n'
    '  function dateToDoWY(dateStr) {\n'
    '    var d = new Date(dateStr + "T00:00:00");\n'
    '    var month = d.getMonth() + 1;\n'
    '    var year = d.getFullYear();\n'
    '    var wyStart = month >= 10\n'
    '      ? new Date(year, 9, 1)\n'
    '      : new Date(year - 1, 9, 1);\n'
    '    return Math.floor((d - wyStart) / 86400000) + 1;\n'
    '  }\n'
    '\n'
    '  function computeStats(values) {\n'
    '    if (!values.length) return null;\n'
    '    var n = values.length;\n'
    '    var mean = values.reduce(function(a, b) { return a + b; }, 0) / n;\n'
    '    var variance = values.reduce(function(a, b) { return a + (b-mean)*(b-mean); }, 0) / n;\n'
    '    var std = Math.sqrt(variance);\n'
    '    var sorted = values.slice().sort(function(a, b) { return a - b; });\n'
    '    var mid = Math.floor(sorted.length / 2);\n'
    '    var median = sorted.length % 2 === 1\n'
    '      ? sorted[mid] : (sorted[mid-1] + sorted[mid]) / 2;\n'
    '    return {mean:mean, std:std, median:median, min:sorted[0], max:sorted[sorted.length-1]};\n'
    '  }\n'
    '\n'
    '  function renderSWEChart(stationCode, csvPath, stationName, chartDiv) {\n'
    '    var variable = document.querySelector("input[name=mapVariable]:checked").value;\n'
    '    var cacheKey = stationCode + "_" + variable;\n'
    '    if (chartCache[cacheKey]) {\n'
    '      Plotly.newPlot(chartDiv, chartCache[cacheKey].traces,\n'
    '        chartCache[cacheKey].layout, {responsive:true, displayModeBar:false});\n'
    '      return;\n'
    '    }\n'
    '    chartDiv.innerHTML = "<div style=\'padding:20px;text-align:center;color:#666;\'>Loading chart...</div>";\n'
    '    fetch(csvPath).then(function(resp) {\n'
    '      if (!resp.ok) throw new Error("CSV not available (HTTP " + resp.status + ")");\n'
    '      return resp.text();\n'
    '    }).then(function(text) {\n'
    '      var parsed = Papa.parse(text, {header:true, dynamicTyping:true, skipEmptyLines:true});\n'
    '      var rows = parsed.data;\n'
    '      var col = variable === "swe" ? "WTEQ" : "SNWD";\n'
    '      var yLabel = variable === "swe" ? "SWE [cm]" : "Snow Depth [cm]";\n'
    '      var today = new Date();\n'
    '      var currentWYStart = today.getMonth() >= 9\n'
    '        ? new Date(today.getFullYear(), 9, 1)\n'
    '        : new Date(today.getFullYear()-1, 9, 1);\n'
    '      var historical = {};\n'
    '      var currentWYX = [], currentWYY = [];\n'
    '      rows.forEach(function(row) {\n'
    '        var dateStr = row.datetime || row.date;\n'
    '        if (!dateStr || row[col] === null || row[col] === undefined || isNaN(row[col])) return;\n'
    '        var d = new Date(dateStr + "T00:00:00");\n'
    '        if (isNaN(d.getTime())) return;\n'
    '        var dowy = dateToDoWY(dateStr);\n'
    '        var val = row[col] * 100;\n'
    '        if (d >= currentWYStart) {\n'
    '          currentWYX.push(dowy); currentWYY.push(val);\n'
    '        } else {\n'
    '          if (!historical[dowy]) historical[dowy] = [];\n'
    '          historical[dowy].push(val);\n'
    '        }\n'
    '      });\n'
    '      var dowys=[],meanArr=[],stdHighArr=[],stdLowArr=[],medianArr=[],minArr=[],maxArr=[];\n'
    '      for (var i = 1; i <= 366; i++) {\n'
    '        var vals = historical[i] || [];\n'
    '        var stats = vals.length >= 3 ? computeStats(vals) : null;\n'
    '        dowys.push(i);\n'
    '        meanArr.push(stats ? parseFloat(stats.mean.toFixed(3)) : null);\n'
    '        stdHighArr.push(stats ? parseFloat((stats.mean+stats.std).toFixed(3)) : null);\n'
    '        stdLowArr.push(stats ? parseFloat(Math.max(0,stats.mean-stats.std).toFixed(3)) : null);\n'
    '        medianArr.push(stats ? parseFloat(stats.median.toFixed(3)) : null);\n'
    '        minArr.push(stats ? parseFloat(stats.min.toFixed(3)) : null);\n'
    '        maxArr.push(stats ? parseFloat(stats.max.toFixed(3)) : null);\n'
    '      }\n'
    '      var traces = [\n'
    '        {x:dowys.concat(dowys.slice().reverse()),\n'
    '         y:stdHighArr.concat(stdLowArr.slice().reverse()),\n'
    '         fill:"toself", fillcolor:"rgba(147,112,219,0.25)",\n'
    '         line:{color:"transparent"}, name:"mean \u00b11 std",\n'
    '         type:"scatter", hoverinfo:"skip", showlegend:true},\n'
    '        {x:dowys,y:minArr,mode:"lines",line:{color:"red",width:1.5},name:"min",type:"scatter",connectgaps:false},\n'
    '        {x:dowys,y:maxArr,mode:"lines",line:{color:"blue",width:1.5},name:"max",type:"scatter",connectgaps:false},\n'
    '        {x:dowys,y:meanArr,mode:"lines",line:{color:"purple",width:1.5},name:"mean",type:"scatter",connectgaps:false},\n'
    '        {x:dowys,y:medianArr,mode:"lines",line:{color:"green",width:1.5},name:"median",type:"scatter",connectgaps:false},\n'
    '        {x:currentWYX,y:currentWYY,mode:"markers",\n'
    '         marker:{color:"black",size:5,symbol:"circle"},\n'
    '         name:"Current WY",type:"scatter"}\n'
    '      ];\n'
    '      var layout = {\n'
    '        title:{text:stationName+" \u2014 "+yLabel+" by DOWY",font:{size:12}},\n'
    '        xaxis:{title:"Day of Water Year (Oct 1 = Day 1)",range:[0,366],showgrid:true,gridcolor:"#eee"},\n'
    '        yaxis:{title:yLabel,rangemode:"tozero",showgrid:true,gridcolor:"#eee"},\n'
    '        legend:{x:0.01,y:0.99,bgcolor:"rgba(255,255,255,0.8)",font:{size:11}},\n'
    '        margin:{l:55,r:10,t:35,b:45},\n'
    '        height:280,plot_bgcolor:"white",paper_bgcolor:"white"\n'
    '      };\n'
    '      chartCache[cacheKey] = {traces:traces, layout:layout};\n'
    '      chartDiv.innerHTML = "";\n'
    '      Plotly.newPlot(chartDiv, traces, layout, {responsive:true, displayModeBar:false});\n'
    '    }).catch(function(err) {\n'
    '      chartDiv.innerHTML = "<div style=\'padding:10px;color:#c00;font-size:12px;\'>"\n'
    '        + "Chart unavailable: " + err.message\n'
    '        + "<br><small>Charts load when served over HTTP (e.g. GitHub Pages)</small></div>";\n'
    '    });\n'
    '  }\n'
    '\n'
    '  mapObj.on("popupopen", function(e) {\n'
    '    var el = e.popup.getElement();\n'
    '    if (!el) return;\n'
    '    var stationDiv = el.querySelector("[data-station]");\n'
    '    if (!stationDiv) return;\n'
    '    var stationCode = stationDiv.getAttribute("data-station");\n'
    '    var csvPath     = stationDiv.getAttribute("data-csvpath");\n'
    '    var stationName = stationDiv.getAttribute("data-stationname");\n'
    '    var chartDiv    = stationDiv.querySelector(".swe-chart");\n'
    '    if (!chartDiv || !stationCode || !csvPath) return;\n'
    '    renderSWEChart(stationCode, csvPath, stationName, chartDiv);\n'
    '  });\n'
    '});\n'
    '</script>'
).replace('__MAP_NAME__', map_name)

m.get_root().html.add_child(folium.Element(chart_js))

# ── Save ───────────────────────────────────────────────────────────────────────

output_path = 'live_swe_map.html'
m.save(output_path)
print(f'Map saved to {output_path}')
