# Pull daily SNOTEL and CCSS station data from auto-updating CSVs  
**Eric Gagliano (egagli@uw.edu)**   
**Updated: February 8th, 2024**

A repository to make SNOTEL and CCSS station (daily) data a little easier to handle. Note that you do not need to clone or install this repo, you simply need pandas and geopandas to pull data contained in this repo. No confusing / hard to use APIs and download functions!

This repository hosts:
- a geojson of all SNOTEL and CCSS stations [all_stations.geojson](https://github.com/egagli/snotel_ccss_stations/blob/main/all_stations.geojson)
- a data folder containing a CSV for each station with daily SNWD [m], WTEQ [m], PRCPSA [m], TMIN [C], TMAX [C], TAVG [C] (data automatically updated daily via a github action) 
- an example notebook [example_usage.ipynb](https://github.com/egagli/snotel_ccss_stations/blob/main/example_usage.ipynb)

## Quickstart 

### Create geodataframe of all stations
```python
all_stations_gdf = gpd.read_file('https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/all_stations.geojson').set_index('code')
```

### Read station data given a station code
```python
station_id = '679_WA_SNTL'
data_df = pd.read_csv(f'https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/data/{station_id}.csv',index_col='datetime', parse_dates=True)
```

### Check out the [example_usage.ipynb](https://github.com/egagli/snotel_ccss_stations/blob/main/example_usage.ipynb) notebook for examples


![p1](https://github.com/egagli/snotel_ccss_stations/assets/67975937/348e5b99-20e6-4952-9e39-0e4ceceae9e7)
![p2](https://github.com/egagli/snotel_ccss_stations/assets/67975937/7d75b393-6bf7-47b0-adad-986217553b5b)
![p3](https://github.com/egagli/snotel_ccss_stations/assets/67975937/5a12e7f4-c384-4890-846c-4c7796040d71)
![p4](https://github.com/egagli/snotel_ccss_stations/assets/67975937/54c70faa-66df-4f97-bf4a-3306bfa510ff)
![p5](https://github.com/egagli/snotel_ccss_stations/assets/67975937/473bbd45-3e98-41b3-b9ab-dbf329ef3841)











Functions to automatically fetch and update SNOTEL data use [ulmo](https://github.com/ulmo-dev/ulmo). In this repository, I've adapted code from David Shean and Scott Henderson that utilizes ulmo to fetch SNOTEL data. Check out some of those codes out here:   
- https://snowex-2021.hackweek.io/tutorials/geospatial/SNOTEL_query.html   
- https://github.com/scottyhq/snotel   
