# Pull daily SNOTEL and CCSS station data from auto-updating CSVs  
**Eric Gagliano (egagli@uw.edu)**   
**Updated: February 19th, 2024**

A repository to make SNOTEL and CCSS station (daily) data a little easier to handle. Note that you do not need to clone or install this repo, you simply need pandas and geopandas to read data contained in this repo (or you can just download the CSVs). No confusing / hard to use APIs and download functions! Data is automatically updated daily via a github action.

## Contents

#### This repository hosts the following which are updated daily:
- a geojson of all SNOTEL and CCSS stations: [all_stations.geojson](https://github.com/egagli/snotel_ccss_stations/blob/main/all_stations.geojson)
- a data folder containing a CSV for each station with daily SNWD [m], WTEQ [m], PRCPSA [m], TMIN [C], TMAX [C], TAVG [C]: [data/](https://github.com/egagli/snotel_ccss_stations/tree/main/data)
- a compressed file containing all CSVs: [data/all_station_data.tar.lzma](https://github.com/egagli/snotel_ccss_stations/blob/main/data/all_station_data.tar.lzma)  

#### Example notebook:
- a notebook showing different ways of reading the data, as well as some example use cases: [example_usage.ipynb](https://github.com/egagli/snotel_ccss_stations/blob/main/example_usage.ipynb)

#### Though you shouldn't need to access or edit any of these, I've included a list of the utilities used and general workflow for transparency:
- an admin notebook for resetting the data in this repository: [admin_first_time_setup.ipynb](https://github.com/egagli/snotel_ccss_stations/blob/main/admin_first_time_setup.ipynb)
- a file containing all the functions to acquire and clean the data: [snotel_ccss_stations/snotel_ccss_stations.py](https://github.com/egagli/snotel_ccss_stations/blob/main/snotel_ccss_stations/snotel_ccss_stations.py)
- a script for updating the data: [snotel_ccss_stations/update_csv_files.py](https://github.com/egagli/snotel_ccss_stations/blob/main/snotel_ccss_stations/update_csv_files.py)
- a github workflow file to automatically update the data via a github action: [.github/workflows/update_csv_files.yml](https://github.com/egagli/snotel_ccss_stations/blob/main/.github/workflows/update_csv_files.yml)

## Quickstart 

### Create geodataframe of all stations
```python
all_stations_gdf = gpd.read_file('https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/all_stations.geojson').set_index('code')
all_stations_gdf = all_stations_gdf[all_stations_gdf['csvData']==True]
```

### Read station data given a station code
```python
station_id = '679_WA_SNTL'
data_df = pd.read_csv(f'https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/data/{station_id}.csv',index_col='datetime', parse_dates=True)
```

### Check out the [example_usage.ipynb](https://github.com/egagli/snotel_ccss_stations/blob/main/example_usage.ipynb) notebook for examples


<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/348e5b99-20e6-4952-9e39-0e4ceceae9e7" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/7d75b393-6bf7-47b0-adad-986217553b5b" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/5a12e7f4-c384-4890-846c-4c7796040d71" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/54c70faa-66df-4f97-bf4a-3306bfa510ff" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/50a401c8-2aec-4da7-932f-2a5b3401970a" width="1000">
   








Functions to automatically fetch and update SNOTEL data use [ulmo](https://github.com/ulmo-dev/ulmo). In this repository, I've adapted code from David Shean and Scott Henderson that utilizes ulmo to fetch SNOTEL data. Check out some of those codes here:   
- https://snowex-2021.hackweek.io/tutorials/geospatial/SNOTEL_query.html   
- https://github.com/scottyhq/snotel   


## To do list:
- Calculate some variable equivalent of PRCPSA for CCSS stations
    - seems like only accumulated precip available