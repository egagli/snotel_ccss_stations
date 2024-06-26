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

Here are some screenshots from the example notebook:


<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/c78f6965-ce32-46ee-8dd7-c1c09b6d917a" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/4e05931b-b691-4512-95f0-cf0ed8342a59" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/4f7ddea3-8c0a-4cd1-bfdb-979edcb0beb5" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/4aea21ae-0b40-421e-bfab-7b63ae71425d" width="1000">\
<img src="https://github.com/egagli/snotel_ccss_stations/assets/67975937/4f7fbcee-8b1b-411c-8956-4cab0362e683" width="1000">


## Acknowledgments

This repository builds on the work of a lot of great scientists and coders! Of note:
- The idea for this repo originated while using Scott Henderson's [snotel](https://github.com/scottyhq/snotel) repository and recognizing the value of having SNOTEL data staged somehow. I've also adapted Scott's code to create the all_stations.geojson
- I use Emilio Mayorga's [ulmo](https://github.com/ulmo-dev/ulmo) underneath the hood to fetch SNOTEL data
- I've adapated code from David Shean's [SnowEx Hackweek 2021 SNOTEL tutorial](https://snowex-2021.hackweek.io/tutorials/geospatial/SNOTEL_query.html) to use ulmo to pull SNOTEL data, and also adapted one of his plot ideas in the example notebook
- Github user rgzn for their [SnowSurvey](https://github.com/rgzn/SnowSurvey/tree/master) repo from which I took inspiration for querying the CCSS stations
- NRCS for the SNOTEL network, and CCSS for theirs


## To do list:
- Calculate some variable equivalent of PRCPSA for CCSS stations
    - seems like only accumulated precip available
