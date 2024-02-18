#!/usr/bin/env python

print('starting csv update.')

import pandas as pd
import geopandas as gpd
import datetime
import sys
sys.path.append('../snotel_ccss_stations')
import snotel_ccss_stations
import glob
import re


today = datetime.datetime.today().strftime('%Y-%m-%d')

fns = glob.glob('data/*.csv')


for fn in fns:
    
    all_stations_gdf = gpd.read_file('all_stations.geojson')

    pattern = r"/(?P<filename>[^/.]+)\."
    stationcode = re.search(pattern,fn).group('filename')
        
    print(f'working on {stationcode}...')    

    try:

        existing_data = pd.read_csv(fn, index_col=0)
        
        last_time = existing_data.index[-3]
        next_time = pd.to_datetime(last_time)+datetime.timedelta(days=1)

        if len(stationcode) == 3:
            new_data = snotel_ccss_stations.construct_daily_ccss_dataframe(stationcode,start_date=next_time,end_date=today)
        else:
            new_data = snotel_ccss_stations.construct_daily_dataframe(stationcode,start_date=next_time,end_date=today)

        # Append the new data to the existing data
        combined_data = existing_data.append(new_data)

        # Drop any duplicate rows
        combined_data = combined_data.drop_duplicates(keep='last')    

        # Write the combined data back to the CSV
        combined_data.to_csv(fn, index=True, header=False)
        #new_data.to_csv(fn, mode='a', index=True, header=False)
        
        all_stations_gdf.loc[all_stations_gdf.code == stationcode, 'endDate'] = next_time
        all_stations_gdf.to_file('all_stations.geojson')
        
        
    except:
        print(f'{stationcode} failed.')












