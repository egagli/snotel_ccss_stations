#!/usr/bin/env python

print('starting csv update.')

import pandas as pd
import geopandas as gpd
import datetime
import sys
sys.path.append('../snotel_tools')
import snotel_tools
import glob
import re


today = datetime.datetime.today().strftime('%Y-%m-%d')

fns = glob.glob('data/*.csv')

all_stations = gpd.read_file('all_stations.geojson')

for fn in fns:
    
    pattern = r"/(?P<filename>[^/.]+)\."
    sitecode = re.search(pattern,fn).group('filename')
        
    print(f'working on {sitecode}...')    

    try:

        last_time = pd.read_csv(fn,index_col=0).index[-1]
        next_time = pd.to_datetime(last_time)+datetime.timedelta(days=1)

        if len(sitecode) == 3:
             new_data = snotel_tools.construct_daily_ccss_dataframe(sitecode,start_date=next_time,end_date=today)
        else:
            new_data = snotel_tools.construct_daily_dataframe(sitecode,start_date=next_time,end_date=today)

        new_data.to_csv(fn, mode='a', index=True, header=False)
        
        all_stations.loc[all_stations.code == sitecode, 'endDate'] = next_time
        all_stations.to_file('all_stations.geojson')
        
        
    except:
        print(f'{sitecode} failed.')












