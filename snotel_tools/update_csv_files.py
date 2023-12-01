#!/usr/bin/env python

print('starting csv update.')

import pandas as pd
import datetime
import sys
sys.path.append('../snotel_tools')
import snotel_tools
import glob
import re


today = datetime.datetime.today().strftime('%Y-%m-%d')

fns = glob.glob('data/*.csv')


for fn in fns:
    
    pattern = r"/(?P<filename>[^/.]+)\."
    sitecode = re.search(pattern,fn).group('filename')
        
    print(f'working on {sitecode}...')    

    try:

        last_time = pd.read_csv(fn,index_col=0).index[-1]
        next_time = pd.to_datetime(last_time)+datetime.timedelta(days=1)

        new_data = snotel_tools.construct_daily_dataframe(sitecode,start_date=next_time,end_date=today)

        new_data.to_csv(fn, mode='a', index=True, header=False)
        
    except:
        print(f'{sitecode} failed.')












