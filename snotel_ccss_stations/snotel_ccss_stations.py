# python library for snotel functions, mostly an aggregation of awesome work and code by Scott Henderson and David Shean [https://github.com/scottyhq/snotel/blob/main/SNOTEL-stations.ipynb, https://github.com/scottyhq/snotel/blob/main/MergeMetadata.ipynb] [https://snowex-2021.hackweek.io/tutorials/geospatial/SNOTEL_query.html] 
#built on ulmo https://ulmo.readthedocs.io/en/latest/

#mamba create -n ulmo ulmo shapely geopandas pyarrow folium ipykernel
import numpy as np
import pandas as pd
import geopandas as gpd
import ulmo
import fsspec
import datetime
import mgrs
import shapely
import os
import tqdm
import requests
import ee
from io import StringIO
import time

wsdl_url = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL'
today = datetime.datetime.today().strftime('%Y-%m-%d')


def comments2dict(comment):    
    comments = comment.split('|')
    keys = [x.split('=')[0] for x in comments]
    vals = [x.split('=')[1] for x in comments] 
    dictionary = dict(zip(keys,vals))
    return dictionary

def get_mgrs_square(longitude, latitude):
    m = mgrs.MGRS()
    c = m.toMGRS(latitude, longitude, MGRSPrecision=0)
    return c

def latlon_to_huc(geometry):
    huc12 = ee.FeatureCollection('USGS/WBD/2017/HUC12')
    point = ee.Geometry.Point([geometry.x, geometry.y])  # Longitude, Latitude
    feature = huc12.filterBounds(point).first()
    huc12_code = feature.get('huc12').getInfo()
    return huc12_code


def all_snotel_stations(from_ulmo=False):
    
    if from_ulmo == True:
        stations_dict = ulmo.cuahsi.wof.get_sites(wsdl_url)
        stations_df = pd.DataFrame.from_dict(stations_dict,orient='index').dropna().drop(columns='network').set_index('code')
        locations = pd.json_normalize(stations_df.location).set_index(stations_df.index)
        stations_df = stations_df.join(locations)
        props = pd.json_normalize(stations_df.site_property).set_index(stations_df.index)
        stations_df = stations_df.join(props[['county','state']])
        


        mapping = props.site_comments.apply(comments2dict)
        df3 = pd.json_normalize(mapping).set_index(stations_df.index)
        df3['HUC'] = df3['HUC'].replace('','17100204')
        stations_df = stations_df.join(df3.loc[:,['beginDate','endDate','HUC','isActive']]).drop(columns=['location','site_property'])
        
        #Note everything was parsed as strings, so change to appropriated dytes
        stations_df['endDate'] = pd.to_datetime(stations_df.endDate)
        stations_df['beginDate'] = pd.to_datetime(stations_df.beginDate)
        stations_df = stations_df.astype(dict(elevation_m=np.float32, 
                            latitude=np.float32,
                            longitude=np.float32,
                            HUC=str,
                           ))
        
        stations_df['isActive'] = stations_df.isActive.replace({'True':True, 'False':False})
        geometry = gpd.points_from_xy(stations_df.longitude, stations_df.latitude, crs='EPSG:4326')
        
        stations_gdf = gpd.GeoDataFrame(stations_df, geometry=geometry)
        


        stations_gdf['mgrs'] = stations_gdf.apply(lambda x: get_mgrs_square(x.longitude, x.latitude), axis=1)

        dataset = 'GMBA_Inventory_v2.0_standard_300.zip'
        url = f'https://data.earthenv.org/mountains/standard/{dataset}'
        gf_gmba = gpd.read_file('zip+'+url)

        # mountain ranges within envelop of snotel stations
        gf_gmba_crop = gf_gmba[gf_gmba.intersects(stations_gdf.unary_union.convex_hull)]

        # https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#spatial-joins
        stations_gdf['mountainRange'] = stations_gdf.sjoin(gf_gmba_crop)['MapName'] # assigns NaN if point not in a mountainRange
    
        stations_gdf = gpd.GeoDataFrame(stations_gdf.drop(columns='geometry'), geometry=geometry)
    
    else:
        stations_gdf = gpd.read_file('data/snotel-stations.geojson').set_index('code')
        
    return stations_gdf 


def sort_closest_snotel_stations(aoi_geom,print_closest=False):
    
    if isinstance(aoi_geom,shapely.geometry.base.BaseGeometry):
        aoi_gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[aoi_geom])
    elif isinstance(aoi_geom,gpd.geodataframe.GeoDataFrame):
        aoi_gdf = aoi_geom

    stations_gdf = all_snotel_stations(from_ulmo=False)

    stations_gdf_distances = stations_gdf
    stations_gdf_distances['distance_km'] = stations_gdf_distances.to_crs(32611).distance(aoi_gdf.to_crs(32611).geometry[0])/1000
    stations_gdf_distances = stations_gdf_distances.sort_values(by='distance_km')
    
    if print_closest:
        print(f'The 5 closest snotel stations are {stations_gdf_distances.head()}')
    
    return stations_gdf_distances

def get_variables(sitecode,print_vars=False):
    
    variables = ulmo.cuahsi.wof.get_site_info(wsdl_url, 'SNOTEL:'+sitecode)['series']
    
    if print_vars == True:
        for key,val in variables:
            print(key,val['variable']['name'])
    
    return variables


def snotel_fetch(stationcode, variablecode='SNWD_D', start_date='1900-01-01', end_date=today):
    values_df = None
    try:
        station_values = ulmo.cuahsi.wof.get_values(wsdl_url, 'SNOTEL:'+stationcode, 'SNOTEL:'+variablecode, start=start_date, end=end_date)
        values_df = pd.DataFrame.from_dict(station_values['values'])

        values_df['datetime'] = pd.to_datetime(values_df['datetime'], utc=True)
        values_df = values_df.set_index('datetime')
        
        values_df['value'] = pd.to_numeric(values_df['value']).replace(-9999, np.nan)
        
    except:
        raise
        print("Unable to fetch %s" % variablecode)

    return values_df


# Simply save a parquet timeseries with all values
def construct_daily_dataframe(stationcode, start_date='1900-01-01', end_date=today):
    '''write out parquet of all daily measurements'''
    station_info = ulmo.cuahsi.wof.get_site_info(wsdl_url, 'SNOTEL:'+stationcode)['series']
    daily_vars = [x for x in station_info.keys() if x.endswith('_D')]
    
    
    df = snotel_fetch(stationcode, variablecode='TAVG_D', start_date=start_date, end_date=end_date)
    name = 'TAVG'
    df = df.rename(columns={'value':name})[name].to_frame()
    
    for var in ['TMIN_D','TMAX_D']:
        tmp = snotel_fetch(stationcode, variablecode=var, start_date=start_date, end_date=end_date)
        name = var[:-2]
        tmp = tmp.rename(columns={'value':name})[name]
        df = df.join(tmp,how='outer')
    
    # Convert F to Celsius
    df = (df - 32) * 5/9
    
    # Add snowdepth and precip (all inches
    for var in ['SNWD_D','WTEQ_D','PRCPSA_D']: 
        tmp = snotel_fetch(stationcode, variablecode=var, start_date=start_date, end_date=end_date)
        name = var[:-2]
        tmp = tmp.rename(columns={'value':name})[name]
        tmp /= 39.3701
        df = df.join(tmp,how='outer')

    # Drop UTC timestamp since all 0, and add freq='D'
    df.index = df.index.tz_localize(None).normalize()
    return df.astype('float32')


def download_snotel_data_csv(stations_gdf):
    # do not run this
    for station in tqdm.tqdm(stations_gdf.index):
        output = f'data/{station}.csv'
        if not os.path.exists(output):
            try:
                df = construct_daily_dataframe(station,start_date='1900-01-01',end_date=today)
                df.to_csv(output)
                print(f'{station} complete!')
            except:
                print(f'{station} failed :(')


# CCSS functions
                
                
                
def all_ccss_stations():
# https://cdec.water.ca.gov/snow/current/snow/
# https://cdec.water.ca.gov/reportapp/javareports?name=SnowSensors
# adpated from https://github.com/rgzn/SnowSurvey/blob/master/SensorScraper.r
    csv = 'http://cdec.water.ca.gov/dynamicapp/staSearch?sta=&sensor_chk=on&sensor=18&collect=NONE+SPECIFIED&dur=&active=&lon1=&lon2=&lat1=&lat2=&elev1=-5&elev2=99000&nearby=&basin=NONE+SPECIFIED&hydro=NONE+SPECIFIED&county=NONE+SPECIFIED&agency_num=160&display=sta'
    response = requests.get(csv)
    stations_df = pd.read_html(StringIO(response.content.decode('utf-8')))[0]
    
    stations_gdf = gpd.GeoDataFrame(stations_df, geometry=gpd.points_from_xy(stations_df['Longitude'], stations_df['Latitude']))
    stations_gdf.crs = "EPSG:4326"

    stations_gdf = stations_gdf.rename(columns={'ID':'code','Station Name':'name','Latitude':'latitude','Longitude':'longitude','County':'county','Operator':'operator'})
    stations_gdf['elevation_m'] = stations_gdf['Elevation Feet'].astype(float)*0.3048
    stations_gdf['name'] = stations_gdf['name'].str.title()
    stations_gdf['county'] = stations_gdf['county'].str.title()
    stations_gdf['longitude'] = stations_gdf['longitude'].astype(float)
    stations_gdf['latitude'] = stations_gdf['latitude'].astype(float)
    stations_gdf = stations_gdf.set_index('code')
    
    stations_gdf['state'] = 'California'
    stations_gdf.loc[stations_gdf['county'] == 'State Of Nevada','state'] = 'Nevada'
    stations_gdf.loc['49M','state'] = 'Nevada'
    stations_gdf.loc['HYC','state'] = 'Nevada'
    
    stations_gdf = stations_gdf[stations_gdf['name'] != 'Snow Surveys Test Station']
    


    stations_gdf['mgrs'] = stations_gdf.apply(lambda x: get_mgrs_square(x.longitude, x.latitude), axis=1)
    stations_gdf['HUC'] = stations_gdf['geometry'].apply(latlon_to_huc) 
    
    dataset = 'GMBA_Inventory_v2.0_standard_300.zip'
    url = f'https://data.earthenv.org/mountains/standard/{dataset}'
    gf_gmba = gpd.read_file('zip+'+url)

    # mountain ranges within envelop of snotel stations
    gf_gmba_crop = gf_gmba[gf_gmba.intersects(stations_gdf.unary_union.convex_hull)]

    # https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#spatial-joins
    stations_gdf['mountainRange'] = stations_gdf.sjoin(gf_gmba_crop)['MapName'] # assigns NaN if point not in a mountainRange
    
    start_date = '1900-01-01'
    end_date = today
    snow_vars = {'SNWD':18}

    chunk_size = 200
    chunks = [stations_gdf.iloc[i:i + chunk_size] for i in range(0, len(stations_gdf), chunk_size)]

    times_df = []

    for chunk in chunks:
        params = {
            'Stations': f'{",".join(chunk.index)}',
            'SensorNums': f'18',
            'dur_code': 'D',
            'Start': f'{start_date}',
            'End': f'{end_date}'}

        response = requests.get('http://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet', params=params)
        data = pd.read_csv(StringIO(response.content.decode('utf-8')),on_bad_lines='skip')
        times_df.append(data)

    all_times_df = pd.concat(times_df)
    
    all_times_df['datetime'] = pd.to_datetime(all_times_df['DATE TIME'])
    all_times_df = all_times_df.dropna(subset='datetime')
    start_and_end_dates = all_times_df.groupby('STATION_ID')['datetime'].agg(['min','max']).rename(columns={'min':'beginDate','max':'endDate'})
    stations_gdf = stations_gdf.join(start_and_end_dates)
    
    
    stations_gdf = stations_gdf[['name','elevation_m','latitude','longitude','county','state','HUC','mgrs','mountainRange','beginDate','endDate','geometry']]
    
    
    
    return stations_gdf


def ccss_fetch(stationcode, start_date='1900-01-01', end_date=today):
    snow_vars = {'TAVG':30,'TMIN':32,'TMAX':31,'SNWD':18,'WTEQ':82,'PRCPSA':45} #https://cdec.water.ca.gov/misc/senslist.html 

    params = {
        'Stations': f'{stationcode}',
        'SensorNums': f'{",".join([str(x) for x in snow_vars.values()])}',
        'dur_code': 'D',
        'Start': f'{start_date}',
        'End': f'{end_date}'}


    response = requests.get('http://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet', params=params)
    data = pd.read_csv(StringIO(response.content.decode('utf-8')),on_bad_lines='skip')

    # Convert DATE TIME to datetime and set as index along with SENSOR_NUMBER
    data['datetime'] = pd.to_datetime(data['DATE TIME'])
    data.set_index(['datetime', 'SENSOR_NUMBER'], inplace=True)

    return data

def construct_daily_ccss_dataframe(stationcode, start_date='1900-01-01', end_date=today):
    '''write out parquet of all daily measurements'''

    # Fetch all variables at once
    df = ccss_fetch(stationcode, start_date=start_date, end_date=end_date)

    # Pivot the DataFrame to get each sensor number as a separate column
    df = df['VALUE'].unstack(level=-1)

    # Rename columns and convert units
    rename_dict = {30: 'TAVG', 32: 'TMIN', 31: 'TMAX', 18: 'SNWD', 82: 'WTEQ', 45: 'PRCPSA'}
    for sensor_num, new_name in rename_dict.items():
        if sensor_num in df.columns:
            df[new_name] = pd.to_numeric(df[sensor_num], errors='coerce')
            if new_name in ['TAVG', 'TMIN', 'TMAX']:
                df[new_name] = (df[new_name] - 32) * 5/9  # Convert F to Celsius
            elif new_name in ['SNWD', 'WTEQ', 'PRCPSA']:
                df[new_name] /= 39.3701  # Convert inches to meters
            df.drop(sensor_num, axis=1, inplace=True)
        else:
            df[new_name] = np.nan

    return df.astype('float32').dropna(how='all')


def download_ccss_data_csv(stations_gdf):
    # do not run this
    for station in tqdm.tqdm(stations_gdf.index):
        output = f'data/{station}.csv'
        if not os.path.exists(output):
            try:
                time.sleep(10)
                df = construct_daily_ccss_dataframe(station,start_date='1900-01-01',end_date=today)
                if len(df) > 10:
                    df.to_csv(output)
                    print(f'{station} complete!')
                else:
                    print(f'{station} failed, no data present in response!')
            except:
                print(f'{station} failed :(')









