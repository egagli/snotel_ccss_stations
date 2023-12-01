# python library for snotel functions, mostly an aggregation of awesome work and code by Scott Henderson and David Shean [https://github.com/scottyhq/snotel/blob/main/SNOTEL-stations.ipynb, https://github.com/scottyhq/snotel/blob/main/MergeMetadata.ipynb] [https://snowex-2021.hackweek.io/tutorials/geospatial/SNOTEL_query.html] 

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

wsdl_url = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL'
today = datetime.datetime.today().strftime('%Y-%m-%d')


def all_snotel_sites(from_ulmo=False):
    
    if from_ulmo == True:
        sites_dict = ulmo.cuahsi.wof.get_sites(wsdl_url)
        sites_df = pd.DataFrame.from_dict(sites_dict,orient='index').dropna().drop(columns='network').set_index('code')
        locations = pd.json_normalize(sites_df.location).set_index(sites_df.index)
        sites_df = sites_df.join(locations)
        props = pd.json_normalize(sites_df.site_property).set_index(sites_df.index)
        sites_df = sites_df.join(props[['county','state']])
        
        def comments2dict(comment):    
            comments = comment.split('|')
            keys = [x.split('=')[0] for x in comments]
            vals = [x.split('=')[1] for x in comments] 
            dictionary = dict(zip(keys,vals))
            return dictionary

        mapping = props.site_comments.apply(comments2dict)
        df3 = pd.json_normalize(mapping).set_index(sites_df.index)
        df3['HUC'] = df3['HUC'].replace('','17100204')
        sites_df = sites_df.join(df3.loc[:,['beginDate','endDate','HUC','isActive']]).drop(columns=['location','site_property'])
        
        #Note everything was parsed as strings, so change to appropriated dytes
        sites_df['endDate'] = pd.to_datetime(sites_df.endDate)
        sites_df['beginDate'] = pd.to_datetime(sites_df.beginDate)
        sites_df = sites_df.astype(dict(elevation_m=np.float32, 
                            latitude=np.float32,
                            longitude=np.float32,
                            HUC=str,
                           ))
        
        sites_df['isActive'] = sites_df.isActive.replace({'True':True, 'False':False})
        geometry = gpd.points_from_xy(sites_df.longitude, sites_df.latitude, crs='EPSG:4326')
        
        sites_gdf = gpd.GeoDataFrame(sites_df, geometry=geometry)
        
        m = mgrs.MGRS()

        def get_mgrs_square(longitude, latitude):
            c = m.toMGRS(latitude, longitude, MGRSPrecision=0)
            return c

        sites_gdf['mgrs'] = sites_gdf.apply(lambda x: get_mgrs_square(x.longitude, x.latitude), axis=1)

        #dataset = 'GMBA_Inventory_v2.0_standard.zip' # full res
        dataset = 'GMBA_Inventory_v2.0_standard_300.zip'
        url = f'https://data.earthenv.org/mountains/standard/{dataset}'
        gf_gmba = gpd.read_file('zip+'+url)

        # mountain ranges within envelop of snotel sites
        gf_gmba_crop = gf_gmba[gf_gmba.intersects(sites_gdf.unary_union.convex_hull)]

        # https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#spatial-joins
        sites_gdf['mountainRange'] = sites_gdf.sjoin(gf_gmba_crop)['MapName'] # assigns NaN if point not in a mountainRange
    
        sites_gdf = gpd.GeoDataFrame(sites_gdf.drop(columns='geometry'), geometry=geometry)
    
    else:
        sites_gdf = gpd.read_file('data/snotel-sites.geojson').set_index('code')
        
    return sites_gdf 


def sort_closest_snotel_sites(aoi_geom,print_closest=False):
    
    if isinstance(aoi_geom,shapely.geometry.base.BaseGeometry):
        aoi_gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[aoi_geom])
    elif isinstance(aoi_geom,gpd.geodataframe.GeoDataFrame):
        aoi_gdf = aoi_geom

    sites_gdf = all_snotel_sites(from_ulmo=False)

    sites_gdf_distances = sites_gdf
    sites_gdf_distances['distance_km'] = sites_gdf_distances.to_crs(32611).distance(aoi_gdf.to_crs(32611).geometry[0])/1000
    sites_gdf_distances = sites_gdf_distances.sort_values(by='distance_km')
    
    if print_closest:
        print(f'The 5 closest snotel sites are {sites_gdf_distances.head()}')
    
    return sites_gdf_distances


def snotel_fetch(sitecode, variablecode='SNWD_D', start_date='1900-01-01', end_date=today):
    values_df = None
    try:
        site_values = ulmo.cuahsi.wof.get_values(wsdl_url, 'SNOTEL:'+sitecode, 'SNOTEL:'+variablecode, start=start_date, end=end_date)
        values_df = pd.DataFrame.from_dict(site_values['values'])

        values_df['datetime'] = pd.to_datetime(values_df['datetime'], utc=True)
        values_df = values_df.set_index('datetime')
        
        values_df['value'] = pd.to_numeric(values_df['value']).replace(-9999, np.nan)
        
    except:
        raise
        print("Unable to fetch %s" % variablecode)

    return values_df


# Simply save a parquet timeseries with all values
def construct_daily_dataframe(sitecode, start_date='1900-01-01', end_date=today):
    '''write out parquet of all daily measurements'''
    station_info = ulmo.cuahsi.wof.get_site_info(wsdl_url, 'SNOTEL:'+sitecode)['series']
    daily_vars = [x for x in station_info.keys() if x.endswith('_D')]
    
    
    # # convert inches to meters
    df = snotel_fetch(sitecode, variablecode='TAVG_D', start_date=start_date, end_date=end_date)
    name = 'TAVG'
    df = df.rename(columns={'value':name})[name].to_frame()
    
    for var in ['TMIN_D','TMAX_D']:
        tmp = snotel_fetch(sitecode, variablecode=var, start_date=start_date, end_date=end_date)
        name = var[:-2]
        tmp = tmp.rename(columns={'value':name})[name]
        df = df.join(tmp)
    
    # Convert F to Celsius
    df = (df - 32) * 5/9
    
    # Add snowdepth and precip (all inches
    for var in ['SNWD_D','WTEQ_D','PRCPSA_D']: 
        tmp = snotel_fetch(sitecode, variablecode=var, start_date=start_date, end_date=end_date)
        name = var[:-2]
        tmp = tmp.rename(columns={'value':name})[name]
        tmp /= 39.3701
        df = df.join(tmp)

    # Drop UTC timestamp since all 0, and add freq='D'
    df.index = df.index.tz_localize(None).normalize()
    return df.astype('float32')






def download_snotel_data_csv(sites_gdf):
    # do not run this
    for station in tqdm.tqdm(sites_gdf.index):
        output = f'data/{station}.csv'
        if not os.path.exists(output):
            try:
                df = construct_daily_dataframe(station,start_date='1900-01-01',end_date=today)
                df.to_csv(output)
                print(f'{station} complete!')
            except:
                print(f'{station} failed :(')














