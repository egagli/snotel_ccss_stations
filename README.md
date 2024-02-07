# snotel_tools

A repository to make snotel data a little easier to handle. This repository doubles as storage of daily SNWD, WTEQ, TMIN, TMAX, TAVG, PRCPSA data in csv format with a github action setup to update daily. Functions to fetch snotel data using ulmo. Lots of the code for these tools was written by Scott Henderson and David Shean, so big shoutout to them! 


Usage: 

Pull in and view all sites by network:
all_stations = gpd.read_file('data/all-stations.geojson')
all_stations.astype(dict(beginDate=str, endDate=str)).explore(column='network',cmap='spring')

Pull in a single site:
paradise_snotel_df = pd.read_csv('')
# plot vars

Pull in WTEQ from multiple sites:
site_list = []
for site in site_list:
  df = 
plot
