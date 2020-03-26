# this script finds geo location of the stations lon and lat

import pandas as pd 
import reverse_geocoder as rg
import ast
from progress.bar import Bar

stations = pd.read_csv('stations.csv', index_col=False)
stations = stations.dropna()
coordinates = (50.7678, 6.091499)

def latlon():
    for i, station in stations.iterrows():
        #station['latlong'] = ast.literal_eval(station['latlong'])
        yield ast.literal_eval(station['latlong'])

latlonlist = list(station for station in latlon())

for i, st in enumerate(latlonlist):
    stations.at[i, 'lat'] = st[0]
    stations.at[i, 'lon'] = st[1]

geo_locations = rg.search(latlonlist)

stations = stations[[loca['cc'] == 'DE' for loca in geo_locations]]

stations.to_csv('stations.csv', index=False)