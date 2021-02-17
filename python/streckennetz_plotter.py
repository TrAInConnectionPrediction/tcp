import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import osmnx as ox
import networkx as nx
import shapely
from shapely.geometry import Point, LineString, Polygon
import numpy as np
import geopy.distance
import itertools
import pandas as pd
import geopandas as gpd
from helpers.StationPhillip import StationPhillip
import matplotlib.pyplot as plt
import pickle
from concurrent.futures import ProcessPoolExecutor
from database.engine import engine
import matplotlib.pyplot as plt

stations = StationPhillip()
station_gdf = stations.get_geopandas()
station_gdf = station_gdf.set_index('name')

streckennetz = pd.read_sql_table('full_streckennetz', con=engine).set_index(['u', 'v', 'key'])
streckennetz_nodes = pd.read_sql_table('full_streckennetz_nodes', con=engine)

def wkb_reverse_hexer(wbk_hex):
    return shapely.wkb.loads(wbk_hex, hex=True)


streckennetz['geometry'] = streckennetz['geometry'].apply(wkb_reverse_hexer)
streckennetz.to_pickle('cache/full_streckennetz.pkl')

streckennetz_nodes['geometry'] = streckennetz_nodes['geometry'].apply(wkb_reverse_hexer)
streckennetz_nodes.to_pickle('cache/full_streckennetz_nodes.pkl')
print('saved cache')

streckennetz = pd.read_pickle('cache/full_streckennetz.pkl')
streckennetz = gpd.GeoDataFrame(streckennetz, geometry='geometry')

streckennetz_nodes = pd.read_pickle('cache/full_streckennetz_nodes.pkl')
streckennetz_nodes = gpd.GeoDataFrame(streckennetz_nodes, geometry='geometry')
streckennetz_graph = ox.graph_from_gdfs(streckennetz_nodes, streckennetz)

strecke = streckennetz.plot()
station_gdf = station_gdf.loc[[ 'Niederschlag', 'Kretscham-Rothensehma'], :]
station_gdf.plot(ax=strecke, marker='o', color='red', markersize=5)

for name, row in station_gdf.iterrows():
    plt.annotate(text=name, xy=row['geometry'].coords[0])
plt.show()

# Test functionality
path = ox.shortest_path(streckennetz_graph, 'Niederschlag', 'Kretscham-Rothensehma', weight='length')
print(nx.shortest_path_length(streckennetz_graph, 'Niederschlag', 'Kretscham-Rothensehma', weight='length'))
ox.plot_graph_route(streckennetz_graph, path)

# streckennetz.to_sql('full_streckennetz', if_exists='replace', method='multi', con=engine)