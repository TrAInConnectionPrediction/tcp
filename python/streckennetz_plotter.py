import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import osmnx as ox
import networkx as nx
import shapely
import datetime
from shapely.geometry import Point, LineString, Polygon
import pandas as pd
import geopandas as gpd
from helpers.StationPhillip import StationPhillip
import matplotlib.pyplot as plt
import pickle
import matplotlib.pyplot as plt
from helpers.BetriebsstellenBill import BetriebsstellenBill
from helpers.ObstacleOlly import ObstacleOlly
from database.cached_table_fetch import cached_table_fetch


plt.style.use('dark_background')


def wkb_reverse_hexer(wbk_hex):
    return shapely.wkb.loads(wbk_hex, hex=True)


def plot_construction_work(ax):
    obstacles = ObstacleOlly(prefer_cache=True)

    rows = []
    station_obsacles = []
    for i, obstacle in obstacles.obstacles.iterrows():
        if obstacle['dir'] != 3:
            rows.append((obstacle['u'], obstacle['v'], 0))
            rows.append((obstacle['v'], obstacle['u'], 0))
        else:
            station_obsacles.append(obstacle['from_edge'])

    obstacle_edges = streckennetz.loc[streckennetz.index.isin(rows)]
    return obstacle_edges.plot(color='red', ax=ax)


def annotate_station_names(ax, names, station_gdf):
    for name, row in station_gdf.iterrows():
        if name in names:
            ax.annotate(text=name, xy=row['geometry'].coords[0])


stations = StationPhillip(prefer_cache=True)
station_gdf = stations.to_gpd()

betriebsstellen = BetriebsstellenBill(prefer_cache=True)
betriebsstellen_gdf = betriebsstellen.to_gdf()

streckennetz = cached_table_fetch('full_streckennetz', prefer_cache=True).set_index(['u', 'v', 'key'])
streckennetz['geometry'] = streckennetz['geometry'].apply(wkb_reverse_hexer)

nodes = cached_table_fetch('full_streckennetz_nodes', prefer_cache=True)
nodes['geometry'] = nodes['geometry'].apply(wkb_reverse_hexer)

streckennetz = gpd.GeoDataFrame(streckennetz, geometry='geometry')

nodes = gpd.GeoDataFrame(nodes, geometry='geometry')
station_nodes = nodes.loc[~nodes['type'].isna()]

strecke = streckennetz.plot(color='lightgrey', linewidth=0.2)
# station_nodes.plot(color='green', ax=strecke)

strecke = plot_construction_work(strecke)

plt.show()
strecke.set_aspect('equal', 'datalim')
plt.show()