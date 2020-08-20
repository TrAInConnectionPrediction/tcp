import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

import requests
import pickle
import geopy.distance

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt 
from mpl_toolkits.basemap import Basemap
import numpy as np 
# import swagger_client
import trassenfinder_route_request
from helpers.StationPhillip import StationPhillip
from helpers.BetriebsstellenBill import BetriebsstellenBill
from progress.bar import Bar
from time import sleep
import sqlalchemy
import sys

from config import db_database, db_password, db_server, db_username

betriebstellen = BetriebsstellenBill()
stations = StationPhillip()
checked = pickle.load( open( "checked_pickle", "rb" ) )
streckennetz = pickle.load( open( "streckennetz2_pickle", "rb" ) )

streckennetz.add_edge(np.nan, np.nan)

# These coordinates form the bounding box of Germany
left = 5.67
right = 15.64
bot = 47.06
top = 55.06
plt.figure(figsize=(180/2,100/2))
m = Basemap(llcrnrlon=left,llcrnrlat=bot,urcrnrlon=right,urcrnrlat=top,
             resolution='i', projection='tmerc', lat_0 = 51, lon_0 = 10)

# m.drawcoastlines(linewidth=0.72, color='black')
# m.drawcountries(zorder=0, color='black')
# m.drawrivers(color='blue')
# xpt,ypt = m(stations.station_df['lon'].to_numpy(), stations.station_df['lat'].to_numpy())
# m.scatter(x=xpt, y=ypt, marker='.')

# streckennetz.remove_edge(np.nan, np.nan)

for (n1,n2) in streckennetz.edges(data=False):
    if not n1:
        print(n1)
    att_list = [att for att in streckennetz[n1][n2]]
    for att in att_list:
        if not att == 'distance':
            streckennetz[n1][n2].pop(att, None) 

positions = {}
betriebsstellen_without_location = []
for station in streckennetz.nodes():
    if not station:
        streckennetz.remove_node(station)
        # print("removed node")
    try:
        positions[station] = m(*stations.get_location(name=station))
    except KeyError:
        try:
            positions[station] = m(*stations.get_location(ds100=station))
        except KeyError:
            try:
                positions[station] = m(*betriebstellen.get_location(ds100=station))
            except betriebstellen.NoLocationError:
                betriebsstellen_without_location.append(station)
                positions[station] = m(0.0,0.0)
            except KeyError:
                betriebsstellen_without_location.append(station)
                positions[station] = m(0.0,0.0)
    if not positions[station][0] or not positions[station][1]:
        del positions[station]

# for station in streckennetz.nodes():
#     positions[station] = m(0.0,0.0)
positions = nx.spring_layout(streckennetz, k=0.0001, pos=positions, fixed = positions.keys())

for i, edge in enumerate(streckennetz.edges(data=False)):
    if not type(edge[0]) == np.str_ and not type(edge[0]) == str:
        streckennetz.remove_edge(edge[0], edge[1])
    # for station in edge:
    #     if not type(station) == np.str_ and not type(station) == str:
    #         print(i, station)

nx.draw_networkx(streckennetz, pos=positions) #nx.spring_layout(streckennetz))

plt.show()