import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import geopy.distance
import pandas as pd
import requests
import json
from helpers.StationPhillip import StationPhillip

if __name__ == '__main__':
    # Add missing locations to stations
    stations = StationPhillip()
    for station in stations:
        coords = stations.get_location(name=station)
        if pd.isna(coords[0]) or pd.isna(coords[1]):
            print('getting coordinates for', station)
            data = requests.get('https://marudor.de/api/hafas/v1/station/{station}'.format(station=station.replace('/', ' '))).json()
            for row in data:
                if station == row['title']:
                    stations.name_index_stations.at[station, 'lat'] = row['coordinates']['lat']
                    stations.name_index_stations.at[station, 'lon'] = row['coordinates']['lng']
                    break
            else:
                print('no location for', station)

    # Manual changes
    stations.name_index_stations = stations.name_index_stations.drop('Radolfzell Fähre', axis=0)
    stations.name_index_stations = stations.name_index_stations.drop('Romanshorn (See)', axis=0)

    stations.name_index_stations.at['Mosbach (Baden)', 'lat'] = 49.35237
    stations.name_index_stations.at['Mosbach (Baden)', 'lon'] = 9.143585

    stations.name_index_stations.at['Bahnhofsvorplatz, Aue', 'lat'] = 50.590814
    stations.name_index_stations.at['Bahnhofsvorplatz, Aue', 'lon'] = 12.698261

    stations.name_index_stations.at['Postplatz, Aue', 'lat'] = 50.587982
    stations.name_index_stations.at['Postplatz, Aue', 'lon'] = 12.700374
    df = stations.name_index_stations.reset_index()
    df = df.drop('index', axis=1)
    from database.engine import engine
    df.to_sql('stations', con=engine, if_exists='replace')
