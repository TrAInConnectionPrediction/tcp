import os
import sys
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from database import DB_CONNECT_STRING, cached_table_fetch
import json
import datetime
from pytz import timezone
from helpers import StreckennetzSteffi, BetriebsstellenBill
import networkx as nx
import functools
import re
from tqdm import tqdm

# From http://db-livemaps.hafas.de/bin/query.exe/dn?L=vs_baustellen& page source
priorities_text = {
    2 : 'Ausfall',
    8 : 'Teilausfall',
    16 : 'Zurückhalten von Zügen',
    23 : 'Umleitung',
    24 : 'Totalsperrung',
    25 : 'Streckenruhe',
    37 : 'Fahren auf dem Gegengleis mit Zs 8 oder Befehl',
    60 : 'Umleitung unter erleichterten Bedingungen',
    63 : 'Fahrzeitverlängerung auf Regellaufweg',
    65 : 'Fahren auf dem Gegengleis mit Zs 6',
    70 : 'Sonstiges',
    80 : 'Abweichung vom Fahrplan für Zugmeldestellen',
    90 : 'Ohne Abweichung des Laufwegs',
}

class ObstacleOlly(StreckennetzSteffi):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs

        self.ds100_regex = r".*\(([A-Z\s]+)\)"
        self.betriebsstellen = BetriebsstellenBill(**kwargs)

        self.obstacles = cached_table_fetch('parsed_obstacles', **kwargs).set_index('edge_id')
        self.dict_index = {edge_id: index for index, edge_id in enumerate(self.obstacles.index)}
        self.np_obstacles = self.obstacles[['priority', 'length', 'from_time', 'to_time']].to_numpy()
        self.simpler_obstacles = cached_table_fetch('simpler_obstacles', **kwargs)

    def find_ds100(self, name):
        try:
            return re.search(self.ds100_regex, name).group(1)
        except AttributeError:
            print(name, 'did not contain ds100')
            # return some ds100 that does not exist in order to not crash
            # the programm
            return 'LASJVLKEWLSK' 

    def simpler_obstacles_to_edge_obstacles(self):
        # TODO: make this compatible with date based stations
        edge_obstacles = {
            'from_time': [],
            'to_time': [],
            'length': [],
            'edge_id': [],
            'priority': [],
            'u': [],
            'v': [],
        }
        unknown_stations = []
        for i, obstacle in tqdm(self.simpler_obstacles.iterrows(), total=len(self.simpler_obstacles)):
            try: 
                source = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['from_station']))
            except KeyError:
                try:
                    source = self.get_name(eva=int(obstacle['from_id']))
                except KeyError:
                    unknown_station = f"{obstacle['from_station']}\t{obstacle['from_id']}"
                    if unknown_station not in unknown_stations:
                        unknown_stations.append(unknown_station)
                    continue
            try:
                target = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['to_station']))
            except KeyError:
                try:
                    target = self.get_name(eva=int(obstacle['to_id']))
                except KeyError:
                    unknown_station = f"{obstacle['to_station']}\t{obstacle['to_id']}"
                    if unknown_station not in unknown_stations:
                        unknown_stations.append(unknown_station)
                    continue
            if source == target:
                # ignore these for now
                pass
            # Category:
            # category == 0: Störung
            # category == 1: Baustelle
            # category == 2: Streckenunruche (Was auch immer das heißt)
            elif obstacle['category'] == 1:
                path = self.get_edge_path_persistent_cache(source, target)
                if path is None:
                    continue
                for edge_id in path:
                    edge_obstacles['edge_id'].append(edge_id)
                    edge_obstacles['length'].append(self.streckennetz_igraph.es[edge_id]['length'])
                    edge_obstacles['u'].append(
                        self.streckennetz_igraph.vs[
                            self.streckennetz_igraph.es[edge_id].source
                        ]['name']
                    )
                    edge_obstacles['v'].append(
                        self.streckennetz_igraph.vs[
                            self.streckennetz_igraph.es[edge_id].target
                        ]['name']
                    )
                    edge_obstacles['from_time'].append(obstacle['from_time'])
                    edge_obstacles['to_time'].append(obstacle['to_time'])

                    edge_obstacles['priority'].append(obstacle['priority'])

        print('unknown stations:')
        for unknown_station in unknown_stations:
            print(unknown_station)

        self.obstacles = pd.DataFrame(edge_obstacles)
        self.obstacles = self.obstacles.set_index('edge_id')

    def hafas_obstacles_to_sql_table(self):
        obstacles = cached_table_fetch('obstacle', **self.kwargs)
        simpler_obstacles = {
            'from_time': [],
            'to_time': [],
            'from_id': [],
            'to_id': [],
            'from_station': [],
            'to_station': [],
            'dir': [],
            'type': [],
            'summary': [],
            'text': [],
            'category': [],
            'modified': [],
            'priority': [],
            'priority_text': [],
            'icon_title': [],
            'icon_type': [],
        }

        for i, obstacle in obstacles.iterrows():
            obstacle = obstacle['data']
            if 'edges' not in obstacle:
                print('no edges')
                continue

            for edge in obstacle['edges']:
                for event in obstacle['events']:
                    simpler_obstacles['from_time'].append(event['start'])
                    simpler_obstacles['to_time'].append(event['end'])

                    simpler_obstacles['from_station'].append(edge['fromLoc']['name'])
                    simpler_obstacles['to_station'].append(edge['toLoc']['name'])
                    simpler_obstacles['from_id'].append(edge['fromLoc']['id'])
                    simpler_obstacles['to_id'].append(edge['toLoc']['id'])
                    simpler_obstacles['dir'].append(edge['dir'])

                    simpler_obstacles['icon_title'].append(edge['icon']['title'])
                    simpler_obstacles['icon_type'].append(edge['icon']['type'])

                    simpler_obstacles['type'].append(obstacle['type'])
                    simpler_obstacles['summary'].append(obstacle['summary'])
                    simpler_obstacles['text'].append(obstacle['text'])
                    simpler_obstacles['category'].append(obstacle['category'])
                    simpler_obstacles['modified'].append(obstacle['modified'])
                    simpler_obstacles['priority'].append(obstacle['priority'])
                    simpler_obstacles['priority_text'].append(priorities_text[obstacle['priority']])

        self.simpler_obstacles = pd.DataFrame(simpler_obstacles)
        self.simpler_obstacles['from_time'] = pd.to_datetime(self.simpler_obstacles['from_time'])
        self.simpler_obstacles['to_time'] = pd.to_datetime(self.simpler_obstacles['to_time'])

    def parse(self):
        self.hafas_obstacles_to_sql_table()
        self.simpler_obstacles_to_edge_obstacles()

    @functools.lru_cache(maxsize=8000)
    def get_path(self, source, target):
        try:
            return nx.shortest_path(self.streckennetz, source, target, weight='length')
        except nx.exception.NetworkXNoPath:
            print('no path from', source, 'to', target)
            return
        except nx.exception.NodeNotFound:
            print(source, 'or', target, 'not found in streckennetz')
            return

    def push_to_db(self):
        self.obstacles.to_sql('parsed_obstacles', DB_CONNECT_STRING, if_exists='replace', method='multi')
        self.simpler_obstacles.to_sql('simpler_obstacles', DB_CONNECT_STRING, if_exists='replace', method='multi')

    def obstacles_of_path(self, path: List[str], time: datetime.datetime) -> dict:
        waypoints = []
        if path is not None and time is not None:
            for i in range(len(path) - 1):
                waypoints_part = self.get_edge_path_persistent_cache(path[i], path[i+1])
                if waypoints_part is not None:
                    waypoints.extend(waypoints_part)
            if len(waypoints):
                # Get the obstacles on the path
                obstacles_on_path = self.np_obstacles[[self.dict_index[waypoint] for waypoint in waypoints if waypoint in self.dict_index], :]
                # Filter for the time of the journey
                obstacles_on_path = obstacles_on_path[
                    (obstacles_on_path[:, 2] <= time)
                    & (obstacles_on_path[:, 3] >= time),
                    :3
                ]
                if obstacles_on_path.size:
                    # Group by priority and sum the length of the obstacles
                    return {
                        'priority_24': obstacles_on_path[obstacles_on_path[:, 0] == 24, 1].sum(),
                        'priority_37': obstacles_on_path[obstacles_on_path[:, 0] == 37, 1].sum(),
                        'priority_63': obstacles_on_path[obstacles_on_path[:, 0] == 63, 1].sum(),
                        'priority_65': obstacles_on_path[obstacles_on_path[:, 0] == 65, 1].sum(),
                        'priority_70': obstacles_on_path[obstacles_on_path[:, 0] == 70, 1].sum(),
                        'priority_80': obstacles_on_path[obstacles_on_path[:, 0] == 80, 1].sum(),
                    }
        return {
            'priority_24': 0.0,
            'priority_37': 0.0,
            'priority_63': 0.0,
            'priority_65': 0.0,
            'priority_70': 0.0,
            'priority_80': 0.0,
        }


def from_hafas_time(hafas_time):
    datetime.datetime.strptime(hafas_time, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone("Europe/Berlin"))


if __name__ == '__main__':
    obstacles = ObstacleOlly(prefer_cache=True)
    # obstacles.obstacles_of_path(['Tübingen Hbf', 'Stuttgart Hbf', 'Köln Hbf'], datetime.datetime(2021, 3, 18, 12))
    obstacles.parse()
    obstacles.push_to_db()
