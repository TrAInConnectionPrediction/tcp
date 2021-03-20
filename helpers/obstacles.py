import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from database.engine import DB_CONNECT_STRING
import json
import datetime
from pytz import timezone
from helpers.StreckennetzSteffi import StreckennetzSteffi
from helpers.BetriebsstellenBill import BetriebsstellenBill
import networkx as nx
import functools
import re
from tqdm import tqdm
from database.cached_table_fetch import cached_table_fetch


class ObstacleOlly(StreckennetzSteffi):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.ds100_regex = r".*\(([A-Z\s]+)\)"
        self.betriebsstellen = BetriebsstellenBill(**kwargs)

        self.obstacles = cached_table_fetch('parsed_obstacles', **kwargs).set_index('edge_id')
        self.np_index = self.obstacles.index.to_numpy()
        self.dict_index = {edge_id: index for index, edge_id in enumerate(self.obstacles.index)}
        self.np_obstacles = self.obstacles[['category', 'priority', 'length', 'from_time', 'to_time']].to_numpy()
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
        edge_obstacles = {
            'from_time': [],
            'to_time': [],
            'length': [],
            # 'from_edge': [],
            # 'to_edge': [],
            'edge_id': [],
            'dir': [],
            'type':[],
            'summary': [],
            'category': [],
            'modified': [],
            'priority': [],
        }
        for i, obstacle in tqdm(self.simpler_obstacles.iterrows(), total=len(self.simpler_obstacles)):
            try: 
                source = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['from_station']))
            except KeyError:
                source = self.get_name(eva=int(obstacle['from_id']))
            try:
                target = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['to_station']))
            except KeyError:
                target = self.get_name(eva=int(obstacle['to_id']))
            if source == 'unknown':
                print('unknown:', obstacle['from_station'], int(obstacle['from_id']))
                continue
                # print(self.search_station(obstacle['from_station']))
            if target == 'unknown':
                print('unknown:', obstacle['to_station'], int(obstacle['to_id']))
                continue
            if source == target:
                # ignore these for now
                pass
                # edge_obstacles['from_edge'].append(source)
                # edge_obstacles['to_edge'].append(target)
                # edge_obstacles['length'].append(0)
                # edge_obstacles['from_time'].append(obstacle['from_time'])
                # edge_obstacles['to_time'].append(obstacle['to_time'])
                # edge_obstacles['dir'].append(obstacle['dir'])
                # edge_obstacles['type'].append(obstacle['type'])

                # edge_obstacles['summary'].append(obstacle['summary'])
                # edge_obstacles['category'].append(obstacle['category'])
                # edge_obstacles['modified'].append(obstacle['modified'])
                # edge_obstacles['priority'].append(obstacle['priority'])
            else:
                # path = self.get_path(source, target)
                path = self.get_edge_path(source, target)
                if path is None:
                    continue
                for edge_id in path:
                    edge_obstacles['edge_id'].append(edge_id)
                    edge_obstacles['length'].append(self.streckennetz_igraph.es[edge_id]['length'])
                    edge_obstacles['from_time'].append(obstacle['from_time'])
                    edge_obstacles['to_time'].append(obstacle['to_time'])
                    edge_obstacles['dir'].append(obstacle['dir'])
                    edge_obstacles['type'].append(obstacle['type'])

                    edge_obstacles['summary'].append(obstacle['summary'])
                    edge_obstacles['category'].append(obstacle['category'])
                    edge_obstacles['modified'].append(obstacle['modified'])
                    edge_obstacles['priority'].append(obstacle['priority'])


                # for node_id in range(len(path) - 1):
                #     # The obstacle information is directional. We don't know thought which direction
                #     # is which, but this seems resonable to us
                #     if obstacle['dir'] == 1:
                #         edge_obstacles['from_edge'].append(path[node_id])
                #         edge_obstacles['to_edge'].append(path[node_id + 1])
                #     else:
                #         edge_obstacles['from_edge'].append(path[node_id + 1])
                #         edge_obstacles['to_edge'].append(path[node_id])

                #     edge_obstacles['length'].append(self.streckennetz[path[node_id]][path[node_id + 1]]['length'])
                #     edge_obstacles['from_time'].append(obstacle['from_time'])
                #     edge_obstacles['to_time'].append(obstacle['to_time'])
                #     edge_obstacles['dir'].append(obstacle['dir'])
                #     edge_obstacles['type'].append(obstacle['type'])

                #     edge_obstacles['summary'].append(obstacle['summary'])
                #     edge_obstacles['category'].append(obstacle['category'])
                #     edge_obstacles['modified'].append(obstacle['modified'])
                #     edge_obstacles['priority'].append(obstacle['priority'])
        self.obstacles = pd.DataFrame(edge_obstacles)
        self.obstacles = self.obstacles.set_index('edge_id')

    def hafas_obstacles_to_sql_table(self):
        obstacles = cached_table_fetch('obstacle', prefer_cache=True)
        # obstacles = pd.read_sql_table('obstacle', DB_CONNECT_STRING)
        simpler_obstacles = {
            'from_time': [],
            'to_time': [],
            'from_id': [],
            'to_id': [],
            'from_station': [],
            'to_station': [],
            'dir': [],
            'type':[],
            'summary': [],
            'category': [],
            'modified': [],
            'priority': [],
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

                    simpler_obstacles['type'].append(obstacle['type'])
                    simpler_obstacles['summary'].append(obstacle['summary'])
                    simpler_obstacles['category'].append(obstacle['category'])
                    simpler_obstacles['modified'].append(obstacle['modified'])
                    simpler_obstacles['priority'].append(obstacle['priority'])

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

    def obstacles_of_path(self, path, time):
        waypoints = []
        for i in range(len(path) - 1):
            waypoints_part = self.get_edge_path(path[i], path[i+1])
            if waypoints_part is not None:
                waypoints.extend(waypoints_part)
        if len(waypoints):
            obstacles_on_path = self.np_obstacles[[self.dict_index[waypoint] for waypoint in waypoints if waypoint in self.dict_index], :]
            obstacles_on_path = obstacles_on_path[
                (obstacles_on_path[:, 3] <= time)
                & (obstacles_on_path[:, 4] >= time),
                :3
            ]
            if obstacles_on_path.size:
                # obstacles_on_path = obstacles_on_path[['category', 'priority', 'length']].to_numpy()
                summed = obstacles_on_path.sum(axis=0)
                mean = obstacles_on_path.mean(axis=0)
                agg = {}
                agg['category_sum'] = summed[0]
                agg['priority_sum'] = summed[1]
                agg['length_sum'] = summed[2]
                agg['category_mean'] = mean[0]
                agg['priority_mean'] = mean[1]
                agg['length_mean'] = mean[2]
                agg['length_count'] = len(obstacles_on_path)
                # agg = {}
                # agg['category_sum'] = ob
                # agg = obstacles_on_path.aggregate({
                #     'category': ['sum', 'mean'],
                #     'priority': ['sum', 'mean'],
                #     'length': ['sum', 'mean', 'count'],
                # })
                return agg
            else:
                return None
        return None

def from_hafas_time(hafas_time):
    datetime.datetime.strptime(hafas_time, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone("Europe/Berlin"))

if __name__ == '__main__':
    obstacles = ObstacleOlly(prefer_cache=True)
    obstacles.obstacles_of_path(['Tübingen Hbf', 'Stuttgart Hbf', 'Köln Hbf'], datetime.datetime(2021, 3, 18, 12))
    obstacles.parse()
    obstacles.push_to_db()