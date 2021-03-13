import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from database.engine import DB_CONNECT_STRING
import json
from helpers.StreckennetzSteffi import StreckennetzSteffi
from helpers.BetriebsstellenBill import BetriebsstellenBill
import networkx as nx

# obstacles = pd.read_sql_table('obstacle', DB_CONNECT_STRING)
# simpler_obstacles = {
#     'from_time': [],
#     'to_time': [],
#     'from_id': [],
#     'to_id': [],
#     'from_station': [],
#     'to_station': [],
#     'dir': [],
#     'type':[],
#     'summary': [],
#     'category': [],
#     'modified': [],
#     'priority': [],
# }

# for i, obstacle in obstacles.iterrows():
#     print('i:\t', i)
#     # obstacle = json.load(obstacle)
#     obstacle = obstacle['data']
#     if 'edges' not in obstacle:
#         print('no edges')
#         continue
#     print('ev:\t', len(obstacle['events']))
#     print('ed:\t', len(obstacle['edges']))

#     for edge in obstacle['edges']:
#         for event in obstacle['events']:
#             simpler_obstacles['from_time'].append(event['start'])
#             simpler_obstacles['to_time'].append(event['end'])

#             simpler_obstacles['from_station'].append(edge['fromLoc']['name'])
#             simpler_obstacles['to_station'].append(edge['toLoc']['name'])
#             simpler_obstacles['from_id'].append(edge['fromLoc']['id'])
#             simpler_obstacles['to_id'].append(edge['toLoc']['id'])
#             simpler_obstacles['dir'].append(edge['dir'])

#             simpler_obstacles['type'].append(obstacle['type'])
#             simpler_obstacles['summary'].append(obstacle['summary'])
#             simpler_obstacles['category'].append(obstacle['category'])
#             simpler_obstacles['modified'].append(obstacle['modified'])
#             simpler_obstacles['priority'].append(obstacle['priority'])

# # print(obstacles)
# simpler_obstacles = pd.DataFrame(simpler_obstacles)
# simpler_obstacles.to_sql('simpler_obstacles', DB_CONNECT_STRING, if_exists='replace')

import re

class ObstacleOlly(StreckennetzSteffi):
    def __init__(self, prefer_cache):
        super().__init__(prefer_cache=prefer_cache)

        self.ds100_regex = r".*\(([A-Z\s]+)\)"
        self.betriebsstellen = BetriebsstellenBill()
        
        self.obstacles = pd.read_sql_table('simpler_obstacles', DB_CONNECT_STRING)
        self.edge_obstacles = {
            'from_time': [],
            'to_time': [],
            'length': [],
            'from_edge': [],
            'to_edge': [],
            'dir': [],
            'type':[],
            'summary': [],
            'category': [],
            'modified': [],
            'priority': [],
        }
        for i, obstacle in self.obstacles.iterrows():
            try: 
                source = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['from_station']))
            except KeyError:
                source = self.get_name(eva=int(obstacle['from_id']))
            try:
                target = self.betriebsstellen.get_name(ds100=self.find_ds100(obstacle['to_station']))
            except KeyError:
                target = self.get_name(eva=int(obstacle['to_id']))
            if source == 'unknown':
                print(obstacle['from_station'], int(obstacle['from_id']))
                # print(self.search_station(obstacle['from_station']))
            if source == 'unknown' or target == 'unknown':
                print('unknown')
                continue
            if source == target:
                self.edge_obstacles['from_edge'].append(source)
                self.edge_obstacles['to_edge'].append(target)
                self.edge_obstacles['length'].append(0)
                self.edge_obstacles['from_time'].append(obstacle['from_time'])
                self.edge_obstacles['to_time'].append(obstacle['to_time'])
                self.edge_obstacles['dir'].append(obstacle['dir'])
                self.edge_obstacles['type'].append(obstacle['type'])

                self.edge_obstacles['summary'].append(obstacle['summary'])
                self.edge_obstacles['category'].append(obstacle['category'])
                self.edge_obstacles['modified'].append(obstacle['modified'])
                self.edge_obstacles['priority'].append(obstacle['priority'])
            else:
                try:
                    path = nx.shortest_path(self.streckennetz, source, target, weight='length')
                except nx.exception.NetworkXNoPath:
                    print('no path from', source, 'to', target)
                    continue
                except nx.exception.NodeNotFound:
                    print(source, 'or', target, 'not found in streckennetz')
                    continue
                for node_id in range(len(path) - 1):
                    # The obstacle information is directional. We don't know thought which direction
                    # is which, but this seems resonable to us
                    if obstacle['dir'] == 1:
                        self.edge_obstacles['from_edge'].append(path[node_id])
                        self.edge_obstacles['to_edge'].append(path[node_id + 1])
                    else:
                        self.edge_obstacles['from_edge'].append(path[node_id + 1])
                        self.edge_obstacles['to_edge'].append(path[node_id])

                    self.edge_obstacles['length'].append(self.streckennetz[path[node_id]][path[node_id + 1]]['length'])
                    self.edge_obstacles['from_time'].append(obstacle['from_time'])
                    self.edge_obstacles['to_time'].append(obstacle['to_time'])
                    self.edge_obstacles['dir'].append(obstacle['dir'])
                    self.edge_obstacles['type'].append(obstacle['type'])

                    self.edge_obstacles['summary'].append(obstacle['summary'])
                    self.edge_obstacles['category'].append(obstacle['category'])
                    self.edge_obstacles['modified'].append(obstacle['modified'])
                    self.edge_obstacles['priority'].append(obstacle['priority'])

    def find_ds100(self, name):
        return re.search(self.ds100_regex, name).group(1)

    def obstacles_of_trip(self, start_time, end_time, path):
        pass


if __name__ == '__main__':
    obstacles = ObstacleOlly(prefer_cache=True)
    print('lol')