import json

import requests
import pickle
import geopy.distance

import pandas as pd
import networkx as nx 
import matplotlib.pyplot as plt 
import numpy as np 
import trassenfinder_route_request
from helpers import StationPhillip
from progress.bar import Bar
from time import sleep
import re
import random

from downloader import Tor

tor = Tor()

class ConnectionThrottled(Exception):
    pass

class UnknownDs100(Exception):
    pass

def get_route(start, destination, train_type, start_mutter=False, destination_mutter=False):
    if not type(start) == str or not type(destination) == str:
        raise ValueError

    request_body = trassenfinder_route_request.standarts[train_type]
    request_body['wegpunkte'][0]['betriebsstelle']['ds100'] = start
    request_body['wegpunkte'][0]['betriebsstelle']['mutter'] = start_mutter
    request_body['wegpunkte'][1]['betriebsstelle']['ds100'] = destination
    request_body['wegpunkte'][1]['betriebsstelle']['mutter'] = destination_mutter

    while True:
        try:
            response = tor.session.post('https://openapi.trassenfinder.de/3.7.6/api/v2/infrastrukturen/1/routen/suche',
                data=json.dumps(request_body), headers={'content-type':'application/json'})

            if response.status_code == 429: # api throttle has kicked in
                raise ConnectionThrottled
            break
        except ConnectionThrottled:
            tor.new_ip()
            sleep(random.randint(0, 10))
        except:
            pass
    try:
        if response.status_code == 400:
            response_json = response.json()
            for details in response_json['details']:
                if "'" + start + "'" in details:
                    raise UnknownDs100({'ds100': start})

                if "'" + destination + "'" in details:
                    raise UnknownDs100({'ds100': destination})
            else:
                raise requests.exceptions.InvalidHeader

        if 'failure' in response.json():
            print(response.json())
            raise requests.exceptions.InvalidHeader
        response_df = pd.DataFrame(response.json()['result']['gewichtete_route']['routenpunkte'])
    except json.decoder.JSONDecodeError:
        print('error getting route from', start, 'to', destination, 'response.status_code:', response.status_code)
        raise requests.exceptions.InvalidHeader

    for i in range(len(response_df) - 1):
        for key in response_df.at[i, 'strecke_info']:
            response_df.at[i, key] = response_df.at[i, 'strecke_info'][key]
        for key in response_df.at[i, 'naechstes_streckensegment']:
            response_df.at[i, key] = response_df.at[i, 'naechstes_streckensegment'][key]
            
    return response_df.drop(columns=['wegpunkt_index', 'naechstes_streckensegment', 'technische_fahrzeit_info',
                            'haltart', 'halteplatz_sprungart', 'schiebelok_kupplungsart',
                            'verkehrshalt_trotz_fehlendem_bahnsteig', 'halteplatz_zu_kurz', 'marktsegmente',
                            'trassenpreis_euro', 'stationspreis_euro', 'zusatzkosten_euro', 'strecke_info'])

checked = np.zeros([100000, 2], dtype='U100')
index_in_checked = 0


def is_checked(station1, station2):
    global checked
    global index_in_checked

    if station1 == station2:
        return True
    thing = checked[np.logical_or(checked[:, 0] == station1, checked[:, 0] == station2)] # checked[np.isin(checked[:, 0], [station1, station2])] # 
    thing = thing[np.logical_or(thing[:, 1] == station1, thing[:, 1] == station2)] # thing[np.isin(thing[:, 1], [station1, station2])] # 

    if thing.size == 0:
        if index_in_checked >= (checked.shape[0]):
            checked = np.concatenate([checked, np.zeros([100000,2], dtype='U7')])

        checked[index_in_checked, :] = [station1, station2]
        index_in_checked += 1
        return False
    else:
        return True

# @profile
def check_connection(df):
    for i in range(len(df)):
        for u in range(i, len(df)):
            is_checked(df.at[i, 'ds100'], df.at[u, 'ds100'])

stations = StationPhillip()

# streckennetz = nx.Graph()
station_to_ds100 = pickle.load(open('station_to_ds100', 'rb'))

def get_unique_paths():
    planned_path_df = pd.read_feather('data_buffer/planned_path_df')
    paths = list(path for path in planned_path_df.iloc[:, 0].unique())
    paths.append(path for path in planned_path_df.iloc[:, 1].unique())

    del planned_path_df
    del paths[len(paths)-1]

    bar = Bar('parsing paths', max=len(paths))
    for i, path in enumerate(paths):
        bar.next()
        if path:
            path = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', ';;;', path)
            path = path.replace('{', '').replace('}', '').replace('"', '').split(',')
            path = list(station.replace(';;;', ',') for station in path)
            paths[i] = path
    bar.finish()
    pickle.dump(paths, open('paths_pickle', 'wb'))
    return paths

paths = pickle.load(open('paths_pickle', 'rb'))

def check_path_edges(path):
    bar = Bar('checking path edges', max=len(paths))
    for i, path in enumerate(paths):
        bar.next()
        if path:
            for u in range(len(path) - 1):
                is_checked(path[u], path[u + 1])
    edges = checked[0:index_in_checked]
    pickle.dump(edges, open('edges_streckennetz', 'wb'))
    return edges

# edges = pickle.load(open('edges_streckennetz', 'rb'))

def add_edges_to_streckennetz(edges, streckennetz):
    bar = Bar('adding edges', max=len(edges))
    for i in range(len(edges)):
        bar.next()
        try:
            station1 = edges[i, 0]
            location1 = stations.get_location(name=station1)
            station2 = edges[i, 1]
            location2 = stations.get_location(name=station2)
        except KeyError:
            print('no location for', station1, 'or', station2)
            continue
        try:
            distance = geopy.distance.geodesic(location1, location2).km
        except ValueError:
            print('no distance between', location1, 'and', location2)
            continue
        streckennetz.add_edges_from([(station1, station2, {'distance':distance})])
    bar.finish()
    pickle.dump(streckennetz, open('streckennetz_pickle', 'wb'))


def add_trassenfinder_info_to_streckennetz(streckennetz):
    streckennetz_ds100 = nx.Graph()
    not_found = []
    bar = Bar('checking ds100s', max=len(streckennetz.edges()))
    ecken = list(ecke for ecke in streckennetz.edges)
    for edge in ecken:
        bar.next()

        ds1001 = stations.get_ds100(name=edge[0])
        if ds1001 in not_found:
            ds1001_mutter = True
        else:
            ds1001_mutter = False

        ds1002 = stations.get_ds100(name=edge[1])
        if ds1002 in not_found:
            ds1002_mutter = True
        else:
            ds1002_mutter = False

        # if ds1001 in not_found or ds1002 in not_found:
        #     continue
        route = pd.DataFrame()
        try:
            while True:
                try:
                    route = get_route(ds1001, ds1002, 's_bahn',
                        start_mutter = ds1001_mutter, destination_mutter = ds1002_mutter)
                    break
                except UnknownDs100 as e:
                    details = e.args[0]
                    if ds1001 == details['ds100']:
                        not_found.append(ds1001)
                    elif ds1002 == details['ds100']:
                        not_found.append(ds1002)
                    break
                except ValueError:
                    break
        except requests.exceptions.InvalidHeader:
            continue

        if not route.empty:
            # add edges to graph
            for i in range(len(route) - 1):
                edge_attributes = {'length_km':(route.at[i+1, 'laufende_hm'] - route.at[i, 'laufende_hm']) / 10, 
                    'speed_kmh': route.at[i, 'geschwindigkeit_technisch_hmh'] / 10, 'comment':route.at[i, 'bemerkungen'],
                    'ausserhalb_db_netz': route.at[i, 'ausserhalb_db_netz'], 'nebenbahn': route.at[i, 'nebenbahn'],
                    'zugleitbetrieb': route.at[i, 'zugleitbetrieb'], 'sbahn': route.at[i, 'sbahn'],
                    'streckennummer': route.at[i, 'streckennummer']}
                streckennetz.add_edges_from([(route.at[i, 'von'], route.at[i, 'bis'], edge_attributes)])
            streckennetz.remove_edge(edge[0], edge[1])
    return streckennetz

# pickle.dump(not_found, open('not_found', 'wb'))

if __name__ == '__main__':
    streckennetz = pickle.load(open('streckennetz_pickle', 'rb'))
    streckennetz2 =  add_trassenfinder_info_to_streckennetz(streckennetz)
    pickle.dump(streckennetz2, open('streckennetz2_pickle', 'wb'))






# bar = Bar('gathering streckennetz', max=len(paths))
# for path_index, path in enumerate(paths): # enumerate(planned_path_df.iloc[:, 0]):
#     if path_index % 1000 == 0:
#         bar.next(1000)
#         # pickle.dump(streckennetz, open('streckennetz_pickle2', 'wb'))
#         # pickle.dump(checked, open('checked_pickle', 'wb'))
#         # pickle.dump(index_in_checked, open('index_in_checked_pickle', 'wb'))
#         # print('Saved Streckennetz and Stuff')
#     # bar.next()
#     if path:
#         path = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', ';;;', path)
#         path = path.replace('{', '').replace('}', '').replace('"', '').split(',')
#         for i in range(len(path) - 1):
#             station1 = path[i].replace(';;;', ',')
#             station2 = path[i + 1].replace(';;;', ',')
#             # try:
#             #     if station1 in station_to_ds100:
#             #         ds1001 = station_to_ds100[station1]
#             #     else:
#             #         ds1001 = stations.get_ds100(name=station1)
#             # except KeyError:
#             #     print('no ds100 for', station1)
#             #     continue
#             # try:
#             #     if station2 in station_to_ds100:
#             #         ds1002 = station_to_ds100[station2]
#             #     else:
#             #         ds1002 = stations.get_ds100(name=station2)
#             # except KeyError:
#             #     print('no ds100 for', station2)
#             #     continue
#             # if ds1001 == 'no-data' or ds1002 == 'no-data':
#             #     continue
#             is_checked(station1, station2)
#                 # streckennetz.add_edges_from([(ds1001, ds1002)]) #, edge_attributes
#             # try:
#             #     while True:
#             #         try:
#             #             df = get_route(ds1001, ds1002, 's_bahn')
#             #             break
#             #         except UnknownDs100 as e:
#             #             details = e.args[0]
#             #             if ds1001 == details['ds100']:
#             #                 ds1001 = input('ds100 for trassenfinder for ' + station1 + ': ')
#             #                 station_to_ds100[station1] = ds1001
#             #             if ds1002 == details['ds100']:
#             #                 ds1002 = input('ds100 for trassenfinder for ' + station2 + ': ')
#             #                 station_to_ds100[station2] = ds1002
#             #             pickle.dump(station_to_ds100, open('station_to_ds100', 'wb'))
#             #             if ds1001 == 'no-data' or ds1002 == 'no-data':
#             #                 raise requests.exceptions.InvalidHeader
#             # except requests.exceptions.InvalidHeader:
#             #     continue

#             # # add edges to graph
#             # for i in range(len(df) - 1):
#             #     edge_attributes = {'length_km':(df.at[i+1, 'laufende_hm'] - df.at[i, 'laufende_hm']) / 10, 
#             #         'speed_kmh': df.at[i, 'geschwindigkeit_technisch_hmh'] / 10, 'comment':df.at[i, 'bemerkungen'],
#             #         'ausserhalb_db_netz': df.at[i, 'ausserhalb_db_netz'], 'nebenbahn': df.at[i, 'nebenbahn'],
#             #         'zugleitbetrieb': df.at[i, 'zugleitbetrieb'], 'sbahn': df.at[i, 'sbahn'],
#             #         'streckennummer': df.at[i, 'streckennummer']}
#             #     streckennetz.add_edges_from([(df.at[i, 'von'], df.at[i, 'bis'], edge_attributes)])

#             # check_connection(df)

# for station1 in stations:
#     print('station1:', station1)
#     bar = Bar(station1, max=len(stations))
#     # location1 = stations.get_location(name=station1)

#     for station2 in stations.sta_list:
#         # station2 = 'Alsenz'
#         bar.next()
#         # location2 = stations.get_location(name=station2)
#         # distance = geopy.distance.vincenty(location1, location2).km
#         # if distance < 100:
#         if not is_checked(stations.get_ds100(name=station1), stations.get_ds100(name=station2)):
#             try:
#                 df = get_route(station1, station2, 's_bahn')
#             except requests.exceptions.InvalidHeader:
#                 continue
#             # add edges to graph
#             for i in range(len(df) - 1):
#                 edge_attributes = {'length_km':(df.at[i+1, 'laufende_hm'] - df.at[i, 'laufende_hm']) / 10, 
#                     'speed_kmh': df.at[i, 'geschwindigkeit_technisch_hmh'] / 10, 'comment':df.at[i, 'bemerkungen'],
#                     'ausserhalb_db_netz': df.at[i, 'ausserhalb_db_netz'], 'nebenbahn': df.at[i, 'nebenbahn'],
#                     'zugleitbetrieb': df.at[i, 'zugleitbetrieb'], 'sbahn': df.at[i, 'sbahn'],
#                     'streckennummer': df.at[i, 'streckennummer']}
#                 streckennetz.add_edges_from([(df.at[i, 'von'], df.at[i, 'bis'], edge_attributes)])

#             check_connection(df)

#     pickle.dump(streckennetz, open('streckennetz_pickle', 'wb'))
#     pickle.dump(checked, open('checked_pickle', 'wb'))

# pickle.dump(streckennetz, open('streckennetz_pickle', 'wb'))

# nx.draw(streckennetz, pos=nx.spring_layout(streckennetz))
# plt.show()
# df.to_clipboard()