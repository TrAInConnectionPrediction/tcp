import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
from tqdm import tqdm
from rtd_crawler.hash64 import hash64
# from database.plan import PlanManager
# from database.change import ChangeManager
from database.rtd import RtdManager, sql_types, RtdArrays
from helpers.StreckennetzSteffi import StreckennetzSteffi
import json
import re
import concurrent.futures
import numpy as np
from database.db_manager import DBManager
import networkx as nx

empty_rtd = {key: None for key in sql_types.keys()}

# plan_db = PlanManager()
# change_db = ChangeManager()
rtd = RtdManager()
streckennetz = StreckennetzSteffi(prefer_cache=False)


# These are the names of columns that contain time information and should be parsed into a datetime
time_names = ('pt', 'ct', 'clt', 'ts')
# These are parts of message that might be interesting to us
message_parts_to_parse = ('id', 't', 'c', 'ts')


def db_to_datetime(dt) -> datetime.datetime:
    """
    Convert bahn time in format: '%y%m%d%H%M' to datetime.
    As it it fastest to directly construct a datetime object from this, no strptime is used.

    Args:
        dt (str): bahn time format

    Returns:
        datetime.datetime: converted bahn time
    """
    return datetime.datetime(int('20' + dt[0:2]), int(dt[2:4]), int(dt[4:6]), int(dt[6:8]), int(dt[8:10]))


def parse_stop_plan(stop: dict) -> dict:
    """
    Parse a planned stop: Add index and flatten the arrival and departure events.
    Parameters
    ----------
    stop : dict
        Stop from the Timetables API

    Returns
    -------
    dict
        Parsed Stop
    """
    # Create a int64 hash to be used as index.
    stop['hash_id'] = hash64(stop['id'])

    # Split id into the three id parts: the id unique on the date, the date, the stop number
    id_parts = re.split(r'(?<=\d)(-)(?=\d)', stop['id'])
    stop['dayly_id'] = int(id_parts[0])
    stop['date_id'] = db_to_datetime(id_parts[2])
    stop['stop_id'] = int(id_parts[4])
    if 'tl' in stop:
        for key in stop['tl'][0]:
            stop[key] = stop['tl'][0][key]
        stop.pop('tl')
    if 'ar' in stop:
        for key in stop['ar'][0]:
            if key in time_names:
                stop['ar_' + key] = db_to_datetime(stop['ar'][0][key])
            else:
                stop['ar_' + key] = stop['ar'][0][key]
        stop.pop('ar')
    if 'dp' in stop:
        for key in stop['dp'][0]:
            if key in time_names:
                stop['dp_' + key] = db_to_datetime(stop['dp'][0][key])
            else:
                stop['dp_' + key] = stop['dp'][0][key]
        stop.pop('dp')
    return stop


def add_change_to_stop(stop: dict, change: dict) -> dict:
    """
    Add realtime changes to a stop.

    Parameters
    ----------
    stop : dict
        A parsed stop from parse_plan_stop().
    change : dict
        A stop from the timetables API with realtime changes.

    Returns
    -------
    dict
        The stop with realtime changes added to it.
    """
    # Add arrival changes
    if 'ar' in change:
        for key in change['ar'][0]:
            if key == 'm':
                for msg in change['ar'][0][key]:
                    for msg_part in msg:
                        if msg_part in message_parts_to_parse:
                            if 'ar_m_' + msg_part not in stop:
                                stop['ar_m_' + msg_part] = []
                            if msg_part in time_names:
                                stop['ar_m_' + msg_part].append(db_to_datetime(msg[msg_part]))
                            elif msg_part == 'c':
                                stop['ar_m_c'].append(int(msg[msg_part]))
                            else:
                                stop['ar_m_' + msg_part].append(msg[msg_part])
            else:
                if key in time_names:
                    stop['ar_' + key] = db_to_datetime(change['ar'][0][key])
                else:
                    stop['ar_' + key] = change['ar'][0][key]

    # Add departure changes
    if 'dp' in change:
        for key in change['dp'][0]:
            if key == 'm':
                for msg in change['dp'][0][key]:
                    for msg_part in msg:
                        if msg_part in message_parts_to_parse:
                            if 'dp_m_' + msg_part not in stop:
                                stop['dp_m_' + msg_part] = []
                            if msg_part in time_names:
                                stop['dp_m_' + msg_part].append(db_to_datetime(msg[msg_part]))
                            elif msg_part == 'c':
                                stop['dp_m_c'].append(int(msg[msg_part]))
                            else:
                                stop['dp_m_' + msg_part].append(msg[msg_part])
            else:
                if key in time_names:
                    stop['dp_' + key] = db_to_datetime(change['dp'][0][key])
                else:
                    stop['dp_' + key] = change['dp'][0][key]
    # Add message that is associated with the hole stop.
    if 'm' in change:
        for msg in change['m']:
            for msg_part in msg:
                if msg_part in message_parts_to_parse:
                    if 'm_' + msg_part not in stop:
                        stop['m_' + msg_part] = []
                    if msg_part in time_names:
                        stop['m_' + msg_part].append(db_to_datetime(msg[msg_part]))
                    elif msg_part == 'c':
                        stop['m_c'].append(int(msg[msg_part]))
                    else:
                        stop['m_' + msg_part].append(msg[msg_part])
    return stop    


def add_distance(rtd):
    for prefix in ('ar', 'dp'):
        if prefix + '_ct' in rtd.columns:
            rtd[prefix + '_ct'] = rtd[prefix + '_ct'].fillna(value=rtd[prefix + '_pt'])
            # no_ct = rtd[prefix + '_ct'].isna()
            # rtd.loc[no_ct, prefix + '_ct'] = rtd.loc[no_ct, prefix + '_pt']
        elif prefix + '_pt' in rtd.columns:
            rtd[prefix + '_ct'] = rtd[prefix + '_pt']
        else:
            rtd[prefix + '_pt'] = pd.NaT
            rtd[prefix + '_ct'] = pd.NaT

        if prefix + '_cpth' in rtd.columns:
            rtd[prefix + '_cpth'] = rtd[prefix + '_cpth'].fillna(value=rtd[prefix + '_ppth'])
            # no_cpth = rtd[prefix + '_cpth'].isna()
            # rtd.loc[no_cpth, prefix + '_cpth'] = rtd.loc[no_cpth, prefix + '_ppth']
        elif prefix + '_ppth' in rtd.columns:
            rtd[prefix + '_cpth'] = rtd[prefix + '_ppth']
        else:
            rtd[prefix + '_ppth'] = ''
            rtd[prefix + '_cpth'] = ''

        if prefix + '_cp' in rtd.columns:
            rtd[prefix + '_cp'] = rtd[prefix + '_cp'].fillna(value=rtd[prefix + '_pp'])
            # no_cp = rtd[prefix + '_cp'].isna()
            # rtd.loc[no_cp, prefix + '_cp'] = rtd.loc[no_cp, prefix + '_pp']
        elif prefix + '_pp' in rtd.columns:
            rtd[prefix + '_cp'] = rtd[prefix + '_pp']
        else:
            rtd[prefix + '_pp'] = ''
            rtd[prefix + '_cp'] = ''

    for col in ['ar_ppth', 'ar_cpth', 'dp_ppth', 'dp_cpth']:
        if col in rtd.columns:
            rtd[col] = rtd[col].astype('str').replace('nan', np.nan)
            rtd[col] = rtd[col].str.split('|')

    rtd['category_sum'] = 0
    rtd['category_avg'] = 0
    rtd['priority_sum'] = 0
    rtd['priority_avg'] = 0
    rtd['total_length'] = 0
    rtd['obstacle_no'] = 0

    prev_dp_ct = None
    print(len(rtd))
    for i, row in rtd.iterrows():
        ar_cpth = row['ar_cpth']
        if isinstance(ar_cpth, list):
            rtd.at[i, 'distance_to_last'] = streckennetz.route_length([ar_cpth[-1]] + [row['station']])
            rtd.at[i, 'distance_to_start'] = streckennetz.route_length(ar_cpth + [row['station']])
            waypoints = []
            prev_bhf = None
            if prev_dp_ct:
                for bhf in ar_cpth:
                    if prev_bhf:
                        try:
                            new_waypoints = nx.shortest_path(streckennetz.streckennetz, prev_bhf, bhf, weight='length')
                            # waypoints = [*waypoints, *new_waypoints]
                            prev_waypoint = None
                            for waypoint in new_waypoints:
                                if waypoint == "Berlin-Charlottenburg":
                                    print(waypoint)
                                if prev_waypoint and prev_waypoint in obstacle['from_edge']:
                                    ob = obstacle.loc[obstacle['from_edge'] == prev_waypoint & obstacle['to_edge'] == waypoint & obstacle['from_time'] < prev_dp_ct  & obstacle['to_time'] > row['ar_ct']]
                                    if ob:
                                        rtd.at[i, 'category_sum'] += ob['category']
                                        rtd.at[i, 'priority_sum'] += ob['priority']
                                        rtd.at[i, 'total_length'] += ob['length']
                                        rtd.at[i, 'obstacle_no'] += 1
                                prev_waypoint = waypoint        
                        except (nx.exception.NodeNotFound, nx.exception.NetworkXNoPath) as e:
                            print(str(type(e)) + " due to " + str(e))
                        
                    prev_bhf = bhf
            prev_dp_ct = row['dp_ct']
            
        else:
            rtd.at[i, 'distance_to_last'] = 0
            rtd.at[i, 'distance_to_start'] = 0

        dp_cpth = row['dp_cpth']
        if isinstance(dp_cpth, list):
            rtd.at[i, 'distance_to_next'] = streckennetz.route_length([row['station']] + [dp_cpth[0]])
            rtd.at[i, 'distance_to_end'] = streckennetz.route_length([row['station']] + dp_cpth)
        else:
            rtd.at[i, 'distance_to_next'] = 0
            rtd.at[i, 'distance_to_end'] = 0

    return rtd


def parse_timetable(timetables, db):
    parsed = []
    timetables = [timetable.plan for timetable in timetables]
    train_ids_to_get = []
    for timetable in timetables:
        train_ids_to_get.extend(timetable.keys())
    changes = db.get_changes(train_ids_to_get)
    changes = {change.hash_id: json.loads(change.change) for change in changes}
    for timetable in timetables:
        if timetable is None:
            continue
        for stop in timetable.values():
            stop = parse_stop_plan(stop)

            if stop['hash_id'] in changes:
                stop = add_change_to_stop(stop, changes[stop['hash_id']])
            if stop:
                parsed.append(stop)
    return parsed


def parse_station(station, start_date, end_date):
    with DBManager() as db:
        stations_timetables = db.plan_of_station(station, date1=start_date, date2=end_date)
        parsed = parse_timetable(stations_timetables, db)

    if parsed:
        parsed = pd.DataFrame(parsed)
        parsed = parsed.set_index('hash_id')
        # Remove duplicates. Duplicates may happen if a stop is shifted to the next hour due to delays.
        # It than reappears in the planned timetable of the next hour.
        parsed = parsed.loc[~parsed.index.duplicated(keep='last')]
        parsed['station'] = station
        parsed = add_distance(parsed)
        current_array_cols = [col for col in RtdArrays.__table__.columns.keys() if col in parsed.columns]
        # There are many columns that contain arrays. These take up munch space and aren't
        # used after parsing, so we currently don't store them in the database
        # rtd_arrays_df = parsed.loc[:, current_array_cols]
        # rtd.upsert_arrays(rtd_arrays_df)
        # rtd_df = parsed.drop(current_array_cols, axis=1)
        # db = DBManager()
        # db.upsert_rtd(rtd_df)

    return True

def parse(only_new=True):
    if only_new:
        start_date = rtd.max_date() - datetime.timedelta(days=2)
    else:
        start_date = datetime.datetime(2020, 10, 1, 0, 0)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(parse_station, station, start_date, end_date): station
                   for station
                   in streckennetz}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(streckennetz)):
            future.result()


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    obstacle = pd.read_csv('cache/obstacle.csv', sep='\t')

    start_date = datetime.datetime(2021, 3, 2, 0, 0)
    end_date = datetime.datetime(2021, 3, 6, 0, 0)

    parse_station('Berlin-Charlottenburg', start_date, end_date)

    # parse(only_new=input('Do you wish to only parse new data? ([y]/n)') == 'n')

    # if input('Do you wish to only parse new data? ([y]/n)') == 'n':
    #     start_date = datetime.datetime(2020, 10, 1, 0, 0)
    # else:
    #     start_date = rtd.max_date() - datetime.timedelta(days=2)

    # end_date = datetime.datetime.now() - datetime.timedelta(hours=10)

    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     futures = {executor.submit(parse_station, station, start_date, end_date): station
    #                for station
    #                in streckennetz}
    #     for future in tqdm(concurrent.futures.as_completed(futures), total=len(streckennetz)):
    #         future.result()