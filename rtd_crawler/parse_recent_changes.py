import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
from tqdm import tqdm
from rtd_crawler.hash64 import hash64
from database.rtd import RtdManager, sql_types, RtdArrays
from helpers.obstacles import ObstacleOlly
import json
import re
import concurrent.futures
import numpy as np
from database.db_manager import DBManager

empty_rtd = {key: None for key in sql_types.keys()}

rtd = RtdManager()
obstacles = ObstacleOlly(prefer_cache=False)


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

from helpers.profiler import profile


def add_distance(rtd):
    for prefix in ('ar', 'dp'):
        rtd[prefix + '_ct'] = rtd[prefix + '_ct'].fillna(value=rtd[prefix + '_pt'])

        rtd[prefix + '_cpth'] = rtd[prefix + '_cpth'].fillna(value=rtd[prefix + '_ppth'])

        rtd[prefix + '_cp'] = rtd[prefix + '_cp'].fillna(value=rtd[prefix + '_pp'])

    for col in ['ar_ppth', 'ar_cpth', 'dp_ppth', 'dp_cpth']:
        rtd[col] = rtd[col].astype('str').replace('nan', np.nan).replace('', np.nan)
        try:
            rtd[col] = rtd[col].str.split('|')
        except AttributeError:
            # It might happen, that the column is empty and therefor not of type str
            pass


    for i in rtd.index:
        ar_cpth = rtd.at[i, 'ar_cpth']
        station = rtd.at[i, 'station']
        if isinstance(ar_cpth, list):
            rtd.at[i, 'distance_to_last'] = obstacles.route_length([ar_cpth[-1]] + [station])
            rtd.at[i, 'distance_to_start'] = obstacles.route_length(ar_cpth + [station])

            path_obstacles = obstacles.obstacles_of_path(ar_cpth + [station], rtd.at[i, 'ar_pt'])
            if path_obstacles is not None:
                rtd.at[i, 'category_sum'] = path_obstacles['category_sum']
                rtd.at[i, 'category_mean'] = path_obstacles['category_mean']
                rtd.at[i, 'priority_sum'] = path_obstacles['priority_sum']
                rtd.at[i, 'priority_mean'] = path_obstacles['priority_mean']
                rtd.at[i, 'length_sum'] = path_obstacles['length_sum']
                rtd.at[i, 'length_mean'] = path_obstacles['length_mean']
                rtd.at[i, 'length_count'] = path_obstacles['length_count']
            
        else:
            rtd.at[i, 'distance_to_last'] = 0
            rtd.at[i, 'distance_to_start'] = 0

        dp_cpth = rtd.at[i, 'dp_cpth']
        if isinstance(dp_cpth, list):
            rtd.at[i, 'distance_to_next'] = obstacles.route_length([station] + [dp_cpth[0]])
            rtd.at[i, 'distance_to_end'] = obstacles.route_length([station] + dp_cpth)
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

# @profile
def parse_station(station, start_date, end_date):
    with DBManager() as db:
        stations_timetables = db.plan_of_station(station, date1=start_date, date2=end_date)
        parsed = parse_timetable(stations_timetables, db)

    if parsed:
        parsed = pd.DataFrame(parsed, columns=empty_rtd.keys())
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
        rtd_df = parsed.drop(current_array_cols, axis=1)
        db = DBManager()
        db.upsert_rtd(rtd_df)

    return True

def parse(only_new=True):
    if only_new:
        start_date = rtd.max_date() - datetime.timedelta(days=2)
    else:
        start_date = datetime.datetime(2020, 10, 1, 0, 0)
    end_date = datetime.datetime.now()
    # parse_station('Tübingen Hbf', start_date, end_date)
    with concurrent.futures.ProcessPoolExecutor(min(32, os.cpu_count())) as executor:
        futures = {executor.submit(parse_station, station, start_date, end_date): station
                   for station
                   in obstacles}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(obstacles)):
            future.result()



if __name__ == "__main__":
    import helpers.fancy_print_tcp

    # parse(only_new=False)
    parse(only_new=input('Do you wish to only parse new data? ([y]/n)') != 'n')
