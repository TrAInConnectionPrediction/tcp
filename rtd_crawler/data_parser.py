import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import pangres
import sqlalchemy
from sqlalchemy import Integer, Text, DateTime, String
from sqlalchemy.dialects.postgresql import JSON, insert, ARRAY
import datetime
import progressbar
from helpers.StationPhillip import StationPhillip
from rtd_crawler.DatabaseOfDoom import DatabaseOfDoom
from rtd_crawler.hash64 import hash64

from config import db_database, db_password, db_server, db_username


sql_types = {
    'ar_ppth': Text,
    'ar_cpth': Text,
    'ar_pp': Text,
    'ar_cp': Text,
    'ar_pt': DateTime,
    'ar_ct': DateTime,
    'ar_ps': String(length=1),
    'ar_cs': String(length=1),
    'ar_hi': Integer,
    'ar_clt': DateTime,
    'ar_wings': Text,
    'ar_tra': Text,
    'ar_pde': Text,
    'ar_cde': Text,
    'ar_dc': Integer,
    'ar_l': Text,
    'ar_m_id': ARRAY(Text),
    'ar_m_t': ARRAY(Text),
    'ar_m_ts': ARRAY(DateTime),
    'ar_m_c': ARRAY(Integer),

    'dp_ppth': Text,
    'dp_cpth': Text,
    'dp_pp': Text,
    'dp_cp': Text,
    'dp_pt': DateTime,
    'dp_ct': DateTime,
    'dp_ps': String(length=1),
    'dp_cs': String(length=1),
    'dp_hi': Integer,
    'dp_clt': DateTime,
    'dp_wings': Text,
    'dp_tra': Text,
    'dp_pde': Text,
    'dp_cde': Text,
    'dp_dc': Integer,
    'dp_l': Text,
    'dp_m_id': ARRAY(Text),
    'dp_m_t': ARRAY(Text),
    'dp_m_ts': ARRAY(DateTime),
    'dp_m_c': ARRAY(Integer),

    'f': String(length=1),
    't': Text,
    'o': Text,
    'c': Text,
    'n': Text,

    'm_id': ARRAY(Text),
    'm_t': ARRAY(Text),
    'm_ts': ARRAY(DateTime),
    'm_c': ARRAY(Integer),
    'hd': JSON,
    'hdc': JSON,
    'conn': JSON,
    'rtr': JSON,

    'station': Text,
    'id': Text,
    'hash_id': Integer
}
# these are the names of columns, that contain a time and should be parsed into a datetime
time_names = ('pt', 'ct', 'clt', 'ts')
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
    # add arrival changes
    if 'ar' in change:
        for key in change['ar'][0]:
            if key == 'm':
                # add message
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

    # add departure changes
    if 'dp' in change:
        for key in change['dp'][0]:
            if key == 'm':
                # add message
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
    if 'm' in change:
        # stop['m'] = change['m']
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


def parse_station(station_data):
    parsed = []
    station_data = {hour_batch.date: hour_batch for hour_batch in station_data}
    for date in station_data:
        plan = station_data[date].plan
        if plan is None:
            continue
        for stop in plan:
            stop = parse_stop_plan(stop)
            for changes_delta in range(3, -1, -1):
                try:
                    changes = station_data[date + datetime.timedelta(hours=changes_delta)].changes
                except KeyError:
                    continue
                if changes is None:
                    continue
                # Check whether the stop is still in the next plan. This happens when the train has delay and so its arr/dep is actually in the next hour.
                # We cannot have a stop twice in our database.
                try:
                    if changes_delta > 0:
                        next_plan = station_data[date + datetime.timedelta(hours=changes_delta)].plan
                        if next_plan is not None:
                            if stop['id'] in list(next_stop['id'] for next_stop in next_plan):
                                stop = None
                                break
                except KeyError:
                    pass
                for change in changes:
                    if stop['id'] == change['id']:
                        stop = add_change_to_stop(stop, change)
                        break
            if stop:
                parsed.append(stop)
    return parsed


def upload_data(df):
    """This function uploads the data to our database.

    Arguments:
        df {pd.DataFrame} -- parsed data
    """
    if not df.empty:
        df = df.set_index('hash_id')
        try:
            pangres.upsert(engine, df, if_row_exists='update', table_name='rtd', dtype=sql_types)
        except IndexError:
            df = df.loc[~df.index.duplicated(keep='last')]
            pangres.upsert(engine, df, if_row_exists='update', table_name='rtd', dtype=sql_types)


if __name__ == "__main__":
    engine = sqlalchemy.create_engine(
        'postgresql://' + db_username + ':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require')
    stations = StationPhillip()
    db = DatabaseOfDoom()

    if input('Do you wish to only parse new data? ([y]/n)') == 'n':
        start_date = datetime.datetime(2020, 1, 1, 0, 0)
    else:
        start_date = db.max_date() - datetime.timedelta(days=2)

    end_date = datetime.datetime.now()
    with progressbar.ProgressBar(max_value=len(stations)) as bar:
        for i, station in enumerate(stations):
            station_data = db.get_json(station, date1=start_date, date2=end_date)
            parsed = parse_station(station_data)
            parsed = pd.DataFrame(parsed)
            parsed['station'] = station

            upload_data(parsed)
            bar.update(i)
