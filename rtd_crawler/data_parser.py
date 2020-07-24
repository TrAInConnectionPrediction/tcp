import pandas as pd 
import pangres
import numpy as np 
import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime, String, Boolean
from sqlalchemy.dialects.postgresql import JSON, insert
import datetime
import progressbar
import json
from helpers import StationPhillip
from DatabaseOfDoom import DatabaseOfDoom
import pickle
from cityhash import CityHash64

from config import db_database, db_password, db_server, db_username


sql_types = {
    'ar_ppth': Text,
    'ar_cpth': Text,
    'ar_pp': Text,
    'ar_cp': Text,
    'ar_pt': Integer,
    'ar_ct': Integer,
    'ar_ps': String(length=1),
    'ar_cs': String(length=1),
    'ar_hi': Integer,
    'ar_clt': Integer,
    'ar_wings': Text,
    'ar_tra': Text,
    'ar_pde': Text,
    'ar_cde': Text,
    'ar_dc': Integer,
    'ar_l': Text,
    'ar_m': JSON,

    'dp_ppth': Text,
    'dp_cpth': Text,
    'dp_pp': Text,
    'dp_cp': Text,
    'dp_pt': Integer,
    'dp_ct': Integer,
    'dp_ps': String(length=1),
    'dp_cs': String(length=1),
    'dp_hi': Integer,
    'dp_clt': Integer,
    'dp_wings': Text,
    'dp_tra': Text,
    'dp_pde': Text,
    'dp_cde': Text,
    'dp_dc': Integer,
    'dp_l': Text,
    'dp_m': JSON,

    'f': String(length=1),
    't': Text,
    'o': Text,
    'c': Text,
    'n': Text,

    'm': JSON,
    'hd': JSON,
    'hdc': JSON,
    'conn': JSON,
    'rtr': JSON,

    'station': Text,
    'id': Text,
    'hash_id': Integer
}

def parse_stop_plan(stop):
    stop['hash_id'] = CityHash64(stop['id']) - ((2**63)-1)
    if 'tl' in stop:
        for key in stop['tl'][0]:
            stop[key] = stop['tl'][0][key]
        stop.pop('tl')
    if 'ar' in stop:
        for key in stop['ar'][0]:
            stop['ar_' + key] = stop['ar'][0][key]
        stop.pop('ar')
    if 'dp' in stop:
        for key in stop['dp'][0]:
            stop['dp_' + key] = stop['dp'][0][key]
        stop.pop('dp')
    return stop


def add_change_to_stop(stop, change):
    if 'ar' in change:
        for key in change['ar'][0]:
            stop['ar_' + key] = change['ar'][0][key]
    if 'dp' in change:
        for key in change['dp'][0]:
            stop['dp_' + key] = change['dp'][0][key]
    if 'm' in change:
        stop['m'] = change['m']
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
                # check wether the stop is still in the next plan. This happens when the train has delay and so its arr/dep is actually in the next hour
                # we cannot have a stop twice in our database.
                try:
                    if changes_delta > 0:
                        next_plan = station_data[date + datetime.timedelta(hours=changes_delta)].plan
                        if next_plan is not None:
                            if stop['id'] in (next_stop['id'] for next_stop in next_plan):
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


def upsert_rtd(table, conn, keys, data_iter):
    rows = []
    table = db.Rtd.__table__

    stmt = insert(table).values(rows)

    update_cols = [c.name for c in table.c
                if c not in list(table.primary_key.columns)]

    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=table.primary_key.columns,
        set_={k: getattr(stmt.excluded, k) for k in update_cols}
        )

    conn.execute(on_conflict_stmt)


def upload_data(df):
    """This function uploads the data to our database

    Arguments:
        df {pd.DataFrame} -- parsed data
    """
    df = df.set_index('hash_id')
    try:
        pangres.upsert(engine, df, if_row_exists='update', table_name='rtd', dtype=sql_types)
    except IndexError:
        df = df.loc[~df.index.duplicated(keep='last')]
        pangres.upsert(engine, df, if_row_exists='update', table_name='rtd', dtype=sql_types)
    # # df = df.set_index('id')
    # df.to_sql('rtd', con=engine, if_exists='append', method=upsert_rtd, dtype=sql_types)


if __name__ == "__main__":
    start_date = datetime.datetime(2020, 1, 1, 0, 0)
    end_date = datetime.datetime.now()

    engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require') 
    stations = StationPhillip()
    db = DatabaseOfDoom()
    buffer = pd.DataFrame()
    with progressbar.ProgressBar(max_value=len(stations)) as bar:
        for i, station in enumerate(stations):
            station_data = {}
            station_data = db.get_json(station, date1=start_date, date2=end_date)
            parsed = parse_station(station_data)
            parsed = pd.DataFrame(parsed) # .drop_duplicates(subset=['id'], keep='last')
            parsed['station'] = station
            buffer = pd.concat([buffer, parsed], ignore_index=True)

            # upload the data as soon as it is longer than 1000 rows. This is more efficient than uploading each stations data individually
            if len(buffer) > 1000:
                upload_data(buffer)
                buffer = pd.DataFrame()
            bar.update(i)
            # download jsons
            # open plan json
            # open real jsons
            # iterate over plan json
                # find newest changed event
                # add it to plan
                # break
        