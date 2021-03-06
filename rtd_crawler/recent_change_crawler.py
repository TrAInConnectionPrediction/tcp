import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lxml.etree as etree
from helpers.StationPhillip import StationPhillip
from rtd_crawler.SimplestDownloader import SimplestDownloader
from rtd_crawler.hash64 import hash64
import time
import concurrent.futures
import sqlite3
import json
from itertools import chain
import datetime
from database.change import ChangeManager
from rtd_crawler.xml_parser import xml_to_json
from config import station_to_monitor_per_thread


def preparse_changes(changes):
    changes = etree.fromstring(changes.encode())
    changes = list(xml_to_json(change) for change in changes)
    changes = {hash64(change['id']): change for change in changes if not 'from' in change}
    return changes


def get_db_con():
    connection = sqlite3.connect('data_buffer/recent_changes.db')
    # connection.execute('pragma journal_mode=wal')
    cursor = connection.cursor()
    return connection, cursor


def monitor_recent_change(evas: list):
    new_changes = {}
    for eva in evas:
        changes = dd.get_recent_change(eva)
        if changes:
            changes = preparse_changes(changes)

            for train_id in reversed(list(changes.keys())):
                new_changes[train_id] = changes[train_id]
    return new_changes


def upload_local_db():
    start_time = time.time()
    db = ChangeManager()
    conn, c = get_db_con()
    c.execute('SELECT * from rchg')
    i = 0
    for change in c:
        db.add_change(hash_id=change[0], change=change[1])
        i += 1
    db.commit()
    c.execute('DROP TABLE rchg')
    conn.commit()
    db.commit()
    c.execute("""CREATE TABLE rchg (
                 hash_id int PRIMARY KEY,
                 change json
                 )""")
    conn.commit()
    conn.close()
    print('needed {time} minutes to upload {rows} rows to db'.format(time=(time.time() - start_time) / 60, rows=i))


dd = SimplestDownloader()
if __name__ == '__main__':
    import helpers.fancy_print_tcp
    # Create local database and table if not existing.
    conn, c = get_db_con()
    try:
        c.execute("""CREATE TABLE rchg (
                     hash_id int PRIMARY KEY,
                     change json
                     )""")
        conn.commit()
    except:
        pass
    conn.close()

    stations = StationPhillip()
    eva_list = stations.eva_index_stations.index.to_list()
    eva_list = [eva_list[i:i + station_to_monitor_per_thread] for i in range(0, len(eva_list), station_to_monitor_per_thread)]
    # monitor_recent_change([8000207], dd)
    while True:
        upload_time = time.time()
        while datetime.datetime.now().hour != 3 or (time.time() - upload_time) < 3600:
            start_time = time.time()
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(eva_list)) as executor:
                    print('{}: starting crawlers'.format(str(datetime.datetime.now())), end='\r')
                    new_changes = executor.map(monitor_recent_change, eva_list, timeout=60 * 4)
            except TimeoutError:
                pass
            # Concat list of dicts to single dict
            new_changes = dict(chain.from_iterable(d.items() for d in new_changes))

            conn, c = get_db_con()
            c.executemany('DELETE from rchg WHERE hash_id = :hash_id',
                          [{'hash_id': train_id} for train_id in new_changes])
            c.executemany('INSERT INTO rchg VALUES (:hash_id, :change)',
                          [{'hash_id': train_id, 'change': json.dumps(new_changes[train_id])} for train_id in new_changes])
            conn.commit()

            print('{datetime}: finished after {time_needed} seconds. Now waiting {time_waiting} seconds till restart'
                  .format(datetime=(datetime.datetime.now()),
                          time_needed=time.time() - start_time,
                          time_waiting=120 - (time.time() - start_time)), end='\r')
            time.sleep(max(0.0, 120 - (time.time() - start_time)))

        upload_local_db()
