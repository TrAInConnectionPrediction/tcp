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
import datetime
from database.change import ChangeManager
from rtd_crawler.xml_parser import xml_to_json


def preparse_changes(changes):
    changes = etree.fromstring(changes.encode())
    changes = list(xml_to_json(change) for change in changes)
    changes = {hash64(change['id']): change for change in changes if not 'from' in change}
    return changes


def get_db_con():
    connection = sqlite3.connect('data_buffer/recent_changes.db')
    cursor = connection.cursor()
    return connection, cursor


def monitor_recent_change(evas: list, save_to_db: int, dd):
    # dd = DownloadDave()
    new_changes = {}
    old_changes = [{} for _ in range(len(evas))]
    start_time = time.time()
    while datetime.datetime.now().hour != 3:
        for i, eva in enumerate(evas):
            changes = dd.get_recent_change(eva)
            changes = preparse_changes(changes)
            # There might be two different changes for a specific train in the changes. Looping in reverse makes the
            # most recent change to be added last.
            for train_id in reversed(changes.keys()):
                if train_id not in old_changes or changes[train_id] != old_changes[i][train_id]:
                    new_changes[train_id] = changes[train_id]

            old_changes[i] = changes

        if save_to_db % 10 == 0:
            # load changes to local sqlite db
            conn, c = get_db_con()
            for train_id in new_changes:
                c.execute('DELETE from rchg WHERE hash_id = :hash_id',
                          {'hash_id': train_id})
                c.execute('INSERT INTO rchg VALUES (:hash_id, :change)',
                          {'hash_id': train_id, 'change': json.dumps(new_changes[train_id])})
                conn.commit()
            conn.close()
            save_to_db = 0
            new_changes = {}
        save_to_db += 1

        # print((time.time() - start_time) % 90.0)
        time.sleep(90 - ((time.time() - start_time) % 90))


def upload_local_db():
    start_time = time.time()
    db = ChangeManager()
    conn, c = get_db_con()
    c.execute('SELECT * from rchg')
    for change in c:
        db.add_change(hash_id=change[0], change=change[1])
    c.execute('DELETE * from rchg')
    db.commit()
    print('needed {} minutes to upload db'.format((time.time() - start_time) / 60))


if __name__ == '__main__':
    import fancy_print_tcp
    # create database and table if not existing.
    try:
        conn = sqlite3.connect('data_buffer/recent_changes.db')
        c = conn.cursor()
        c.execute("""CREATE TABLE rchg (
                     hash_id int PRIMARY KEY,
                     change json
                     )""")
        conn.commit()
        conn.close()
    except:
        pass

    stations = StationPhillip()
    eva_list = stations.eva_index_stations.index.to_list()
    eva_list = [eva_list[i:i + 4] for i in range(0, len(eva_list), 4)]
    dd = SimplestDownloader()
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eva_list)) as executor:
            for i, evas in enumerate(eva_list):
                monitor_recent_change([8000207], 0, dd)
                input('lol')
                executor.submit(monitor_recent_change, evas, i % 10, dd)
                executor.shutdown(wait=True)

        upload_local_db()
