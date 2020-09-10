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
import progressbar


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
    new_changes = {}
    start_time = time.time()
    while datetime.datetime.now().hour != 3 or (time.time() - start_time) < 60**3:
        for eva in evas:
            changes = dd.get_recent_change(eva)
            changes = preparse_changes(changes)
            # There might be two different changes for a specific train in the changes. Looping in reverse makes the
            # most recent change to be added last.
            for train_id in reversed(list(changes.keys())):
                new_changes[train_id] = changes[train_id]

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
    i = 0
    for change in c:
        db.add_change(hash_id=change[0], change=change[1])
        i += 1
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


if __name__ == '__main__':
    import fancy_print_tcp
    # Create database and table if not existing.
    try:
        conn, c = get_db_con()
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
            print('starting crawlers for {}'.format(str(datetime.datetime.now())))
            bar = progressbar.ProgressBar(max_value=len(eva_list)).start()
            for i, evas in enumerate(eva_list):
                executor.submit(monitor_recent_change, evas, i % 10, dd)
                bar.update(i)
            bar.finish()
            executor.shutdown(wait=True)

        upload_local_db()
