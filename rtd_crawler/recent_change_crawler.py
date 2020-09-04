import lxml.etree as etree
from helpers.StationPhillip import StationPhillip
from rtd_crawler.SimplestDownloader import SimplestDownloader
from rtd_crawler.hash64 import hash64
import time
import concurrent.futures
import sqlite3
import json
from config import db_database, db_password, db_server, db_username


def xml_to_json(xml):
    """
    A recursive function to convert xml to list dict mix.

    Parameters
    ----------
    xml : lxml.etree
        The xml to convert

    Returns
    -------
    dict
        A dict list mix of the xml
    """
    parsed = dict(xml.attrib)
    for xml_child in list(xml):
        if xml_child.tag in parsed:
            parsed[xml_child.tag].append(xml_to_json(xml_child))
        else:
            parsed[xml_child.tag] = [xml_to_json(xml_child)]
    return parsed


def preparse_changes(changes):
    changes = etree.fromstring(changes.encode())
    changes = (xml_to_json(change) for change in changes)
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
    while True:
        for i, eva in enumerate(evas):
            changes = dd.get_recent_change(eva)
            changes = preparse_changes(changes)
            # There might be two different changes for a specific train in the changes. Looping in reverse makes the
            # most recent change to be added last.
            for train_id in reversed(changes):
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

        print((time.time() - start_time) % 90.0)
        time.sleep(90 - ((time.time() - start_time) % 90))


if __name__ == '__main__':
    # conn = sqlite3.connect('data_buffer/recent_changes.db')
    # c = conn.cursor()
    # c.execute("""CREATE TABLE rchg (
    #              hash_id int PRIMARY KEY,
    #              change json
    #              )""")
    # conn.commit()
    # conn.close()

    stations = StationPhillip()
    eva_list = stations.eva_index_stations.index.to_list()
    eva_list = [eva_list[i:i + 4] for i in range(0, len(eva_list), 4)]
    dd = SimplestDownloader()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eva_list)) as executor:
        for i, evas in enumerate(eva_list):
            # monitor_recent_change(evas, 0, dd)
            # input('lol')
            executor.submit(monitor_recent_change, evas, i % 10, dd)
            # print(i)
