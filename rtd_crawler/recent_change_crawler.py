import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import lxml.etree as etree
import time
import concurrent.futures
from itertools import chain
import datetime
from helpers import StationPhillip
from rtd_crawler.SimplestDownloader import SimplestDownloader
from rtd_crawler.hash64 import hash64
from database import Change, UnparsedChange, sessionfactory
from rtd_crawler.xml_parser import xml_to_json
from config import station_to_monitor_per_thread

engine, Session = sessionfactory()


def preparse_changes(changes):
    changes = etree.fromstring(changes.encode())
    changes = list(xml_to_json(change) for change in changes)
    changes = {hash64(change['id']): change for change in changes if not 'from' in change}
    return changes


def monitor_recent_change(evas: list):
    new_changes = {}
    for eva in evas:
        changes = dd.get_recent_change(eva)
        if changes:
            changes = preparse_changes(changes)

            for train_id in changes:
                new_changes[train_id] = changes[train_id]
    return new_changes


dd = SimplestDownloader()
if __name__ == '__main__':
    import helpers.fancy_print_tcp

    stations = StationPhillip()
    eva_list = stations.eva_index_stations.index.to_list()
    eva_list = [
        eva_list[i:i + station_to_monitor_per_thread]
        for i
        in range(0, len(eva_list), station_to_monitor_per_thread)
    ]
    while True:
        stats = {
            'count': 0,
            'download_time': 0,
            'upload_time': 0,
        }
        stats_time = time.time()
        while (time.time() - stats_time) < 3600:
            download_start = time.time()
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(eva_list)) as executor:
                    new_changes = list(executor.map(monitor_recent_change, eva_list, timeout=60 * 4))
                    
                    stats['download_time'] += time.time() - download_start

                    upload_start = time.time()
                    # Concat list of dicts to single dict
                    new_changes = dict(chain.from_iterable(d.items() for d in new_changes))
                    with Session() as session:
                        Change.add_changes(session, new_changes)
                        UnparsedChange.add(session, new_changes.keys())
                        session.commit()
                    stats['upload_time'] += time.time() - upload_start

                    stats['count'] += 1
                    time.sleep(max(0.0, 120 - (time.time() - download_start)))
            except TimeoutError:
                pass

        print(datetime.datetime.now(),
              'count:', stats['count'],
              'download_time:', stats['download_time'] / stats['count'],
              'upload_time:', stats['upload_time'] / stats['count'])
