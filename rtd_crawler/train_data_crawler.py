import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from multiprocessing import Process, Pool
import datetime
import random
import progressbar
import concurrent.futures
from time import sleep
import logging
import logging.handlers as handlers
from helpers import StationPhillip
from DatabaseOfDoom import DatabaseOfDoom
from downloader import DownloadDave
import requests
import lxml.etree as etree


logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(
    'crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def xml_to_json(xml):
    """a recursive function to convert xml to list dict mix

    Arguments:
        xml {etree} -- the xml to convert

    Returns:
        dict -- a dict list mix of the xml
    """
    parsed = dict(xml.attrib)
    for xml_child in list(xml):
        if xml_child.tag in parsed:
            parsed[xml_child.tag].append(xml_to_json(xml_child))
        else:
            parsed[xml_child.tag] = [xml_to_json(xml_child)]
    return parsed


def get_station_json(station_id, str_date, hour, station, dd):
    """get the plan and real xml from a given station on a given date / time

    Arguments:
        station_id {int} -- id of station
        str_date {str} -- %y%m%d formatted date
        hour {int} -- hour to get the plan
        station {str} -- station as human readable name
        dd {DownloarDave} -- DownloadDave instace to download

    Returns:
        dict -- contains the gathered plan_xml and real_xml as well as the station
    """
    plan_xml = dd.get_plan(station_id, str_date, hour)
    changes_xml = dd.get_real(station_id)

    parser = etree.XMLParser(encoding='utf-8', collect_ids=False)

    if plan_xml and plan_xml != 'None' and plan_xml != '<timetable/>\n':
        plan_tree = etree.fromstring(plan_xml.encode(), parser)
        plan_json = list(xml_to_json(part) for part in list(plan_tree))
    else:
        plan_json = None
    

    if changes_xml and changes_xml != 'None' and changes_xml != '<timetable/>\n':
        changes_tree = etree.fromstring(changes_xml.encode(), parser)
        changes_json = list(xml_to_json(part) for part in list(changes_tree))
    else:
        changes_json = None
    

    return {'plan': plan_json, 'changes': changes_json, 'station': station}


def get_hourely_batch(_lol):
    """gathers plan and real xmls of the current date and hour from all stations
    inside of the stations table of the db. This function should be called once every hour.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        stations = StationPhillip()
        dd = DownloadDave()
        db = DatabaseOfDoom()

        date = datetime.datetime.today().date()
        hour = datetime.datetime.now().time().hour - 2
        str_date = datetime.datetime.now().strftime('%y%m%d')

        station_list = list(station for station in stations.random_iter())
        print('crawling ' + str(datetime.datetime.now()))
        gatherers = []

        # start all gathering threads
        for station in station_list:
            station_id = stations.get_eva(name=station)
            gatherers.append(executor.submit(get_station_json, station_id, str_date, hour, station, dd))

        bar = progressbar.ProgressBar(max_value=len(stations)).start()
        # collect all finished gathering threads while changing the ip
        for i, gatherer in enumerate(concurrent.futures.as_completed(gatherers, timeout=(60*40))):
            jsons = gatherer.result()

            db.add_row(jsons['plan'], jsons['changes'], jsons['station'], date, hour)

            # change ip in average each 400th time
            if random.randint(-200, 200) == 0:
                dd.new_ip()
            bar.update(i)
        db.commit()
        bar.finish()


if (__name__ == '__main__'):
    import fancy_print_tcp
    hour = datetime.datetime.now().time().hour - 1

    with Pool(processes=1, maxtasksperchild=1) as pool:
        while True:
            if hour == datetime.datetime.now().time().hour:
                sleep(20)
            else:
                hour = datetime.datetime.now().time().hour
                try:
                    result = pool.map_async(get_hourely_batch, range(1))
                    result.wait(60 * 50)
                    result.get(timeout=1)

                except Exception as ex:
                    print(ex)
                    logger.exception(ex)