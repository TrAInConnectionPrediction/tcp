from multiprocessing import Process
import datetime
import random
from progress.bar import Bar
import concurrent.futures
from time import sleep
import logging
import logging.handlers as handlers
from helpers import StationPhillip, DatabaseOfDoom
from downloader import DownloadDave
from rtd_parser import xml_parser
import requests
import sys
import lxml.etree as etree
sys.path.append('../')

logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(
    'crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_station_xml(station_id, str_date, hour, station, dd):
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
    real_xml = dd.get_real(station_id)

    parser = etree.XMLParser(encoding='utf-8', collect_ids=False)

    if plan_xml and plan_xml != 'None' and plan_xml != '<timetable/>\n':
        plan_tree = etree.fromstring(plan_xml.encode(), parser)
        plan_json = list(xml_parser(part) for part in list(plan_tree))
    else:
        plan_json = None

    if real_xml and real_xml != 'None' and real_xml != '<timetable/>\n':
        changes_tree = etree.fromstring(real_xml.encode(), parser)
        changes_json = list(xml_parser(part) for part in list(changes_tree))
    else:
        changes_json = None

    return {'plan': plan_json, 'changes': changes_json, 'station': station}


def get_hourely_batch():
    """gathers plan and real xmls of the current date and hour from all stations
    inside of the stations table of the db. This function should be called once every hour.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        stations = StationPhillip()
        dd = DownloadDave()
        db = DatabaseOfDoom()

        datetime_date = datetime.datetime.today().date()
        hour = datetime.datetime.now().time().hour
        str_date = datetime.datetime.now().strftime('%y%m%d')

        station_list = list(station for station in stations.random_iter())
        bar = Bar('crawling ' + str(datetime.datetime.now()), max=len(stations))
        gatherers = []

        # start all gathering threads
        for station in station_list:
            station_id = stations.get_eva(name=station)
            gatherers.append(executor.submit(get_station_xml, station_id, str_date, hour, station, dd))
        try:
            # collect all finished gathering threads while changing the ip
            for gatherer in concurrent.futures.as_completed(gatherers, timeout=(60*55)):
                jsons = gatherer.result()

                db.add_jsons(jsons['plan'], jsons['changes'], jsons['station'], datetime_date, hour)
                bar.next()

                # change ip in average each 400th time
                if random.randint(-200, 200) == 0:
                    dd.new_ip()
        except concurrent.futures._base.TimeoutError:
            pass
        executor.shutdown(wait=False)
    bar.finish()


def gather_day(start_hour=0):
    hour = datetime.datetime.now().time().hour - 1
    last_hour = hour

    while True:
        if hour == datetime.datetime.now().time().hour:
            sleep(20)
        else:
            hour = datetime.datetime.now().time().hour
            if last_hour > hour:
                break
            try:
                last_hour = datetime.datetime.now().time().hour

                if 'data_crawler' in locals():
                    try:
                        data_crawler.join(timeout=0)
                    except Exception as ex:
                        print('crawler error')
                        logger.exception(ex)
                data_crawler = Process(target=get_hourely_batch)
                data_crawler.start()

            except Exception as ex:
                print(ex)
                logger.exception(ex)
    if 'data_crawler' in locals():
        try:
            data_crawler.join(timeout=0)
        except Exception as ex:
            print('crawler error')
            logger.exception(ex)


if (__name__ == '__main__'):
    import fancy_print_tcp
    while True:
        hour = datetime.datetime.now().time().hour
        gather_day(start_hour=hour)
