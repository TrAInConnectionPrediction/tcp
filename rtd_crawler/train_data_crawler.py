import sys
sys.path.append('../')
import requests
import fancy_print_tcp
from rtd_parser import parse_full_day
from speed import unix_date, unix_now, to_unix
from downloader import *
from helpers import FileLisa, StationPhillip
import logging.handlers as handlers
import logging
from time import sleep
import concurrent.futures
from progress.bar import Bar, IncrementalBar
import random
import datetime
import numpy as np
import os


logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(
    'crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_save_plan(station_id, str_date, hour, date, station):
    try:
        xml = dd.get_plan(station_id, str_date, hour)
        fl.save_plan_xml(xml, station, date)
    except requests.exceptions.ConnectionError as ex:
        print(ex)


def get_save_real(station_id, date, station):
    try:
        xml = dd.get_real(station_id)
        fl.save_real_xml(xml, station, date)
    except requests.exceptions.ConnectionError as ex:
        print(ex)


def get_hourely_batch():
    date = unix_date(unix_now())
    hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).time().hour
    str_date = datetime.datetime.now().strftime('%y%m%d')

    station_list = list(station for station in stations.random_iter())
    bar = Bar('crawling ' + str(datetime.datetime.now()), max=len(stations))
    gatherers = []
    old_eta = 1000000
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        # start all gathering processes
        for station in station_list:
            station_id = stations.get_eva(name=station)
            gatherers.append(executor.submit(get_save_plan, station_id, str_date,
                                             hour, date, station))

            gatherers.append(executor.submit(
                get_save_real, station_id, date, station))

        # collect all finished gathering processes while changing the ip
        for i, gatherer in enumerate(concurrent.futures.as_completed(gatherers)):
            gatherer.result()
            if i % 6 == 0:
                bar.next(3)

            # renew ip if the download slowed down
            if old_eta < bar.eta:
                dd.new_ip()
            old_eta = bar.eta
            # renew ip in average each 60th time
            if random.randint(-30, 30) == 0:
                dd.new_ip()

    bar.finish()


if (__name__ == '__main__'):
    dd = download_dave()
    fl = FileLisa()
    stations = StationPhillip()

    hour = datetime.datetime.now().time().hour
    last_hour = datetime.datetime.now().time().hour - 2
    parsed_last_day = False
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            if last_hour == datetime.datetime.now().time().hour:
                sleep(20)
            else:
                hour = datetime.datetime.now().time().hour
                try:
                    if last_hour > hour:
                        if 'parser_process' in locals():
                            try:
                                parser_process.result()
                            except Exception as ex:
                                print('parser error')
                                logger.exception(ex)
                        parser_process = executor.submit(
                            parse_full_day, datetime.datetime.today() - datetime.timedelta(days=1))
                    last_hour = datetime.datetime.now().time().hour

                    if 'data_crawler' in locals():
                        try:
                            data_crawler.result()
                        except Exception as ex:
                            print('crawler error')
                            logger.exception(ex)
                    data_crawler = executor.submit(get_hourely_batch)

                except Exception as ex:
                    print(ex)
                    logger.exception(ex)
