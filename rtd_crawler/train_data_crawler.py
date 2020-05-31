import sys
sys.path.append('../')
import requests
from rtd_parser import parse_full_day
from speed import unix_date, unix_now, to_unix
from downloader import *
from helpers import FileLisa, StationPhillip
import logging.handlers as handlers
import logging
from time import sleep
import concurrent.futures
from progress.bar import Bar
import random
import datetime
import os

logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(
    'crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_station_xml(station_id, str_date, hour, date, station, dd):
    plan_xml = dd.get_plan(station_id, str_date, hour)
    real_xml = dd.get_real(station_id)
    return {'plan_xml':plan_xml, 'real_xml':real_xml, 'station':station}


def get_hourely_batch():
    stations = StationPhillip()
    dd = DownloadDave()
    fl = FileLisa()

    date = unix_date(unix_now())
    hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).time().hour
    str_date = datetime.datetime.now().strftime('%y%m%d')

    station_list = list(station for station in stations.random_iter())
    bar = Bar('crawling ' + str(datetime.datetime.now()), max=len(stations))
    gatherers = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        # start all gathering processes
        for station in station_list:
            station_id = stations.get_eva(name=station)
            gatherers.append(executor.submit(get_station_xml, station_id, str_date,
                                             hour, date, station, dd))
        try:
            # collect all finished gathering processes while changing the ip
            for gatherer in concurrent.futures.as_completed(gatherers, timeout=(60*55)):
                xmls = gatherer.result()
                fl.save_station_xml(xmls['plan_xml'], xmls['station'], date, 'plan')
                fl.save_station_xml(xmls['real_xml'], xmls['station'], date, 'changes')
                bar.next()

                # renew ip in average each 400th time
                if random.randint(-200, 200) == 0:
                    dd.new_ip()
        except concurrent.futures._base.TimeoutError:
            pass
        except Exception as ex:
            print(ex)
        executor.shutdown(wait=False)
    bar.finish()


def gather_day(start_hour = 0):
    hour = datetime.datetime.now().time().hour - 1
    last_hour = hour
    with concurrent.futures.ProcessPoolExecutor() as executor:
        if start_hour == 0:
            parser_process = executor.submit(
                            parse_full_day, datetime.datetime.today() - datetime.timedelta(days=1))
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
                            data_crawler.result(timeout=0)
                        except Exception as ex:
                            print('crawler error')
                            logger.exception(ex)
                    data_crawler = executor.submit(get_hourely_batch)

                except Exception as ex:
                    print(ex)
        try:
            data_crawler.result(timeout=0)
        except Exception as ex:
            print('crawler error')
            logger.exception(ex)

        try:
            parser_process.result(timeout=0)
        except Exception as ex:
            print('parser error')
            logger.exception(ex)
        
        executor.shutdown(wait=False)


if (__name__ == '__main__'):
    import fancy_print_tcp
    while True:
        # stations = StationPhillip()
        # dd = DownloadDave()
        # fl = FileLisa()
        hour = datetime.datetime.now().time().hour
        gather_day(start_hour=hour)
        # del stations
        # del dd
        # del fl
        

    # last_hour = hour - 2
    # # parsed_last_day = False
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     while True:
    #         if last_hour == datetime.datetime.now().time().hour:
    #             sleep(20)
    #         else:
    #             hour = datetime.datetime.now().time().hour
    #             try:
    #                 if last_hour > hour:
    #                     if 'parser_process' in locals():
    #                         try:
    #                             parser_process.result(timeout=0)
    #                         except Exception as ex:
    #                             print('parser error')
    #                             logger.exception(ex)
    #                     parser_process = executor.submit(
    #                         parse_full_day, datetime.datetime.today() - datetime.timedelta(days=1))
    #                 last_hour = datetime.datetime.now().time().hour

    #                 if 'data_crawler' in locals():
    #                     try:
    #                         data_crawler.result(timeout=0)
    #                     except Exception as ex:
    #                         print('crawler error')
    #                         logger.exception(ex)
    #                 data_crawler = executor.submit(get_hourely_batch)

    #             except Exception as ex:
    #                 print(ex)
    #                 # logger.exception(ex)
