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
from progress.bar import Bar
import random
import datetime
import os

# import cProfile, pstats, io

# def profile(fnc):
    
#     """A decorator that uses cProfile to profile a function"""
    
#     def inner(*args, **kwargs):
        
#         pr = cProfile.Profile()
#         pr.enable()
#         retval = fnc(*args, **kwargs)
#         pr.disable()
#         s = io.StringIO()
#         sortby = 'cumulative'
#         ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#         ps.print_stats()
#         print(s.getvalue())
#         return retval

#     return inner

logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(
    'crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_station_xml(station_id, str_date, hour, date, station):
    # try:
    plan_xml = dd.get_plan(station_id, str_date, hour)
    fl.save_plan_xml(plan_xml, station, date)
    real_xml = dd.get_real(station_id)
    fl.save_real_xml(real_xml, station, date)
    # except requests.exceptions.ConnectionError as ex:
    #     print(ex)


def get_hourely_batch():
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
                                             hour, date, station))

        # collect all finished gathering processes while changing the ip
        for i, gatherer in enumerate(concurrent.futures.as_completed(gatherers)):
            gatherer.result()
            if i % 10 == 0:
                bar.next(10)

            # renew ip in average each 400th time
            if random.randint(-200, 200) == 0:
                dd.new_ip()
    bar.finish()


if (__name__ == '__main__'):
    dd = download_dave()
    fl = FileLisa()
    stations = StationPhillip()

    hour = datetime.datetime.now().time().hour
    last_hour = hour - 2
    parsed_last_day = False
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            if last_hour == datetime.datetime.now().time().hour:
                sleep(20)
            else:
                hour = datetime.datetime.now().time().hour
                try:
                    if last_hour > hour:
                        print('trying to parse')
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
                    # logger.exception(ex)
