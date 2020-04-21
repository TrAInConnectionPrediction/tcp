import fancy_print_tcp
import requests 
import os
import numpy as np
import datetime
import random
from progress.bar import Bar, IncrementalBar
import concurrent.futures
from time import sleep
import logging
import logging.handlers as handlers

from helpers import file_lisa, station_phillip
from downloader import *
from speed import unix_date, unix_now, to_unix
from rtd_parser import parse_full_day

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

# Here we define our formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler('crawler.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
# Here we set our logHandler's formatter
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_save_plan(station_id, str_date, hour, date, station):
    try:
        xml = dd.get_plan(station_id, str_date, hour)
        fl.save_plan_xml(xml, station, date)
    except requests.exceptions.ConnectionError:
        pass

def get_save_real(station_id, date, station):
    try:
        xml = dd.get_real(station_id)
        fl.save_real_xml(xml, station, date)
    except requests.exceptions.ConnectionError:
        pass

def get_save_plan_batch(batch, date, hour, str_date):
    # set hour and date here so that they definetely don't change between gathering and saving
    # hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).time().hour
    # str_date = datetime.datetime.now().strftime('%y%m%d')
    # date = unix_date(unix_now())

    gather_xmls = {}
    running_threads = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for station in batch:
            station_id = stations.get_eva(name=station)
            # start new thread
            gather_xmls[station] = executor.submit(get_save_plan,
                                            station_id,
                                            str_date,
                                            hour, date, station)
            running_threads.append(station)

        # save gathered xmls
        while running_threads:
            # running_threads[0] contains the station name
            gather_xmls[running_threads[0]].result() # plan_xml = 
            #fl.save_plan_xml(plan_xml, running_threads[0], date)
            del running_threads[0]

def get_save_real_batch(batch, date):
    # set these variables here so that they definetely don't change between gathering and saving
    # date = unix_date(unix_now())

    gather_xmls = {}
    running_threads = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for station in batch:
            station_id = stations.get_eva(name=station)
            # start new thread
            gather_xmls[station] = executor.submit(get_save_real, station_id, date, station)
            running_threads.append(station)

        # save gathered xmls
        while running_threads:
            # running_threads[0] contains the station name
            gather_xmls[running_threads[0]].result() #real_xml = 
            # fl.save_real_xml(real_xml, running_threads[0], date)
            del running_threads[0]

def get_hourely_batch():
    date = unix_date(unix_now())
    hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).time().hour
    str_date = datetime.datetime.now().strftime('%y%m%d')

    station_list = list(station for station in stations.random_iter())
    bar = Bar('crawling ' + str(datetime.datetime.now()), max = len(stations))
    i = 0
    old_eta = 1000000
    while i < len(station_list):
        batch_size = random.randint(50, 70)
        batch_size = len(station_list) - i if i + batch_size > len(station_list) else batch_size
        batch = station_list[i:i+batch_size]
        i += batch_size
        # don't put these two lines inside a try, as it slows down the process about 3 orders of magnitude. 
        get_save_plan_batch(batch, date, hour, str_date)
        get_save_real_batch(batch, date)

        # print progress
        bar.next(n=batch_size)
        # print(' eta: ', bar.eta_td, end='\r')

        # renew ip if the download slowed down
        if old_eta < bar.eta:
            dd.new_ip()
        old_eta = bar.eta

        # renew ip in average each 10th time
        if random.randint(-5, 5) == 0:
            dd.new_ip()
    bar.finish()


if (__name__ == '__main__'):
    dd = download_dave()
    fl = file_lisa()
    stations = station_phillip()

    hour = datetime.datetime.now().time().hour
    last_hour = datetime.datetime.now().time().hour - 2
    parsed_last_day = False
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # parser_process = executor.submit(parse_full_day, datetime.datetime.today())
        # parser_process.result()
        while True:
            if last_hour == datetime.datetime.now().time().hour:
                sleep(20)
            else:
                hour = datetime.datetime.now().time().hour
                try:
                    if last_hour > hour:
                        if 'parser_process' in locals():
                            parser_process.result()
                        parser_process = executor.submit(parse_full_day, datetime.datetime.today() - datetime.timedelta(days = 1))
                    last_hour = datetime.datetime.now().time().hour

                    if 'data_crawler' in locals():
                        data_crawler.result()
                    data_crawler = executor.submit(get_hourely_batch)
                except Exception as ex:
                    logger.exception(ex)