from concurrent.futures.process import ProcessPoolExecutor
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lxml.etree as etree
from helpers.StationPhillip import StationPhillip
from rtd_crawler.SimplestDownloader import SimplestDownloader
from rtd_crawler.hash64 import hash64
import time
import datetime
from database.plan import PlanManager
from rtd_crawler.xml_parser import xml_to_json
import progressbar
from concurrent.futures import ThreadPoolExecutor


def preparse_plan(plan):
    plan = etree.fromstring(plan.encode())
    plan = list(xml_to_json(change) for change in plan)
    plan = {hash64(stop['id']): stop for stop in plan}
    return plan


def get_plan(eva, str_date, hour):
    return dd.get_plan(station_id=eva, date=str_date, hour=hour)

if __name__ == '__main__':
    import helpers.fancy_print_tcp
    stations = StationPhillip()
    evas = stations.eva_index_stations.index.to_list()
    dd = SimplestDownloader()
    db = PlanManager()
    hour = datetime.datetime.now().time().hour - 1

    while True:
        if hour == datetime.datetime.now().time().hour:
            time.sleep(20)
        else:
            date = datetime.datetime.today().date()
            hour = datetime.datetime.now().time().hour
            str_date = datetime.datetime.now().strftime('%y%m%d')
            try:
                bar = progressbar.ProgressBar(max_value=len(stations)).start()
                i = 0
                with ThreadPoolExecutor(max_workers=4) as executor:
                    plans = executor.map(lambda eva: get_plan(eva, str_date, hour), evas)
                for bhf, plan in zip(stations.sta_list, plans):
                    if plan is not None:
                        plan = preparse_plan(plan)
                        db.add_plan(plan=plan, bhf=bhf, date=date, hour=hour)
                    bar.update(i)
                    i += 1
                db.commit()
                bar.finish()

            except Exception as ex:
                print(ex)



