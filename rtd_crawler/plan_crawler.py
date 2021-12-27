import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import lxml.etree as etree
from helpers import StationPhillip
from rtd_crawler.SimplestDownloader import SimplestDownloader
from rtd_crawler.hash64 import hash64
import time
import datetime
from database import Plan, PlanById, UnparsedPlan, sessionfactory, plan_by_id
from rtd_crawler.xml_parser import xml_to_json
from concurrent.futures import ThreadPoolExecutor
import traceback

engine, Session = sessionfactory()

def preparse_plan(plan, station):
    """
    Convert xml to json and add hash_id as key
    """
    plan = etree.fromstring(plan.encode())
    plan = list(xml_to_json(stop) for stop in plan)
    plan = {hash64(stop['id']): {**stop, 'station':station} for stop in plan}
    return plan


def get_plan(eva: int, str_date: str, hour: int) -> str:
    return dd.get_plan(station_id=eva, date=str_date, hour=hour)


def hour_in_five_hours() -> int:
    return (datetime.datetime.now() + datetime.timedelta(hours=5)).time().hour

def date_in_five_hours() -> int:
    return (datetime.datetime.now() + datetime.timedelta(hours=5)).date()

if __name__ == '__main__':
    import helpers.fancy_print_tcp
    stations = StationPhillip()
    dd = SimplestDownloader()
    hour = hour_in_five_hours() - 1
    plan_by_station = Plan()
    plan_by_id = PlanById()
    unparsed_plan = UnparsedPlan()

    while True:
        if hour == hour_in_five_hours():
            time.sleep(20)
        else:
            evas = stations.get_eva(date=datetime.datetime.now()).to_list()
            names = stations.get_name(eva=evas, date=datetime.datetime.now()).to_list()
            date = date_in_five_hours()
            hour = hour_in_five_hours()
            str_date = date.strftime('%y%m%d')
            try:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    print(datetime.datetime.now(), 'getting plan for', date, hour)
                    plans = list(executor.map(lambda eva: get_plan(eva, str_date, hour), evas))

                with Session() as session:
                    plans_by_id = {}
                    for station, plan in zip(names, plans):
                        if plan is not None:
                            plan = preparse_plan(plan, station)
                            plan_by_station.add_plan(session, plan=plan, bhf=station, date=date, hour=hour)
                            plans_by_id.update(plan)
                    plan_by_id.add_plan(session, plan=plans_by_id)
                    unparsed_plan.add(session, plans_by_id.keys())
                            
                    session.commit()

                print(datetime.datetime.now(), 'uploaded plan to db')

            except Exception as ex:
                traceback.print_exc()
