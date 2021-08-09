import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tqdm import tqdm
from database import PlanById, sessionfactory, Plan
from helpers import StationPhillip
import sqlalchemy
from typing import Dict
import concurrent.futures

def plan_to_stops(plan: str, station: str) -> Dict[int, Dict]:
    plan = {int(hash_id): {**plan[hash_id], 'station':station} for hash_id in plan}
    return plan


def station_to_by_id(station: str):
    engine, Session = sessionfactory()
    session: sqlalchemy.orm.Session
    with Session() as session:
        plans = Plan.plan_of_station(session, station)
        plans = [plan_to_stops(plan.plan, station) for plan in plans if plan.plan]
        parsed = {}
        for plan in plans:
            parsed.update(plan)
        PlanById.add_plan(session, parsed)
        # session.commit()
    engine.dispose()


if __name__ == '__main__':
    # Create table if it does not exist
    PlanById()

    stations = StationPhillip()
    with concurrent.futures.ProcessPoolExecutor(min(32, os.cpu_count())) as executor:
        futures = {executor.submit(station_to_by_id, station): station for station in stations}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(stations)):
            future.result()
            