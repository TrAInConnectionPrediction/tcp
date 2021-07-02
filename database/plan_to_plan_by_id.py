import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tqdm import tqdm
from database import PlanById, Session, Plan
from helpers import StationPhillip
import sqlalchemy
from typing import Dict

def plan_to_stops(plan: str, station: str) -> Dict[int, Dict]:
    plan = {int(hash_id): {**plan[hash_id], 'station':station} for hash_id in plan}
    return plan

if __name__ == '__main__':
    stations = StationPhillip()
    for station in tqdm(stations, total=len(stations)):
        session: sqlalchemy.orm.Session
        with Session() as session:
            plans = Plan.plan_of_station(session, station)
            plans = [plan_to_stops(plan.plan, station) for plan in plans if plan.plan]
            parsed = {}
            for plan in plans:
                parsed.update(plan)
            PlanById.add_plan(session, parsed)
            session.commit()
            