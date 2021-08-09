import os
import sys
from typing import Dict, List, Tuple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from database import engine, get_engine, upsert_base
import json
import numpy as np

Base = declarative_base()


class PlanById(Base):
    __tablename__ = 'plan_by_id'
    hash_id = Column(BIGINT, primary_key=True, autoincrement=False)
    stop = Column(JSON)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return upsert_base(session, PlanById.__table__, rows)

    @staticmethod
    def add_plan(session: sqlalchemy.orm.Session, plan: dict):
        new_plan = [{'hash_id': hash_id, 'stop': json.dumps(plan[hash_id])}
                        for hash_id in plan if json.dumps(plan[hash_id])]
        for stop in new_plan:
            if 0 < stop['hash_id'] < 400:
                print(stop['hash_id'])
        PlanById.upsert(session, new_plan)

    @staticmethod
    def get_stops(session: sqlalchemy.orm.Session, hash_ids: List[int]) -> Dict[int, dict]:
        """
        Get stops that have a given hash_id

        Parameters
        ----------
        hash_ids: list
            A list of hash_ids to get the corresponding rows from the db.

        Returns
        -------
        Sqlalchemy query with the results.
        """
        stops = session.query(PlanById).filter(PlanById.hash_id.in_(hash_ids)).all()
        return {stop.hash_id: json.loads(stop.stop) for stop in stops}

    @staticmethod
    def count_entries(session: sqlalchemy.orm.Session) -> int:
        """
        Get the number of rows in db.

        Returns
        -------
        int
            Number of Rows.
        """
        return session.query(PlanById).count()

    @staticmethod
    def get_chunk_limits(session: sqlalchemy.orm.Session):
        minimum = session.query(sqlalchemy.func.min(PlanById.hash_id)).scalar()
        maximum = session.query(sqlalchemy.func.max(PlanById.hash_id)).scalar()
        count = session.query(PlanById.hash_id).count()
        n_divisions = count // 20_000
        divisions = np.linspace(minimum, maximum, n_divisions, dtype=int)
        chunk_limits = [(divisions[i], divisions[i+1]) for i in range(len(divisions) - 1)]
        return chunk_limits

    @staticmethod
    def get_stops_from_chunk(session: sqlalchemy.orm.Session, chunk_limits: Tuple[int, int]) -> Dict[int, dict]:
        """
        Get stops that have a given hash_id

        Parameters
        ----------
        hash_ids: list
            A list of hash_ids to get the corresponding rows from the db.

        Returns
        -------
        Sqlalchemy query with the results.
        """
        # IMPORTANT:
        # The filter should use ints and not floats. Ints makes the postgres planner
        # do a fast Index Scan compared to a slow Parallel Seq Scan
        stops = session.query(PlanById) \
            .filter(PlanById.hash_id >= int(chunk_limits[0]), PlanById.hash_id <= int(chunk_limits[1])).all()

        return {stop.hash_id: json.loads(stop.stop) for stop in stops}

    @staticmethod
    def get_hash_ids_in_chunk_limits(session: sqlalchemy.orm.Session, chunk_limits: Tuple[int, int]) -> List[int]:
        hash_ids = session.query(PlanById.hash_id) \
            .filter(PlanById.hash_id >= chunk_limits[0], PlanById.hash_id <= chunk_limits[1]).all()
        return [hash_id[0] for hash_id in hash_ids]



if __name__ == '__main__':
    from database.engine import sessionfactory
    engine, Session = sessionfactory()

    with Session() as session:
        PlanById.get_chunk_limits(session)