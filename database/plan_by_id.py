import os
import sys
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from database import get_engine, upsert_base
import json

Base = declarative_base()


class PlanById(Base):
    __tablename__ = 'plan_by_id'
    hash_id = Column(BIGINT, primary_key=True)
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
    def add_plan(session, plan: dict):
        new_plan = [{'hash_id': train_id, 'stop': json.dumps(plan[train_id])}
                        for train_id in plan]
        PlanById.upsert(session, new_plan)

    @staticmethod
    def get_stops(session, hash_ids: list):
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
        return session.query(PlanById).filter(PlanById.hash_id.in_(hash_ids)).all()

    @staticmethod
    def count_entries(session) -> int:
        """
        Get the number of rows in db.

        Returns
        -------
        int
            Number of Rows.
        """
        return session.query(PlanById).count()


if __name__ == '__main__':
    pass