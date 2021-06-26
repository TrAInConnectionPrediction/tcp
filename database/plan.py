import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, Text, DateTime
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from database import get_engine, upsert_base
import datetime

Base = declarative_base()


class Plan(Base):
    __tablename__ = 'plan_rtd'
    date = Column(DateTime, primary_key=True)
    bhf = Column(Text, primary_key=True)
    plan = Column(JSON)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')

        self.queue = []

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return upsert_base(session, Plan.__table__, rows)

    def add_plan(self, session: sqlalchemy.orm.Session, plan, bhf, date, hour) -> None:
        date = datetime.datetime.combine(date, datetime.time(hour, 0))
        self.queue.append({'date': date, 'bhf': bhf, 'plan': plan})
        if len(self.queue) > 1000:
            self.commit(session)

    def commit(self, session: sqlalchemy.orm.Session):
        self.upsert(session, self.queue)
        self.queue = []

    @staticmethod
    def plan_of_station(
        session: sqlalchemy.orm.Session,
        bhf: str,
        date1: datetime.datetime,
        date2: datetime.datetime
    ) -> List:
        if date1 is None:
            return session.query(Plan).filter((Plan.bhf == bhf)).all()
        if date2 is None:
            return session.query(Plan).filter(
                (Plan.bhf == bhf) & (Plan.date == date1)).first()
        return session.query(Plan).filter((Plan.bhf == bhf)
                                               & (Plan.date >= date1)
                                               & (Plan.date < date2)).all()

    @staticmethod
    def count_entries_at_date(
        session: sqlalchemy.orm.Session,
        date: datetime.datetime
    ) -> int:
        return session.query(Plan).filter(Plan.date == date).count()


if __name__ == '__main__':
    try:
        engine = get_engine()
        Base.metadata.create_all(engine)
        engine.dispose()
    except sqlalchemy.exc.OperationalError:
        print('database.plan running offline!')