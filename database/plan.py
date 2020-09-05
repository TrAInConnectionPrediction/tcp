import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime, String, BIGINT
from sqlalchemy.dialects.postgresql import JSON, insert, ARRAY
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from database.engine import engine
import datetime

Base = declarative_base()


class Plan(Base):
    __tablename__ = 'plan_rtd'
    date = Column(DateTime, primary_key=True)
    bhf = Column(Text, primary_key=True)
    plan = Column(JSON)


try:
    Base.metadata.create_all(engine)
except sqlalchemy.exc.OperationalError:
    print('plan running offline!')


class PlanManager:
    Session = sessionmaker(bind=engine)
    session = Session()

    queue = []

    def upsert(self, rows, no_update_cols=[]):
        table = Plan.__table__

        stmt = insert(table).values(rows)

        update_cols = [c.name for c in table.c
                       if c not in list(table.primary_key.columns)
                       and c.name not in no_update_cols]

        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols}
        )

        self.session.execute(on_conflict_stmt)

    def add_plan(self, plan, bhf, date, hour):
        date = datetime.datetime.combine(date, datetime.time(hour, 0))
        self.queue.append({'date': date, 'bhf': bhf, 'plan': plan})
        if len(self.queue) > 1000:
            self.commit()

    def commit(self):
        self.upsert(self.queue)
        self.queue = []
        self.session.commit()

    def get_json(self, bhf, date1, date2):
        if date1 is None:
            return self.session.query(Plan).filter((Plan.bhf == bhf)).all()
        if date2 is None:
            return self.session.query(Plan).filter(
                (Plan.bhf == bhf) & (Plan.date == date1)).first()
        return self.session.query(Plan).filter((Plan.bhf == bhf)
                                               & (Plan.date >= date1)
                                               & (Plan.date < date2)).all()

    def count_entries_at_date(self, date):
        return self.session.query(Plan).filter(Plan.date == date).count()
