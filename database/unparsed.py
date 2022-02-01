import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT, select
from sqlalchemy.ext.declarative import declarative_base
from database import get_engine, do_nothing_upsert_base, _gt14
from typing import List
import random
import time


Base = declarative_base()


def execute_func_that_might_deadlock(func):
    """
    Execute a function that might deadlock.
    """
    while True:
        try:
            return func()
        except sqlalchemy.exc.OperationalError as e:
            delay = random.randint(0, 5)
            print("Failed to fetch db. Wating", delay, "seconds.")
            print('Failed with exception:')
            print(e)
            time.sleep(delay)


class UnparsedPlan(Base):
    __tablename__ = 'unparased_plan'
    hash_id = Column(BIGINT, primary_key=True, autoincrement=False)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')
    
    @staticmethod
    def get_all(session: sqlalchemy.orm.Session) -> List[int]:
        if _gt14():
            all_hash_ids = session.execute(select(UnparsedPlan.hash_id)).all()
        else:
            all_hash_ids = session.query(UnparsedPlan.hash_id).all()
        return [part[0] for part in all_hash_ids]

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return execute_func_that_might_deadlock(lambda: do_nothing_upsert_base(session, UnparsedPlan.__table__, rows))
        # return do_nothing_upsert_base(session, UnparsedPlan.__table__, rows)
        
    @staticmethod
    def add(session: sqlalchemy.orm.Session, hash_ids: List[int]) -> None:
        if hash_ids:
            rows = [{'hash_id': hash_id} for hash_id in hash_ids]
            UnparsedPlan.upsert(session, rows)

    @staticmethod
    def remove(session: sqlalchemy.orm.Session, hash_ids: List[int]):
        if hash_ids:
            statement = sqlalchemy.delete(UnparsedPlan).where(UnparsedPlan.hash_id.in_(hash_ids))
            execute_func_that_might_deadlock(lambda: session.execute(statement))
            # session.execute(statement)

    @staticmethod
    def remove_all(session: sqlalchemy.orm.Session):
        statement = sqlalchemy.delete(UnparsedPlan)
        execute_func_that_might_deadlock(lambda: session.execute(statement))
        # session.execute(statement)
    

class UnparsedChange(Base):
    __tablename__ = 'unparased_change'
    hash_id = Column(BIGINT, primary_key=True, autoincrement=False)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')

    @staticmethod
    def get_all(session: sqlalchemy.orm.Session) -> List[int]:
        if _gt14():
            all_hash_ids = session.execute(select(UnparsedChange.hash_id)).all()
        else:
            all_hash_ids = session.query(UnparsedChange.hash_id).all()
        return [part[0] for part in all_hash_ids]

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return execute_func_that_might_deadlock(lambda: do_nothing_upsert_base(session, UnparsedChange.__table__, rows))
        # return do_nothing_upsert_base(session, UnparsedChange.__table__, rows)

    @staticmethod
    def add(session: sqlalchemy.orm.Session, hash_ids: List[int]) -> None:
        if hash_ids:
            rows = [{'hash_id': hash_id} for hash_id in hash_ids]
            UnparsedChange.upsert(session, rows)

    @staticmethod
    def remove(session: sqlalchemy.orm.Session, hash_ids: List[int]):
        if hash_ids:
            statement = sqlalchemy.delete(UnparsedChange).where(UnparsedChange.hash_id.in_(hash_ids))
            execute_func_that_might_deadlock(lambda: session.execute(statement))
            # session.execute(statement)

    @staticmethod
    def remove_all(session: sqlalchemy.orm.Session):
        statement = sqlalchemy.delete(UnparsedChange)
        execute_func_that_might_deadlock(lambda: session.execute(statement))
        # session.execute(statement)


if __name__ == '__main__':
    from database import sessionfactory
    engine, Session = sessionfactory()
    
    with Session() as session:
        UnparsedPlan.add(session, [i for i in range(10)])
        print(UnparsedPlan.get_all(session))
        UnparsedPlan.remove(session, [i for i in range(10)])
        print(UnparsedPlan.get_all(session))