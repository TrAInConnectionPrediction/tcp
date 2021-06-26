import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT, select
from sqlalchemy.ext.declarative import declarative_base
from database import get_engine, Session, upsert_base, do_nothing_upsert_base
from typing import List


Base = declarative_base()


class UnparsedPlan(Base):
    __tablename__ = 'unparased_plan'
    hash_id = Column(BIGINT, primary_key=True)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')
    
    @staticmethod
    def get_all(session: sqlalchemy.orm.Session) -> List[int]:
        all_hash_ids = session.execute(select(UnparsedPlan.hash_id)).all()
        return [part[0] for part in all_hash_ids]

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return do_nothing_upsert_base(session, UnparsedPlan.__table__, rows)
        
    @staticmethod
    def add(session: sqlalchemy.orm.Session, hash_ids: List[int]) -> None:
        rows = [{'hash_id': hash_id} for hash_id in hash_ids]
        UnparsedPlan.upsert(session, rows)

    @staticmethod
    def remove(session: sqlalchemy.orm.Session, hash_ids: List[int]):
        statement = sqlalchemy.delete(UnparsedPlan).where(UnparsedPlan.hash_id.in_(hash_ids))
        session.execute(statement)
    

class UnparsedChange(Base):
    __tablename__ = 'unparased_change'
    hash_id = Column(BIGINT, primary_key=True)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')

    @staticmethod
    def get_all(session: sqlalchemy.orm.Session) -> List[int]:
        all_hash_ids = session.execute(select(UnparsedChange.hash_id)).all()
        return [part[0] for part in all_hash_ids]

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return do_nothing_upsert_base(session, UnparsedChange.__table__, rows)

    @staticmethod
    def add(session: sqlalchemy.orm.Session, hash_ids: List[int]) -> None:
        rows = [{'hash_id': hash_id} for hash_id in hash_ids]
        UnparsedChange.upsert(session, rows)

    @staticmethod
    def remove(session: sqlalchemy.orm.Session, hash_ids: List[int]):
        statement = sqlalchemy.delete(UnparsedChange).where(UnparsedChange.hash_id.in_(hash_ids))
        session.execute(statement)


if __name__ == '__main__':
    with Session() as session:
        UnparsedPlan.add(session, [i for i in range(10)])
        print(UnparsedPlan.get_all(session))
        UnparsedPlan.remove(session, [i for i in range(10)])
        print(UnparsedPlan.get_all(session))