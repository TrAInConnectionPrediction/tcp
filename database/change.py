import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from database import get_engine, upsert_base
import json

Base = declarative_base()


class Change(Base):
    __tablename__ = 'change_rtd'
    hash_id = Column(BIGINT, primary_key=True)
    change = Column(JSON)

    def __init__(self) -> None:
        try:
            engine = get_engine()
            self.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{self.__tablename__} running offline!')

    @staticmethod
    def upsert(session: sqlalchemy.orm.Session, rows: List[dict]):
        return upsert_base(session, Change.__table__, rows)

    @staticmethod
    def add_changes(session: sqlalchemy.orm.Session, changes: dict):
        new_changes = [{'hash_id': train_id, 'change': json.dumps(changes[train_id])}
                        for train_id in changes]
        Change.upsert(session, new_changes)

    @staticmethod
    def get_changes(session: sqlalchemy.orm.Session, hash_ids: List) -> List:
        """
        Get changes that have a given hash_id

        Parameters
        ----------
        hash_ids: list
            A list of hash_ids to get the corresponding rows from the db.

        Returns
        -------
        Sqlalchemy query with the results.
        """
        return session.query(Change).filter(Change.hash_id.in_(hash_ids)).all()

    @staticmethod
    def count_entries(session: sqlalchemy.orm.Session) -> int:
        """
        Get the number of rows in db.

        Returns
        -------
        int
            Number of Rows.
        """
        return session.query(Change).count()


if __name__ == '__main__':
    pass