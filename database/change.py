import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, BIGINT
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from database.engine import engine
import json

Base = declarative_base()


class Change(Base):
    __tablename__ = 'change_rtd'
    hash_id = Column(BIGINT, primary_key=True)
    change = Column(JSON)


try:
    Base.metadata.create_all(engine)
except sqlalchemy.exc.OperationalError:
    print('database.change running offline!')


class ChangeManager:
    Session = sessionmaker(bind=engine)
    session = Session()

    queue = []
    changes = {}

    def upsert(self, rows: list):
        table = Change.__table__

        stmt = insert(table).values(rows)

        update_cols = [c.name for c in table.c if c not in list(table.primary_key.columns)]

        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols}
        )
        self.session.execute(on_conflict_stmt)

    def add_change(self, hash_id: int, change: dict):
        self.queue.append({'hash_id': hash_id, 'change': change})
        if len(self.queue) > 10000:
            self.commit()

    def add_changes(self, changes: dict):
        self.changes.update(changes)
        # self.queue.extend(changes)
        if len(self.changes) > 10000:
            new_changes = [{'hash_id': train_id, 'change': json.dumps(self.changes[train_id])}
                            for train_id in self.changes]
            self.upsert(new_changes)
            self.changes = {}
            self.session.commit()
            self.commit()

    def commit(self):
        self.upsert(self.queue)
        self.queue = []
        self.session.commit()

    def get_changes(self, hash_ids: list):
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
        return self.session.query(Change).filter(Change.hash_id.in_(hash_ids)).all()

    def count_entries(self) -> int:
        """
        Get the number of rows in db.

        Returns
        -------
        int
            Number of Rows.
        """
        return self.session.query(Change).count()
