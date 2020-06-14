import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime

from config import db_database, db_password, db_server, db_username


class DatabaseOfDoom:
    engine = sqlalchemy.create_engine(
            'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require',
            pool_pre_ping=True,
            pool_recycle=3600
        )

    Base = declarative_base()

    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()


    class JsonRtd(Base):
        __tablename__ = 'json_rtd_v2'
        date = Column(DateTime, primary_key=True)
        bhf = Column(Text, primary_key=True)
        plan = Column(JSON)
        changes = Column(JSON)

    Base.metadata.create_all(engine)

    queue = []

    
    def upsert(self, rows, no_update_cols=[]):
        table = self.JsonRtd.__table__

        stmt = insert(table).values(rows)

        update_cols = [c.name for c in table.c
                    if c not in list(table.primary_key.columns)
                    and c.name not in no_update_cols]

        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols}
            )

        self.session.execute(on_conflict_stmt)

    def upload_json(self, plan, changes, bhf, date, hour):
        date = datetime.datetime.combine(date, datetime.time(hour, 0))
        # new_row = self.JsonRtd(date=date, bhf=bhf, plan=plan, changes=changes)
        self.queue.append({'date': date, 'bhf': bhf, 'plan': plan, 'changes':changes})
        if len(self.queue) > 20:
            self.commit()
            # self.session.bulk_update_mappings(self.JsonRtd, self.queue)
            # self.queue = []
            # self.session.commit()
        # for _i in range(3):
        #     try:
        #         self.session.merge(new_row)
        #         self.session.commit()
        #         break
        #     except:
        #         print('rollback')
        #         self.session.rollback()

    def commit(self):
        self.upsert(self.queue)
        # self.session.bulk_update_mappings(self.JsonRtd, self.queue)
        self.queue = []
        self.session.commit()

    def get_json(self, bhf, date):
        if date is None:
            return self.session.query(self.JsonRtd).filter((self.JsonRtd.bhf == bhf)).all()
        return self.session.query(self.JsonRtd).filter((self.JsonRtd.bhf == bhf) & (self.JsonRtd.date == date)).first()

    def count_entrys_at_date(self, date):
        return self.session.query(self.JsonRtd).filter(self.JsonRtd.date == date).count()


if __name__ == "__main__":
    db = DatabaseOfDoom()
    print(db.count_entrys_at_date(datetime.datetime(2020, 6, 11, 17)))