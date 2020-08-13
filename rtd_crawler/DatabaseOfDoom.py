import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime, String, BIGINT
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func
import datetime

from config import db_database, db_password, db_server, db_username

"""
\d tcp:
                                          Table "public.rtd"
  Column  |            Type             | Collation | Nullable |               Default                
----------+-----------------------------+-----------+----------+--------------------------------------
 ar_ppth  | text                        |           |          | 
 ar_cpth  | text                        |           |          | 
 ar_pp    | text                        |           |          | 
 ar_cp    | text                        |           |          | 
 ar_pt    | timestamp without time zone |           |          | 
 ar_ct    | timestamp without time zone |           |          | 
 ar_ps    | character varying(1)        |           |          | 
 ar_cs    | character varying(1)        |           |          | 
 ar_hi    | integer                     |           |          | 
 ar_clt   | timestamp without time zone |           |          | 
 ar_wings | text                        |           |          | 
 ar_tra   | text                        |           |          | 
 ar_pde   | text                        |           |          | 
 ar_cde   | text                        |           |          | 
 ar_dc    | integer                     |           |          | 
 ar_l     | text                        |           |          | 
 ar_m     | json                        |           |          | 
 dp_ppth  | text                        |           |          | 
 dp_cpth  | text                        |           |          | 
 dp_pp    | text                        |           |          | 
 dp_cp    | text                        |           |          | 
 dp_pt    | timestamp without time zone |           |          | 
 dp_ct    | timestamp without time zone |           |          | 
 dp_ps    | character varying(1)        |           |          | 
 dp_cs    | character varying(1)        |           |          | 
 dp_hi    | integer                     |           |          | 
 dp_clt   | timestamp without time zone |           |          | 
 dp_wings | text                        |           |          | 
 dp_tra   | text                        |           |          | 
 dp_pde   | text                        |           |          | 
 dp_cde   | text                        |           |          | 
 dp_dc    | integer                     |           |          | 
 dp_l     | text                        |           |          | 
 dp_m     | json                        |           |          | 
 f        | character varying(1)        |           |          | 
 t        | text                        |           |          | 
 o        | text                        |           |          | 
 c        | text                        |           |          | 
 n        | text                        |           |          | 
 m        | json                        |           |          | 
 hd       | json                        |           |          | 
 hdc      | json                        |           |          | 
 conn     | json                        |           |          | 
 rtr      | json                        |           |          | 
 station  | text                        |           |          | 
 id       | text                        |           |          | 
 hash_id  | bigint                      |           | not null | nextval('rtd_hash_id_seq'::regclass)
Indexes:
    "rtd_pkey" PRIMARY KEY, btree (hash_id)
"""


class RtdDbModel:
    """class containing table schemes for our db
    """
    DB_CONNECT_STRING = 'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'
    
    engine = sqlalchemy.create_engine (
        DB_CONNECT_STRING,
        pool_pre_ping=True,
        pool_recycle=3600
    )

    Base = declarative_base()

    class JsonRtd(Base):
        """scheme for table for raw data
        """
        __tablename__ = 'json_rtd_v2'
        date = Column(DateTime, primary_key=True)
        bhf = Column(Text, primary_key=True)
        plan = Column(JSON)
        changes = Column(JSON)

    class Rtd(Base):
        """scheme for parsed data
        """
        __tablename__ = 'rtd'
        ar_ppth = Column(Text)
        ar_cpth = Column(Text)
        ar_pp = Column(Text)
        ar_cp = Column(Text)
        ar_pt = Column(DateTime)
        ar_ct = Column(DateTime)
        ar_ps = Column(String(length=1))
        ar_cs = Column(String(length=1))
        ar_hi = Column(Integer)
        ar_clt = Column(DateTime)
        ar_wings = Column(Text)
        ar_tra = Column(Text)
        ar_pde = Column(Text)
        ar_cde = Column(Text)
        ar_dc = Column(Integer)
        ar_l = Column(Text)
        ar_m = Column(JSON)

        dp_ppth = Column(Text)
        dp_cpth = Column(Text)
        dp_pp = Column(Text)
        dp_cp = Column(Text)
        dp_pt = Column(DateTime)
        dp_ct = Column(DateTime)
        dp_ps = Column(String(length=1))
        dp_cs = Column(String(length=1))
        dp_hi = Column(Integer)
        dp_clt = Column(DateTime)
        dp_wings = Column(Text)
        dp_tra = Column(Text)
        dp_pde = Column(Text)
        dp_cde = Column(Text)
        dp_dc = Column(Integer)
        dp_l = Column(Text)
        dp_m = Column(JSON)

        f = Column(String(length=1))
        t = Column(Text)
        o = Column(Text)
        c = Column(Text)
        n = Column(Text)

        m = Column(JSON)
        hd = Column(JSON)
        hdc = Column(JSON)
        conn = Column(JSON)
        rtr = Column(JSON)

        station = Column(Text)
        id = Column(Text)
        hash_id = Column(BIGINT, primary_key=True)

    Base.metadata.create_all(engine)


class DatabaseOfDoom(RtdDbModel):
    Session = sessionmaker(bind=RtdDbModel.engine)
    session = Session()

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


    def add_row(self, plan, changes, bhf, date, hour):
        date = datetime.datetime.combine(date, datetime.time(hour, 0))
        self.queue.append({'date': date, 'bhf': bhf, 'plan': plan, 'changes':changes})
        if len(self.queue) > 20:
            self.commit()


    def commit(self):
        self.upsert(self.queue)
        self.queue = []
        self.session.commit()

    def get_json(self, bhf, date1, date2):
        if date1 is None:
            return self.session.query(self.JsonRtd).filter((self.JsonRtd.bhf == bhf)).all()
        if date2 is None:
            return self.session.query(self.JsonRtd).filter((self.JsonRtd.bhf == bhf) & (self.JsonRtd.date == date1)).first()
        return self.session.query(self.JsonRtd).filter((self.JsonRtd.bhf == bhf)
            & (self.JsonRtd.date >= date1)
            & (self.JsonRtd.date < date2)).all()

    def count_entrys_at_date(self, date):
        return self.session.query(self.JsonRtd).filter(self.JsonRtd.date == date).count()

    def max_date(self):
        return self.session.query(func.max(self.Rtd.ar_pt))

    def max_date(self):
        return self.session.query(sqlalchemy.func.max(self.Rtd.ar_pt)).scalar()


if __name__ == "__main__":
    db = DatabaseOfDoom()
    # print(db.max_date())
    print(db.count_entrys_at_date(datetime.datetime(2020, 7, 2, 0)))
    print(db.max_date())