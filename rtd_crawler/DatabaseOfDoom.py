import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime

from config import db_database, db_password, db_server, db_username


class DatabaseOfDoom:
    engine = sqlalchemy.create_engine(
            'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require',
            pool_pre_ping=True
        )

    Session = sessionmaker(bind=engine)
    session = Session()

    Base = declarative_base()

    class JsonRtd(Base):
        __tablename__ = 'json_rtd_v2'
        date = Column(DateTime, primary_key=True)
        bhf = Column(Text, primary_key=True)
        plan = Column(JSON)
        changes = Column(JSON)

    Base.metadata.create_all(engine)

    # json_rtd_object = JsonRtd(date=datetime.datetime(2020, 2, 17, 8, 15),
    #     bhf='Test Hbf',
    #     plan={'plan':'test'},
    #     changes={'changes':'test'})

    # json_rtd_object2 = JsonRtd(date=datetime.datetime(2021, 2, 17, 8, 15),
    #     bhf='Test Hbf',
    #     plan={'plan':'test'},
    #     changes={'changes':'test'})

    # session.bulk_save_objects([json_rtd_object, json_rtd_object2])
    # session.commit()
    # print(session.query(JsonRtd).all())

    def __init__(self):
        self.merges = 0

    def upload_json(self, plan, changes, bhf, date, hour):
        date = datetime.datetime.combine(date, datetime.time(hour, 0))
        new_row = self.JsonRtd(date=date, bhf=bhf, plan=plan, changes=changes)
        self.session.merge(new_row)

    def commit(self):
        self.session.commit()

    def get_json(self, bhf, date):
        result = self.session.query(self.JsonRtd).filter(self.JsonRtd.bhf == bhf & self.JsonRtd.date == date).all()
        print(result)

    def count_entrys_at_date(self, date):
        return self.session.query(self.JsonRtd).filter(self.JsonRtd.date == date).count()


if __name__ == "__main__":
    db = DatabaseOfDoom()
    print(db.count_entrys_at_date(datetime.datetime(2020, 6, 11, 17)))
    # result = db.session.query(db.JsonRtd).first()
    # print(result)
    # # db.get_json('Tübingen Hbf', None)
    # print('lol')