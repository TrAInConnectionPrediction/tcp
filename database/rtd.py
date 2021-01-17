import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pangres
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime, String, BIGINT, Float
from sqlalchemy.dialects.postgresql import JSON, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from database.engine import engine
import datetime
from sqlalchemy import exc


Base = declarative_base()


class Rtd(Base):
    """
    Scheme for parsed data.
    """
    __tablename__ = 'recent_change_rtd'
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

    f = Column(String(length=1))
    t = Column(Text)
    o = Column(Text)
    c = Column(Text)
    n = Column(Text)

    distance_to_start = Column(Float)
    distance_to_end = Column(Float)
    distance_to_last = Column(Float)
    distance_to_next = Column(Float)

    station = Column(Text)
    id = Column(Text)
    dayly_id = Column(BIGINT)
    date_id = Column(DateTime)
    stop_id = Column(Integer)
    hash_id = Column(BIGINT, primary_key=True)


class RtdArrays(Base):
    __tablename__ = 'recent_change_rtd_arrays'
    ar_ppth = Column(ARRAY(Text))
    ar_cpth = Column(ARRAY(Text))
    ar_m_id = Column(ARRAY(Text))
    ar_m_t = Column(ARRAY(String(length=1)))
    ar_m_ts = Column(ARRAY(DateTime))
    ar_m_c = Column(ARRAY(Integer))

    dp_ppth = Column(ARRAY(Text))
    dp_cpth = Column(ARRAY(Text))
    dp_m_id = Column(ARRAY(Text))
    dp_m_t = Column(ARRAY(String(length=1)))
    dp_m_ts = Column(ARRAY(DateTime))
    dp_m_c = Column(ARRAY(Integer))

    m_id = Column(ARRAY(Text))
    m_t = Column(ARRAY(String(length=1)))
    m_ts = Column(ARRAY(DateTime))
    m_c = Column(ARRAY(Integer))

    hd = Column(JSON)
    hdc = Column(JSON)
    conn = Column(JSON)
    rtr = Column(JSON)
    
    hash_id = Column(BIGINT, primary_key=True)


# This is the same as Rtd(Base) but as dict. Pangres cannot use the Rtd(Base) class
sql_types = {
    'ar_ppth': ARRAY(Text),
    'ar_cpth': ARRAY(Text),
    'ar_pp': Text,
    'ar_cp': Text,
    'ar_pt': DateTime,
    'ar_ct': DateTime,
    'ar_ps': String(length=1),
    'ar_cs': String(length=1),
    'ar_hi': Integer,
    'ar_clt': DateTime,
    'ar_wings': Text,
    'ar_tra': Text,
    'ar_pde': Text,
    'ar_cde': Text,
    'ar_dc': Integer,
    'ar_l': Text,
    'ar_m_id': ARRAY(Text),
    'ar_m_t': ARRAY(String(length=1)),
    'ar_m_ts': ARRAY(DateTime),
    'ar_m_c': ARRAY(Integer),

    'dp_ppth': ARRAY(Text),
    'dp_cpth': ARRAY(Text),
    'dp_pp': Text,
    'dp_cp': Text,
    'dp_pt': DateTime,
    'dp_ct': DateTime,
    'dp_ps': String(length=1),
    'dp_cs': String(length=1),
    'dp_hi': Integer,
    'dp_clt': DateTime,
    'dp_wings': Text,
    'dp_tra': Text,
    'dp_pde': Text,
    'dp_cde': Text,
    'dp_dc': Integer,
    'dp_l': Text,
    'dp_m_id': ARRAY(Text),
    'dp_m_t': ARRAY(String(length=1)),
    'dp_m_ts': ARRAY(DateTime),
    'dp_m_c': ARRAY(Integer),

    'f': String(length=1),
    't': Text,
    'o': Text,
    'c': Text,
    'n': Text,

    'm_id': ARRAY(Text),
    'm_t': ARRAY(String(length=1)),
    'm_ts': ARRAY(DateTime),
    'm_c': ARRAY(Integer),
    'hd': JSON,
    'hdc': JSON,
    'conn': JSON,
    'rtr': JSON,

    'station': Text,
    'id': Text,
    'dayly_id': BIGINT,
    'date_id': DateTime,
    'stop_id': Integer,
    'hash_id': Integer,
    
    'distance_to_start': Float,
    'distance_to_end': Float,
    'distance_to_last': Float,
    'distance_to_next': Float
}

try:
    Base.metadata.create_all(engine)
except sqlalchemy.exc.OperationalError:
    print('database.rtd running offline!')


class RtdManager:
    Session = sessionmaker(bind=engine)
    session = Session()

    def max_date(self) -> datetime.datetime:
        """
        Get the max date used in Rtd. Can be used to select missing data to parse

        Returns
        -------
        datetime.datetime
            The max date used in Rtd
        """
        return self.session.query(sqlalchemy.func.max(Rtd.ar_pt)).scalar()

    @staticmethod
    def upsert(df: pd.DataFrame):
        """
        Upsert dataframe to db using pangres

        Parameters
        ----------
        df: pd.DataFrame
            Data to upsert
        """
        if not df.empty:
            try:
                df.to_sql(name=Rtd.__tablename__,
                          if_exists='append',
                          con=engine,
                          method='multi',
                          dtype=sql_types)
            except exc.IntegrityError:
                pangres.upsert(engine,
                            df,
                            if_row_exists='update',
                            table_name=Rtd.__tablename__,
                            dtype=sql_types,
                            create_schema=False,
                            add_new_columns=False,
                            adapt_dtype_of_empty_db_columns=False)

    
    @staticmethod
    def upsert_arrays(df: pd.DataFrame):
        """
        Upsert dataframe to db using pangres

        Parameters
        ----------
        df: pd.DataFrame
            Arrays to upsert
        """
        if not df.empty:
            try:
                df.to_sql(name=RtdArrays.__tablename__,
                          if_exists='append',
                          con=engine,
                          method='multi',
                          dtype=sql_types)
            except exc.IntegrityError:
                pangres.upsert(engine,
                            df,
                            if_row_exists='update',
                            table_name=RtdArrays.__tablename__,
                            dtype=sql_types,
                            create_schema=False,
                            add_new_columns=False,
                            adapt_dtype_of_empty_db_columns=False)
