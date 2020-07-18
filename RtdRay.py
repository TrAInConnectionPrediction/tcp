import pandas as pd
import os
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

import sqlalchemy
from sqlalchemy import Column, Integer, Text, DateTime, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base

from config import db_database, db_password, db_server, db_username

class RtdRay:
    DB_CONNECT_STRING = 'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'
    df_dict = {
        # 'platform': pd.Series([], dtype='str'),
        # 'arr': pd.Series([], dtype='Int64'),
        # 'dep': pd.Series([], dtype='Int64'),
        # 'stay_time': pd.Series([], dtype='Int64'),
        # 'pla_arr_path': pd.Series([], dtype='str'),
        # 'pla_dep_path': pd.Series([], dtype='str'),
        # 'train_type': pd.Series([], dtype='str'),
        # 'train_number': pd.Series([], dtype='str'),
        # 'product_class': pd.Series([], dtype='str'),
        # 'trip_type': pd.Series([], dtype='str'),
        # 'owner': pd.Series([], dtype='str'),
        # 'first_id': pd.Series([], dtype='Int64'),
        # 'middle_id': pd.Series([], dtype='Int64'),
        # 'last_id': pd.Series([], dtype='Int64'),
        # 'arr_changed_path': pd.Series([], dtype='str'),
        # 'arr_changed_platform': pd.Series([], dtype='str'),
        # 'arr_changed_time': pd.Series([], dtype='Int64'),
        # 'arr_changed_status': pd.Series([], dtype='str'),
        # 'arr_cancellation_time': pd.Series([], dtype='Int64'),
        # 'arr_line': pd.Series([], dtype='str'),
        # 'arr_message': pd.Series([], dtype='str'),
        # 'dep_changed_path': pd.Series([], dtype='str'),
        # 'dep_changed_platform': pd.Series([], dtype='str'),
        # 'dep_changed_time': pd.Series([], dtype='Int64'),
        # 'dep_changed_status': pd.Series([], dtype='str'),
        # 'dep_cancellation_time': pd.Series([], dtype='Int64'),
        # 'dep_line': pd.Series([], dtype='str'),
        # 'dep_message': pd.Series([], dtype='str'),
        # 'station':pd.Series([], dtype='str')

        'ar_ppth': pd.Series([], dtype='str'),
        'ar_cpth': pd.Series([], dtype='str'),
        'ar_pp': pd.Series([], dtype='str'),
        'ar_cp': pd.Series([], dtype='str'),
        'ar_pt': pd.Series([], dtype='Int64'),
        'ar_ct': pd.Series([], dtype='Int64'),
        'ar_ps': pd.Series([], dtype='str'),
        'ar_cs': pd.Series([], dtype='str'),
        'ar_hi': pd.Series([], dtype='Int64'),
        'ar_clt': pd.Series([], dtype='Int64'),
        'ar_wings': pd.Series([], dtype='str'),
        'ar_tra': pd.Series([], dtype='str'),
        'ar_pde': pd.Series([], dtype='str'),
        'ar_cde': pd.Series([], dtype='str'),
        'ar_dc': pd.Series([], dtype='Int64'),
        'ar_l': pd.Series([], dtype='str'),
        'ar_m': pd.Series([], dtype='object'),

        'dp_ppth': pd.Series([], dtype='str'),
        'dp_cpth': pd.Series([], dtype='str'),
        'dp_pp': pd.Series([], dtype='str'),
        'dp_cp': pd.Series([], dtype='str'),
        'dp_pt': pd.Series([], dtype='Int64'),
        'dp_ct': pd.Series([], dtype='Int64'),
        'dp_ps': pd.Series([], dtype='str'),
        'dp_cs': pd.Series([], dtype='str'),
        'dp_hi': pd.Series([], dtype='Int64'),
        'dp_clt': pd.Series([], dtype='Int64'),
        'dp_wings': pd.Series([], dtype='str'),
        'dp_tra': pd.Series([], dtype='str'),
        'dp_pde': pd.Series([], dtype='str'),
        'dp_cde': pd.Series([], dtype='str'),
        'dp_dc': pd.Series([], dtype='Int64'),
        'dp_l': pd.Series([], dtype='str'),
        'dp_m': pd.Series([], dtype='object'),

        'f': pd.Series([], dtype='str'),
        't': pd.Series([], dtype='str'),
        'o': pd.Series([], dtype='str'),
        'c': pd.Series([], dtype='str'),
        'n': pd.Series([], dtype='str'),

        'm': pd.Series([], dtype='object'),
        'hd': pd.Series([], dtype='object'),
        'hdc': pd.Series([], dtype='object'),
        'conn': pd.Series([], dtype='object'),
        'rtr': pd.Series([], dtype='object'),

        'station': pd.Series([], dtype='str')
        # 'id': pd.Series([], dtype='str', primary_key=True)
    }

    meta = dd.from_pandas(pd.DataFrame(df_dict), npartitions=1)

    def __init__(self, tablename='rtd', notebook=False):
        self.tablename = tablename
        if notebook:
            self.LOCAL_BUFFER_PATH = '../data_buffer/' + self.tablename + '_local_buffer'
        else:
            self.LOCAL_BUFFER_PATH = 'data_buffer/' + self.tablename + '_local_buffer'

    def create_table_in_database(self):
        self.engine = sqlalchemy.create_engine(
                'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require',
                pool_pre_ping=True,
                pool_recycle=3600
            )

        self.Base = declarative_base()

        class SqlStruct(self.Base):
            __tablename__ = self.tablename
            ar_ppth = Column(Text)
            ar_cpth = Column(Text)
            ar_pp = Column(Text)
            ar_cp = Column(Text)
            ar_pt = Column(Integer)
            ar_ct = Column(Integer)
            ar_ps = Column(String(length=1))
            ar_cs = Column(String(length=1))
            ar_hi = Column(Integer)
            ar_clt = Column(Integer)
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
            dp_pt = Column(Integer)
            dp_ct = Column(Integer)
            dp_ps = Column(String(length=1))
            dp_cs = Column(String(length=1))
            dp_hi = Column(Integer)
            dp_clt = Column(Integer)
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
            id = Column(Text, primary_key=True)

        self.Base.metadata.create_all(self.engine, self.Base.metadata.tables.values(), checkfirst=True)

    def refresh_local_buffer(self):
        with ProgressBar():
            rtd = dd.read_sql_table(self.tablename, self.DB_CONNECT_STRING, index_col='id', meta=self.meta,
                divisions=['-9','-8','-7','-6','-5','-4','-3','-2','-1','0','1','2','3','4','5','6','7','8','9'])
            # rtd = rtd.set_index('index')
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow')

    def load_data(self, columns=None, filters=None):
        try:
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, columns=columns, filters=filters)
        except FileNotFoundError:
            print('there was no buffer found. A new buffer will be downloaded from the datebase. This will take a while.')
            self.refresh_local_buffer()
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, columns=columns, filters=filters)
        
        return data

if __name__ == "__main__":
    rtd_d = RtdRay()
    # rtd_d.create_table_in_database()
    rtd_d.refresh_local_buffer()
    # print(rtd_d.load_data())