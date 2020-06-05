import pandas as pd
import os
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

from config import db_database, db_password, db_server, db_username

class RtdRay:
    LOCAL_BUFFER_PATH = 'data_buffer/rtd_local_buffer'
    DB_CONNECT_STRING = 'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'

    def __init__(self):
        pass

    def refresh_local_buffer(self):
        with ProgressBar():
            rtd = dd.read_sql_table('rtd', self.DB_CONNECT_STRING, index_col='index', head_rows=10000)
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='fastparquet')

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
    print(rtd_d.load_data())