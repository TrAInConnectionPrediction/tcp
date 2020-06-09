import pandas as pd
import os
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

from config import db_database, db_password, db_server, db_username

class RtdRay:
    
    DB_CONNECT_STRING = 'postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'
    df_dict = {
        'platform': pd.Series([], dtype='str'),
        'arr': pd.Series([], dtype='Int64'),
        'dep': pd.Series([], dtype='Int64'),
        'stay_time': pd.Series([], dtype='Int64'),
        'pla_arr_path': pd.Series([], dtype='str'),
        'pla_dep_path': pd.Series([], dtype='str'),
        'train_type': pd.Series([], dtype='str'),
        'train_number': pd.Series([], dtype='str'),
        'product_class': pd.Series([], dtype='str'),
        'trip_type': pd.Series([], dtype='str'),
        'owner': pd.Series([], dtype='str'),
        'first_id': pd.Series([], dtype='Int64'),
        'middle_id': pd.Series([], dtype='Int64'),
        'last_id': pd.Series([], dtype='Int64'),
        'arr_changed_path': pd.Series([], dtype='str'),
        'arr_changed_platform': pd.Series([], dtype='str'),
        'arr_changed_time': pd.Series([], dtype='Int64'),
        'arr_changed_status': pd.Series([], dtype='str'),
        'arr_cancellation_time': pd.Series([], dtype='Int64'),
        'arr_line': pd.Series([], dtype='str'),
        'arr_message': pd.Series([], dtype='str'),
        'dep_changed_path': pd.Series([], dtype='str'),
        'dep_changed_platform': pd.Series([], dtype='str'),
        'dep_changed_time': pd.Series([], dtype='Int64'),
        'dep_changed_status': pd.Series([], dtype='str'),
        'dep_cancellation_time': pd.Series([], dtype='Int64'),
        'dep_line': pd.Series([], dtype='str'),
        'dep_message': pd.Series([], dtype='str'),
        'station':pd.Series([], dtype='str')
    }

    meta = dd.from_pandas(pd.DataFrame(df_dict), npartitions=1)

    def __init__(self, notebook=False):
        if notebook:
            self.LOCAL_BUFFER_PATH = '../data_buffer/rtd_local_buffer'
        else:
            self.LOCAL_BUFFER_PATH = 'data_buffer/rtd_local_buffer'

    def refresh_local_buffer(self):
        with ProgressBar():
            rtd = dd.read_sql_table('rtd', self.DB_CONNECT_STRING, index_col='index', meta=self.meta, npartitions=111)
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
    rtd_d.refresh_local_buffer()
    print(rtd_d.load_data())