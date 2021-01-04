import os, sys
import pickle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from database.rtd import Rtd, sql_types
from database.engine import engine, DB_CONNECT_STRING
from sqlalchemy.dialects import postgresql
import datetime
from sqlalchemy import sql

"""
Table "public.recent_change_rtd"
      Column       |             Type              | Storage  | Stats target | Description 
-------------------+-------------------------------+----------+--------------+-------------
 ar_ppth           | text[]                        | extended |              | planned path
 ar_cpth           | text[]                        | extended |              | changed path
 ar_pp             | text                          | extended |              | planned platform
 ar_cp             | text                          | extended |              | changed platform
 ar_pt             | timestamp without time zone   | plain    |              | planned arrival time
 ar_ct             | timestamp without time zone   | plain    |              | changed arrival time
 ar_ps             | character varying(1)          | extended |              | planned status
 ar_cs             | character varying(1)          | extended |              | changed status
 ar_hi             | integer                       | plain    |              | hidden
 ar_clt            | timestamp without time zone   | plain    |              | time the arrival was canceled
 ar_wings          | text                          | extended |              | wings
 ar_tra            | text                          | extended |              | transition
 ar_pde            | text                          | extended |              | planned distant endpoint
 ar_cde            | text                          | extended |              | changed distant endpoint
 ar_dc             | integer                       | plain    |              | 
 ar_l              | text                          | extended |              | line
 ar_m_id           | text[]                        | extended |              | message id
 ar_m_t            | character varying(1)[]        | extended |              | message type
 ar_m_ts           | timestamp without time zone[] | extended |              | message timestamp
 ar_m_c            | integer[]                     | extended |              | message code
 dp_ppth           | text[]                        | extended |              | 
 dp_cpth           | text[]                        | extended |              | 
 dp_pp             | text                          | extended |              | 
 dp_cp             | text                          | extended |              | 
 dp_pt             | timestamp without time zone   | plain    |              | 
 dp_ct             | timestamp without time zone   | plain    |              | 
 dp_ps             | character varying(1)          | extended |              | 
 dp_cs             | character varying(1)          | extended |              | 
 dp_hi             | integer                       | plain    |              | 
 dp_clt            | timestamp without time zone   | plain    |              | 
 dp_wings          | text                          | extended |              | 
 dp_tra            | text                          | extended |              | 
 dp_pde            | text                          | extended |              | 
 dp_cde            | text                          | extended |              | 
 dp_dc             | integer                       | plain    |              | 
 dp_l              | text                          | extended |              | 
 dp_m_id           | text[]                        | extended |              | 
 dp_m_t            | character varying(1)[]        | extended |              | 
 dp_m_ts           | timestamp without time zone[] | extended |              | 
 dp_m_c            | integer[]                     | extended |              | 
 f                 | character varying(1)          | extended |              | 
 t                 | text                          | extended |              | 
 o                 | text                          | extended |              | 
 c                 | text                          | extended |              | 
 n                 | text                          | extended |              | 
 m_id              | text[]                        | extended |              | 
 m_t               | character varying(1)[]        | extended |              | 
 m_ts              | timestamp without time zone[] | extended |              | 
 m_c               | integer[]                     | extended |              | 
 hd                | json                          | extended |              | 
 hdc               | json                          | extended |              | 
 conn              | json                          | extended |              | 
 rtr               | json                          | extended |              | 
 distance_to_start | double precision              | plain    |              | 
 distance_to_end   | double precision              | plain    |              | 
 distance_to_last  | double precision              | plain    |              | 
 distance_to_next  | double precision              | plain    |              | 
 station           | text                          | extended |              | 
 id                | text                          | extended |              | 
 dayly_id          | bigint                        | plain    |              | 
 date_id           | timestamp without time zone   | plain    |              | 
 stop_id           | integer                       | plain    |              | 
 hash_id           | bigint                        | plain    |              | 
"""


class RtdRay(Rtd):
    df_dict = {
        # 'ar_ppth': pd.Series([], dtype='object'),
        # 'ar_cpth': pd.Series([], dtype='object'),
        'ar_pp': pd.Series([], dtype='str'),
        'ar_cp': pd.Series([], dtype='str'),
        'ar_pt': pd.Series([], dtype='datetime64[ns]'),
        'ar_ct': pd.Series([], dtype='datetime64[ns]'),
        'ar_ps': pd.Series([], dtype='str'),
        'ar_cs': pd.Series([], dtype='str'),
        'ar_hi': pd.Series([], dtype='Int64'),
        'ar_clt': pd.Series([], dtype='datetime64[ns]'),
        'ar_wings': pd.Series([], dtype='str'),
        'ar_tra': pd.Series([], dtype='str'),
        'ar_pde': pd.Series([], dtype='str'),
        'ar_cde': pd.Series([], dtype='str'),
        'ar_dc': pd.Series([], dtype='Int64'),
        'ar_l': pd.Series([], dtype='str'),
        # 'ar_m_id': pd.Series([], dtype='object'),
        # 'ar_m_t': pd.Series([], dtype='object'),
        # 'ar_m_ts': pd.Series([], dtype='object'),
        # 'ar_m_c': pd.Series([], dtype='object'),

        # 'dp_ppth': pd.Series([], dtype='object'),
        # 'dp_cpth': pd.Series([], dtype='object'),
        'dp_pp': pd.Series([], dtype='str'),
        'dp_cp': pd.Series([], dtype='str'),
        'dp_pt': pd.Series([], dtype='datetime64[ns]'),
        'dp_ct': pd.Series([], dtype='datetime64[ns]'),
        'dp_ps': pd.Series([], dtype='str'),
        'dp_cs': pd.Series([], dtype='str'),
        'dp_hi': pd.Series([], dtype='Int64'),
        'dp_clt': pd.Series([], dtype='datetime64[ns]'),
        'dp_wings': pd.Series([], dtype='str'),
        'dp_tra': pd.Series([], dtype='str'),
        'dp_pde': pd.Series([], dtype='str'),
        'dp_cde': pd.Series([], dtype='str'),
        'dp_dc': pd.Series([], dtype='Int64'),
        'dp_l': pd.Series([], dtype='str'),
        # 'dp_m_id': pd.Series([], dtype='object'),
        # 'dp_m_t': pd.Series([], dtype='object'),
        # 'dp_m_ts': pd.Series([], dtype='object'),
        # 'dp_m_c': pd.Series([], dtype='object'),

        'f': pd.Series([], dtype='str'),
        't': pd.Series([], dtype='str'),
        'o': pd.Series([], dtype='str'),
        'c': pd.Series([], dtype='str'),
        'n': pd.Series([], dtype='str'),

        # 'm_id': pd.Series([], dtype='object'),
        # 'm_t': pd.Series([], dtype='object'),
        # 'm_ts': pd.Series([], dtype='object'),
        # 'm_c': pd.Series([], dtype='object'),
        # 'hd': pd.Series([], dtype='object'),
        # 'hdc': pd.Series([], dtype='object'),
        # 'conn': pd.Series([], dtype='object'),
        # 'rtr': pd.Series([], dtype='object'),

        'distance_to_start': pd.Series([], dtype='float'),
        'distance_to_end': pd.Series([], dtype='float'),
        'distance_to_last': pd.Series([], dtype='float'),
        'distance_to_next': pd.Series([], dtype='float'),

        'station': pd.Series([], dtype='str'),
        'id': pd.Series([], dtype='str'),
        'dayly_id': pd.Series([], dtype='Int64'),
        'date_id': pd.Series([], dtype='datetime64[ns]'),
        'stop_id': pd.Series([], dtype='Int64')
    }

    arr_cols = ['ar_ppth', 'ar_cpth', 'ar_m_id', 'ar_m_t', 'ar_m_ts', 'ar_m_c',
                'dp_ppth', 'dp_cpth', 'dp_m_id', 'dp_m_t', 'dp_m_ts', 'dp_m_c',
                'm_id', 'm_t', 'm_ts', 'm_c']

    def __init__(self, notebook=False):
        self.meta = dd.from_pandas(pd.DataFrame(self.df_dict), npartitions=1).persist()
        self.no_array_meta = dd.from_pandas(pd.DataFrame({key: self.df_dict[key] for key in self.df_dict if key not in self.arr_cols}), npartitions=1).persist()
        if notebook:
            self.DATA_CACHE_PATH = '../cache/' + self.__tablename__ + '_local_buffer2'
            self.ENCODER_PATH = '../cache/' + self.__tablename__ + '_encoder'
        else:
            self.DATA_CACHE_PATH = 'cache/' + self.__tablename__ + '_local_buffer2'
            self.ENCODER_PATH = 'cache/' + self.__tablename__ + '_encoder'

    @staticmethod
    def get_delays(rtd_df):
        rtd_df['ar_cancellations'] = rtd_df['ar_cs'] != 'c'
        rtd_df['ar_cancellation_time_delta'] = (rtd_df['ar_clt'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
        rtd_df['ar_delay'] = (rtd_df['ar_ct'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
        ar_mask = (rtd_df['ar_cs'] != 'c') & (rtd_df['ar_delay'].notnull())
        rtd_df['ar_on_time_5'] = rtd_df.loc[ar_mask, 'ar_delay'] < 6

        rtd_df['dp_cancellations'] = rtd_df['dp_cs'] != 'c'
        rtd_df['dp_cancellation_time_delta'] = (rtd_df['dp_clt'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
        rtd_df['dp_delay'] = (rtd_df['dp_ct'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
        dp_mask = (rtd_df['dp_cs'] != 'c') & (rtd_df['dp_delay'].notnull())
        rtd_df['dp_on_time_5'] = rtd_df.loc[dp_mask, 'dp_delay'] < 6

        return rtd_df

    def update_local_buffer(self):
        rtd = self.load_data()
        max_date = rtd['ar_pt'].max().compute() - datetime.timedelta(days=2)
        max_date = max_date.to_pydatetime()

        from sqlalchemy import Column, DateTime
        with engine.connect() as connection:
            query = sql.select([Column(c) for c in self.df_dict if c not in self.arr_cols] + [Column('hash_id')])\
                .where((Column('ar_pt', DateTime) > str(max_date)) | (Column('dp_pt', DateTime) > str(max_date)))\
                .select_from(sql.table(Rtd.__tablename__))\
                .alias('new_rtd_mat')
            view_query = 'CREATE MATERIALIZED VIEW new_rtd_mat AS {}'.format(str(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})))
            connection.execute(view_query)
            new_rtd = dd.read_sql_table('new_rtd_mat', DB_CONNECT_STRING,
                                        index_col='hash_id', meta=self.meta, npartitions=200)
            # new_rtd.to_parquet(self.LOCAL_BUFFER_PATH + '_new', engine='pyarrow')
            # new_rtd = dd.read_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow')
            new_rtd = self.categorize(new_rtd)
            # Remove changes from rtd that are also present in new_rtd
            # rtd = rtd.loc[~rtd.index.isin(new_rtd.index), :]
            rtd = dd.concat([rtd, new_rtd], axis=0, sort=False, ignore_index=False)
            # rtd.drop_duplicates(keep='last', subset=['index'])
            rtd.to_parquet(self.DATA_CACHE_PATH, engine='pyarrow')

    def refresh_local_buffer(self):
        """
        Pull the hole rtd table from db and save it on disk. This takes a while.
        """
        with ProgressBar():
            rtd = dd.read_sql_table(self.__tablename__, DB_CONNECT_STRING,
                                    index_col='hash_id', meta=self.no_array_meta, npartitions=200)
            rtd = self.get_delays(rtd)
            rtd = self.categorize(rtd)
            # rtd = rtd.map_partitions(RtdRay.catagorize) # , meta=rtd
            # Save data to parquet. We have to use pyarrow as fastparquet does not support pd.Int64
            rtd.to_parquet(self.DATA_CACHE_PATH, engine='pyarrow', write_metadata_file=False)

    def categorize(self, rtd):
        """
        Change dtype of categorical like columns to 'category'
        Parameters
        ----------
        rtd: dask.dataframe

        Returns
        -------
        dask.dataframe
            Dataframe with categorical columns as dtype category

        """
        rtd = rtd.astype({'f': 'category', 't': 'category', 'o': 'category',
                          'c': 'category', 'n': 'category', 'ar_ps': 'category',
                          'ar_pp': 'category', 'dp_ps': 'category', 'dp_pp': 'category',
                          'station': 'category'})
        from dask_ml import preprocessing
        for key in ['o', 'c', 'n', 'station']:
            le = preprocessing.LabelEncoder()
            le.fit(rtd[key])
            tranformed = le.transform(le.classes_)
            pickle.dump(pd.DataFrame(index=le.classes_, data=tranformed).to_dict()[0], open(self.ENCODER_PATH + key + '_dict.pkl', "wb"))
            pickle.dump(le, open(self.ENCODER_PATH + key + '_orig.pkl', "wb"))

        return rtd

    def load_data(self, **kwargs):
        """
        Try to load data from disk. If not present, pull db to disk and then open it.
        It may not work after the data was pulled from db (unicode decode error).
        Deleting _metadata and _common_metadata will resolve this.

        Parameters
        ----------
        kwargs
            kwargs passed to dask.dataframe.read_parquet()

        Returns
        -------
        dask.DataFrame
            dask.DataFrame containing the loaded data
        """
        try:
            data = dd.read_parquet(self.DATA_CACHE_PATH, engine='pyarrow', **kwargs)
        except FileNotFoundError:
            print('There was no buffer found. A new buffer will be downloaded from the db. This will take a while.')
            self.refresh_local_buffer()
            data = dd.read_parquet(self.DATA_CACHE_PATH, engine='pyarrow', **kwargs)

        return data

    def load_for_ml_model(self, max_date=None, min_date=None, return_date_id=False):
        rtd = self.load_data(columns=['station',
                                      'o',
                                      'c',
                                      'n',
                                      'distance_to_start',
                                      'distance_to_end',
                                      'ar_delay',
                                      'dp_delay',
                                      'date_id',
                                      'ar_ct',
                                      'ar_pt',
                                      'dp_ct',
                                      'dp_pt'])
        if min_date and max_date:
            rtd = rtd.loc[(rtd['date_id'] > min_date) & (rtd['date_id'] < max_date)]

        rtd['hour'] = rtd['ar_pt'].dt.hour
        rtd['hour'] = rtd['hour'].fillna(value=rtd.loc[:, 'dp_pt'].dt.hour)
        rtd['day'] = rtd['ar_pt'].dt.dayofweek
        rtd['day'] = rtd['day'].fillna(value=rtd.loc[:, 'dp_pt'].dt.dayofweek)
        for key in ['o', 'c', 'n', 'station']:
            le = pickle.load(open(self.ENCODER_PATH + key + '_orig.pkl', "rb"))
            rtd[key] = le.transform(rtd[key])
        if return_date_id:
            return rtd.drop(columns=['ar_ct',
                                     'ar_pt',
                                     'dp_ct',
                                     'dp_pt'], axis=0)
        else:
            return rtd.drop(columns=['date_id',
                                     'ar_ct',
                                     'ar_pt',
                                     'dp_ct',
                                     'dp_pt'], axis=0)


if __name__ == "__main__":
    import helpers.fancy_print_tcp
    from dask.distributed import Client
    client = Client()
    rtd_d = RtdRay()
    # rtd_d.refresh_local_buffer()
    # rtd_d.update_local_buffer()
    #
    # rtd = rtd_d.load_data()
    # rtd = rtd_d.categorize(rtd)

    rtd = rtd_d.load_for_ml_model()
