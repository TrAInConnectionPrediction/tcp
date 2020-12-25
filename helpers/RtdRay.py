import os, sys
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
Output of \d+ recent_change_rtd
                                                                   Table "public.recent_change_rtd"
      Column       |             Type              | Collation | Nullable |                      Default                       | Storage  | Stats target | Description 
-------------------+-------------------------------+-----------+----------+----------------------------------------------------+----------+--------------+-------------
 ar_ppth           | text[]                        |           |          |                                                    | extended |              | 
 ar_cpth           | text[]                        |           |          |                                                    | extended |              | 
 ar_pp             | text                          |           |          |                                                    | extended |              | 
 ar_cp             | text                          |           |          |                                                    | extended |              | 
 ar_pt             | timestamp without time zone   |           |          |                                                    | plain    |              | 
 ar_ct             | timestamp without time zone   |           |          |                                                    | plain    |              | 
 ar_ps             | character varying(1)          |           |          |                                                    | extended |              | 
 ar_cs             | character varying(1)          |           |          |                                                    | extended |              | 
 ar_hi             | integer                       |           |          |                                                    | plain    |              | 
 ar_clt            | timestamp without time zone   |           |          |                                                    | plain    |              | 
 ar_wings          | text                          |           |          |                                                    | extended |              | 
 ar_tra            | text                          |           |          |                                                    | extended |              | 
 ar_pde            | text                          |           |          |                                                    | extended |              | 
 ar_cde            | text                          |           |          |                                                    | extended |              | 
 ar_dc             | integer                       |           |          |                                                    | plain    |              | 
 ar_l              | text                          |           |          |                                                    | extended |              | 
 ar_m_id           | text[]                        |           |          |                                                    | extended |              | 
 ar_m_t            | character varying(1)[]        |           |          |                                                    | extended |              | 
 ar_m_ts           | timestamp without time zone[] |           |          |                                                    | extended |              | 
 ar_m_c            | integer[]                     |           |          |                                                    | extended |              | 
 dp_ppth           | text[]                        |           |          |                                                    | extended |              | 
 dp_cpth           | text[]                        |           |          |                                                    | extended |              | 
 dp_pp             | text                          |           |          |                                                    | extended |              | 
 dp_cp             | text                          |           |          |                                                    | extended |              | 
 dp_pt             | timestamp without time zone   |           |          |                                                    | plain    |              | 
 dp_ct             | timestamp without time zone   |           |          |                                                    | plain    |              | 
 dp_ps             | character varying(1)          |           |          |                                                    | extended |              | 
 dp_cs             | character varying(1)          |           |          |                                                    | extended |              | 
 dp_hi             | integer                       |           |          |                                                    | plain    |              | 
 dp_clt            | timestamp without time zone   |           |          |                                                    | plain    |              | 
 dp_wings          | text                          |           |          |                                                    | extended |              | 
 dp_tra            | text                          |           |          |                                                    | extended |              | 
 dp_pde            | text                          |           |          |                                                    | extended |              | 
 dp_cde            | text                          |           |          |                                                    | extended |              | 
 dp_dc             | integer                       |           |          |                                                    | plain    |              | 
 dp_l              | text                          |           |          |                                                    | extended |              | 
 dp_m_id           | text[]                        |           |          |                                                    | extended |              | 
 dp_m_t            | character varying(1)[]        |           |          |                                                    | extended |              | 
 dp_m_ts           | timestamp without time zone[] |           |          |                                                    | extended |              | 
 dp_m_c            | integer[]                     |           |          |                                                    | extended |              | 
 f                 | character varying(1)          |           |          |                                                    | extended |              | 
 t                 | text                          |           |          |                                                    | extended |              | 
 o                 | text                          |           |          |                                                    | extended |              | 
 c                 | text                          |           |          |                                                    | extended |              | 
 n                 | text                          |           |          |                                                    | extended |              | 
 m_id              | text[]                        |           |          |                                                    | extended |              | 
 m_t               | character varying(1)[]        |           |          |                                                    | extended |              | 
 m_ts              | timestamp without time zone[] |           |          |                                                    | extended |              | 
 m_c               | integer[]                     |           |          |                                                    | extended |              | 
 hd                | json                          |           |          |                                                    | extended |              | 
 hdc               | json                          |           |          |                                                    | extended |              | 
 conn              | json                          |           |          |                                                    | extended |              | 
 rtr               | json                          |           |          |                                                    | extended |              | 
 distance_to_start | double precision              |           |          |                                                    | plain    |              | 
 distance_to_end   | double precision              |           |          |                                                    | plain    |              | 
 distance_to_last  | double precision              |           |          |                                                    | plain    |              | 
 distance_to_next  | double precision              |           |          |                                                    | plain    |              | 
 station           | text                          |           |          |                                                    | extended |              | 
 id                | text                          |           |          |                                                    | extended |              | 
 dayly_id          | bigint                        |           |          |                                                    | plain    |              | 
 date_id           | timestamp without time zone   |           |          |                                                    | plain    |              | 
 stop_id           | integer                       |           |          |                                                    | plain    |              | 
 hash_id           | bigint                        |           | not null | nextval('recent_change_rtd_hash_id_seq'::regclass) | plain    |              | 
"""


class RtdRay(Rtd):
    df_dict = {
        'ar_ppth': pd.Series([], dtype='object'),
        'ar_cpth': pd.Series([], dtype='object'),
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
        'ar_m_id': pd.Series([], dtype='object'),
        'ar_m_t': pd.Series([], dtype='object'),
        'ar_m_ts': pd.Series([], dtype='object'),
        'ar_m_c': pd.Series([], dtype='object'),

        'dp_ppth': pd.Series([], dtype='object'),
        'dp_cpth': pd.Series([], dtype='object'),
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
        'dp_m_id': pd.Series([], dtype='object'),
        'dp_m_t': pd.Series([], dtype='object'),
        'dp_m_ts': pd.Series([], dtype='object'),
        'dp_m_c': pd.Series([], dtype='object'),

        'f': pd.Series([], dtype='str'),
        't': pd.Series([], dtype='str'),
        'o': pd.Series([], dtype='str'),
        'c': pd.Series([], dtype='str'),
        'n': pd.Series([], dtype='str'),

        'm_id': pd.Series([], dtype='object'),
        'm_t': pd.Series([], dtype='object'),
        'm_ts': pd.Series([], dtype='object'),
        'm_c': pd.Series([], dtype='object'),
        'hd': pd.Series([], dtype='object'),
        'hdc': pd.Series([], dtype='object'),
        'conn': pd.Series([], dtype='object'),
        'rtr': pd.Series([], dtype='object'),

        'distance_to_start': pd.Series([], dtype='float'),
        'distance_to_end': pd.Series([], dtype='float'),
        'distance_to_last': pd.Series([], dtype='float'),
        'distance_to_next': pd.Series([], dtype='float'),

        'station': pd.Series([], dtype='str'),
        'id': pd.Series([], dtype='str'),
        'dayly_id': pd.Series([], dtype='Int64'),
        'date_id': pd.Series([], dtype='Int64'),
        'stop_id': pd.Series([], dtype='Int64')
    }

    arr_cols = ['ar_ppth', 'ar_cpth', 'ar_m_id', 'ar_m_t', 'ar_m_ts', 'ar_m_c',
                'dp_ppth', 'dp_cpth', 'dp_m_id', 'dp_m_t', 'dp_m_ts', 'dp_m_c',
                'm_id', 'm_t', 'm_ts', 'm_c']

    def __init__(self, notebook=False):
        self.meta = dd.from_pandas(pd.DataFrame(self.df_dict), npartitions=1).persist()
        self.no_array_meta = dd.from_pandas(pd.DataFrame({key: self.df_dict[key] for key in self.df_dict if key not in self.arr_cols}), npartitions=1).persist()
        if notebook:
            self.LOCAL_BUFFER_PATH = '../data_buffer/' + self.__tablename__ + '_local_buffer'
        else:
            self.LOCAL_BUFFER_PATH = 'data_buffer/' + self.__tablename__ + '_local_buffer'

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
            new_rtd = self.catagorize(new_rtd)
            # Remove changes from rtd that are also present in new_rtd
            # rtd = rtd.loc[~rtd.index.isin(new_rtd.index), :]
            rtd = dd.concat([rtd, new_rtd], axis=0, sort=False, ignore_index=False)
            # rtd.drop_duplicates(keep='last', subset=['index'])
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow')

    def refresh_local_buffer(self):
        """
        Pull the hole rtd table from db and save it on disk. This takes a while.
        """
        with ProgressBar():
            from sqlalchemy import Column
            from sqlalchemy.exc import ProgrammingError
            with engine.connect() as connection:
                try:
                    query = sql.select([Column(c) for c in self.df_dict if c not in self.arr_cols] + [Column('hash_id')])\
                        .select_from(sql.table(Rtd.__tablename__))\
                        .alias('no_array_rtd')
                    view_query = 'CREATE MATERIALIZED VIEW no_array_rtd AS {}'.format(str(query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})))
                    connection.execute(view_query)
                except ProgrammingError:
                    connection.execute('REFRESH MATERIALIZED VIEW no_array_rtd')

            rtd = dd.read_sql_table('no_array_rtd_mater', DB_CONNECT_STRING,
                                    index_col='hash_id', meta=self.no_array_meta, npartitions=200)
            rtd = self.catagorize(rtd)
            # rtd = rtd.map_partitions(RtdRay.catagorize) # , meta=rtd
            # Save data to parquet. We have to use pyarrow as fastparquet does not support pd.Int64
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow', write_metadata_file=False)

    @staticmethod
    def catagorize(rtd):
        """
        Change dtype of categorical like columns to 'category'
        Parameters
        ----------
        rtd: dask.dataframe

        Returns
        -------

        """
        rtd = rtd.astype({'f': 'category', 't': 'category', 'o': 'category',
                          'c': 'category', 'n': 'category', 'ar_ps': 'category',
                          'ar_pp': 'category', 'dp_ps': 'category', 'dp_pp': 'category',
                          'station': 'category'})
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
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow', **kwargs)
        except FileNotFoundError:
            print('There was no buffer found. A new buffer will be downloaded from the db. This will take a while.')
            self.refresh_local_buffer()
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow', **kwargs)

        return data


if __name__ == "__main__":
    import fancy_print_tcp
    from dask.distributed import Client
    client = Client()
    rtd_d = RtdRay()
    rtd_d.refresh_local_buffer()
    # rtd_d.update_local_buffer()
    #
    # rtd = rtd_d.load_data()
