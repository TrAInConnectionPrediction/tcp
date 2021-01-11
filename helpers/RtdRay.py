import os, sys
import pickle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from database.rtd import Rtd
from helpers.StationPhillip import StationPhillip
from database.engine import DB_CONNECT_STRING


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
 ar_dc             | integer                       | plain    |              | distant change
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

        'f': pd.Series([], dtype='str'),
        't': pd.Series([], dtype='str'),
        'o': pd.Series([], dtype='str'),
        'c': pd.Series([], dtype='str'),
        'n': pd.Series([], dtype='str'),

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

    def __init__(self, notebook=False):
        self.meta = dd.from_pandas(pd.DataFrame(self.df_dict), npartitions=1).persist()
        if notebook:
            self.DATA_CACHE_PATH = '../cache/' + self.__tablename__
            self.ENCODER_PATH = '../cache/' + self.__tablename__ + '_encoder'
        else:
            self.DATA_CACHE_PATH = 'cache/' + self.__tablename__
            self.ENCODER_PATH = 'cache/' + self.__tablename__ + '_encoder'

    @staticmethod
    def _get_delays(rtd):
        """
        Add cancellations, cancellation_time_delta, delay and on time to
        arrival and departure

        Parameters
        ----------
        rtd : dd.DataFrame
            result of self.load_data()

        Returns
        -------
        dd.DataFrame
            rtd with additional columns
        """
        rtd['ar_cancellations'] = rtd['ar_cs'] != 'c'
        rtd['ar_cancellation_time_delta'] = (rtd['ar_clt'] - rtd['ar_pt']) / pd.Timedelta(minutes=1)
        rtd['ar_delay'] = ((rtd['ar_ct'] - rtd['ar_pt']) / pd.Timedelta(minutes=1)).astype('Int64')
        ar_mask = (rtd['ar_cs'] != 'c') & (rtd['ar_delay'].notnull())
        rtd['ar_on_time_5'] = rtd.loc[ar_mask, 'ar_delay'] < 6

        rtd['dp_cancellations'] = rtd['dp_cs'] != 'c'
        rtd['dp_cancellation_time_delta'] = (rtd['dp_clt'] - rtd['dp_pt']) / pd.Timedelta(minutes=1)
        rtd['dp_delay'] = ((rtd['dp_ct'] - rtd['dp_pt']) / pd.Timedelta(minutes=1)).astype('Int64')
        dp_mask = (rtd['dp_cs'] != 'c') & (rtd['dp_delay'].notnull())
        rtd['dp_on_time_5'] = rtd.loc[dp_mask, 'dp_delay'] < 6

        return rtd

    @staticmethod
    def _add_station_coordinates(rtd):
        """Add latitude and logitude to rtd

        Parameters
        ----------
        rtd : pd.DataFrame
            Data to add the coordinates to            

        Returns
        -------
        pd.DataFrame
            DataFrame with columns lat and lon
        """
        stations = StationPhillip()
        for station in rtd['station'].unique():
            mask = rtd['station'] == station
            lon = stations.get_location(name=station)[0]
            rtd.loc[mask, 'lon'] = lon
            lat = stations.get_location(name=station)[1]
            rtd.loc[mask, 'lat'] = lat
        return rtd[['lat', 'lon']]

    def _categorize(self, rtd):
        """
        Change dtype of categorical like columns to 'category', compute categories
        and save the categories of each column to disk

        Parameters
        ----------
        rtd: dask.dataframe

        Returns
        -------
        dask.dataframe
            Dataframe with categorical columns as dtype category

        """
        with ProgressBar():
            categoricals = {'f': 'category', 't': 'category', 'o': 'category',
                            'c': 'category', 'n': 'category', 'ar_ps': 'category',
                            'ar_pp': 'category', 'dp_ps': 'category', 'dp_pp': 'category',
                            'station': 'category'}
            rtd = rtd.astype(categoricals)
            for col in categoricals.keys():
                print('categorizing', col)
                rtd[col] = rtd[col].cat.as_known()

        # Save categorical encoding as dicts to be used in production
        for key in ['o', 'c', 'n', 'station', 'ar_pp', 'dp_pp']:
            dict_keys = rtd[key].head(1).cat.categories.to_list()
            pickle.dump(dict(zip(dict_keys, range(len(dict_keys)))), open(self.ENCODER_PATH + key + '_dict.pkl', "wb"))

        return rtd

    def refresh_local_buffer(self):
        """
        Pull the Rtd.__tablename__ table from db, add delays save it on disk.
        """
        with ProgressBar():
            rtd = dd.read_sql_table(self.__tablename__, DB_CONNECT_STRING,
                                    index_col='hash_id', meta=self.no_array_meta, npartitions=200)
            rtd.to_parquet(self.DATA_CACHE_PATH, engine='pyarrow', write_metadata_file=False)
            rtd = dd.read_parquet(self.DATA_CACHE_PATH, engine='pyarrow')
            rtd = self._get_delays(rtd)
            rtd = self._categorize(rtd)

            print('adding latitude and logitude')
            rtd['lat'] = 0.0
            rtd['lon'] = 0.0
            lat_lon = rtd.map_partitions(self._add_station_coordinates,
                                         meta=rtd[['lat', 'lon']])
            rtd['lat'] = lat_lon['lat']
            rtd['lon'] = lat_lon['lon']

            # Save data to parquet. We have to use pyarrow as fastparquet does not support pd.Int64
            rtd.to_parquet(self.DATA_CACHE_PATH, engine='pyarrow', write_metadata_file=False)

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
            print('There was no cache found. New data will be downloaded from the db. This will take a while.')
            self.refresh_local_buffer()
            data = dd.read_parquet(self.DATA_CACHE_PATH, engine='pyarrow', **kwargs)

        return data

    def load_for_ml_model(self, max_date=None, min_date=None, return_date_id=False):
        rtd = self.load_data(columns=['station',
                                      'lat',
                                      'lon',
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
                                      'ar_pp',
                                      'dp_ct',
                                      'dp_pt',
                                      'dp_pp',
                                      'stop_id'])
        if min_date and max_date:
            rtd = rtd.loc[(rtd['date_id'] > min_date) & (rtd['date_id'] < max_date)]

        rtd['hour'] = rtd['ar_pt'].dt.hour.astype('Int64')
        rtd['hour'] = rtd['hour'].fillna(value=rtd.loc[:, 'dp_pt'].dt.hour)
        rtd['day'] = rtd['ar_pt'].dt.dayofweek.astype('Int64')
        rtd['day'] = rtd['day'].fillna(value=rtd.loc[:, 'dp_pt'].dt.dayofweek)
        rtd['stay_time'] = ((rtd['dp_pt'] - rtd['ar_pt']).dt.seconds // 60).astype('Int64')

        # Label encode categorical columns
        for key in ['o', 'c', 'n', 'station', 'ar_pp', 'dp_pp']:
            # dd.read_parquet reads categoricals as unknown categories. All the categories howerver get
            # saved in each partition. So we read those and set them as categories for the whole column.
            # https://github.com/dask/dask/issues/2944 
            rtd[key] = rtd[key].cat.set_categories(rtd[key].head(1).cat.categories)

            rtd[key] = rtd[key].cat.codes

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
    import fancy_print_tcp
    # from dask.distributed import Client
    # client = Client()

    rtd_ray = RtdRay()
    print('length of data: {} rows'.format(len(rtd_ray.load_data(columns='dayly_id'))))
    # rtd_ray.refresh_local_buffer()
    # rtd = rtd_ray.load_for_ml_model()