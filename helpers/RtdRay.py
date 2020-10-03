import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from database.rtd import Rtd
from database.engine import DB_CONNECT_STRING


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

        'station': pd.Series([], dtype='str'),
        'id': pd.Series([], dtype='str')
    }

    meta = dd.from_pandas(pd.DataFrame(df_dict), npartitions=1)

    def __init__(self, notebook=False):
        if notebook:
            self.LOCAL_BUFFER_PATH = '../data_buffer/' + self.__tablename__ + '_local_buffer'
        else:
            self.LOCAL_BUFFER_PATH = 'data_buffer/' + self.__tablename__ + '_local_buffer'

    def refresh_local_buffer(self):
        """
        Pull the hole rtd table from db and save it on disk. This takes a while.
        """
        with ProgressBar():
            rtd = dd.read_sql_table(self.__tablename__, DB_CONNECT_STRING,
                                    index_col='hash_id', meta=self.meta, npartitions=200)
            rtd = self.parse_unparsed(rtd)
            # Save data to parquet. We have to use pyarrow as fastparquet does not support pd.Int64
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow')

    @staticmethod
    def add_missing_changes(df):
        for prefix in ('ar', 'dp'):
            no_ct = df[prefix + '_ct'].isna()
            df.loc[no_ct, prefix + '_ct'] = df.loc[no_ct, prefix + '_pt']

            no_cpth = df[prefix + '_cpth'] == pd.Series([['nan'] for _ in range(len(df))], index=df.index)
            df.loc[no_cpth, prefix + '_cpth'] = df.loc[no_cpth, prefix + '_ppth']

            no_cp = df[prefix + '_cp'].isna()
            df.loc[no_cp, prefix + '_cp'] = df.loc[no_cp, prefix + '_pp']
        return df

    @staticmethod
    def add_distances(df):
        from helpers.StreckennetzSteffi import StreckennetzSteffi
        streckennetz = StreckennetzSteffi()
        for i, row in df.iterrows():
            try:
                df.at[i, 'distance_to_last'] = streckennetz.route_length([row['ar_cpth'][-1]] + [row['station']])
                df.at[i, 'distance_to_start'] = streckennetz.route_length(row['ar_cpth'] + [row['station']])
            except KeyError:
                df.at[i, 'distance_to_last'] = 0
                df.at[i, 'distance_to_start'] = 0

            try:
                df.at[i, 'distance_to_next'] = streckennetz.route_length([row['station']] + [row['dp_cpth'][0]])
                df.at[i, 'distance_to_end'] = streckennetz.route_length([row['station']] + row['dp_cpth'])
            except KeyError:
                df.at[i, 'distance_to_next'] = 0
                df.at[i, 'distance_to_end'] = 0
        return df.loc[:, ['distance_to_last', 'distance_to_start', 'distance_to_next', 'distance_to_end']]

    @staticmethod
    def parse_unparsed(rtd):
        rtd = rtd.astype({'f': 'category', 't': 'category', 'o': 'category',
                          'c': 'category', 'n': 'category', 'ar_ps': 'category',
                          'ar_pp': 'category', 'dp_ps': 'category', 'dp_pp': 'category',
                          'station': 'category'})

        # Convert all arrays from str to list.
        arr_cols = ['ar_ppth', 'ar_cpth', 'ar_m_id', 'ar_m_t', 'ar_m_ts', 'ar_m_c',
                    'dp_ppth', 'dp_cpth', 'dp_m_id', 'dp_m_t', 'dp_m_ts', 'dp_m_c',
                    'm_id', 'm_t', 'm_ts', 'm_c']
        for arr_col in arr_cols:
            rtd[arr_col] = rtd[arr_col].str.replace('{', '')
            rtd[arr_col] = rtd[arr_col].str.replace('}', '')

            rtd[arr_col] = rtd[arr_col].astype('str')
            rtd[arr_col] = rtd[arr_col].str.split('|')
        rtd = rtd.map_partitions(RtdRay.add_missing_changes, meta=rtd)
        add_distance_meta = {'distance_to_last': 'f8', 'distance_to_start': 'f8',
                             'distance_to_next': 'f8', 'distance_to_end': 'f8'}
        rtd[['distance_to_last', 'distance_to_start',
             'distance_to_next', 'distance_to_end']] = rtd.map_partitions(RtdRay.add_distances, meta=add_distance_meta)
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
    from dask.distributed import Client
    client = Client()
    rtd_d = RtdRay()
    # rtd_d.refresh_local_buffer()
    print(rtd_d.load_data().head())
    # rtd_df = rtd_d.load_data().head().compute().to_clipboard()
    # print(rtd_d.parse_unparsed(rtd_df))
    # print(rtd_df.head())
