import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from database.rtd import Rtd


class RtdRay(Rtd):
    df_dict = {
        'ar_ppth': pd.Series([], dtype='str'),
        'ar_cpth': pd.Series([], dtype='str'),
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

        'dp_ppth': pd.Series([], dtype='str'),
        'dp_cpth': pd.Series([], dtype='str'),
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
            self.LOCAL_BUFFER_PATH = '../data_buffer/' + self.Rtd.__tablename__ + '_local_buffer'
        else:
            self.LOCAL_BUFFER_PATH = 'data_buffer/' + self.Rtd.__tablename__ + '_local_buffer'

    def refresh_local_buffer(self):
        """
        Pull the hole rtd table from db and save it on disk. This takes a while.
        """
        with ProgressBar():
            rtd = dd.read_sql_table(self.Rtd.__tablename__, self.DB_CONNECT_STRING,
                                    index_col='hash_id', meta=self.meta, npartitions=200)
            rtd.to_parquet(self.LOCAL_BUFFER_PATH, engine='pyarrow')

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
            dask.dataframe containing the loaded data

        """
        try:
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, **kwargs)
        except FileNotFoundError:
            print(
                'There was no buffer found. A new buffer will be downloaded from the db. This will take a while.')
            self.refresh_local_buffer()
            data = dd.read_parquet(self.LOCAL_BUFFER_PATH, **kwargs)

        return data


if __name__ == "__main__":
    rtd_d = RtdRay()
    rtd_d.refresh_local_buffer()
    # print(rtd_d.load_data())
