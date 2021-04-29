import os, sys
import pickle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from database import Rtd, DB_CONNECT_STRING

class ConnectionRay(Rtd):
    def __init__(self, notebook=False):
        if notebook:
            self.AR_PATH = '../cache/ar_connections'
            self.DP_PATH = '../cache/dp_connections'
            self.ENCODER_PATH = '../cache/' + self.__tablename__ + '_encoder'
        else:
            self.AR_PATH = 'cache/ar_connections'
            self.DP_PATH = 'cache/dp_connections'
            self.ENCODER_PATH = 'cache/' + self.__tablename__ + '_encoder'

    def load(self, path, label_encode=True, return_times=False):
        rtd = dd.read_parquet(path,
                              engine='pyarrow',
                              columns=['station',
                                      'lat',
                                      'lon',
                                      'o',
                                      'c',
                                      'n',
                                      'distance_to_start',
                                      'distance_to_end',
                                      'ar_delay',
                                      'dp_delay',
                                      'ar_ct',
                                      'ar_pt',
                                      'ar_pp',
                                      'dp_ct',
                                      'dp_pt',
                                      'dp_pp',
                                      'stop_id'])

        rtd['hour'] = rtd['ar_pt'].dt.hour
        rtd['hour'] = rtd['hour'].fillna(value=rtd.loc[:, 'dp_pt'].dt.hour)
        rtd['day'] = rtd['ar_pt'].dt.dayofweek
        rtd['day'] = rtd['day'].fillna(value=rtd.loc[:, 'dp_pt'].dt.dayofweek)
        rtd['stay_time'] = ((rtd['dp_pt'] - rtd['ar_pt']).dt.seconds // 60)

        # Label encode categorical columns
        categoricals = {'o': 'category',
                        'c': 'category', 'n': 'category',
                        'ar_pp': 'category', 'dp_pp': 'category',
                        'station': 'category'}
        rtd = rtd.astype(categoricals)
        for key in ['o', 'c', 'n', 'station', 'ar_pp', 'dp_pp']:
            # dd.read_parquet reads categoricals as unknown categories. All the categories howerver get
            # saved in each partition. So we read those and set them as categories for the whole column.
            # https://github.com/dask/dask/issues/2944 
            rtd[key] = rtd[key].cat.set_categories(rtd[key].head(1).cat.categories)

            if label_encode:
                rtd[key] = rtd[key].cat.codes.astype('int')
        rtd['stop_id'] = rtd['stop_id'].astype('int')

        if return_times:
            return rtd
        else:
            return rtd.drop(columns=['ar_ct',
                                    'ar_pt',
                                    'dp_ct',
                                    'dp_pt'], axis=0)

    def load_ar(self, **kwargs):
        return self.load(self.AR_PATH, **kwargs)

    def load_dp(self, **kwargs):
        return self.load(self.DP_PATH, **kwargs)


if __name__ == "__main__":
    import fancy_print_tcp
    from dask.distributed import Client
    client = Client()

    connection_ray = ConnectionRay()
    rtd = connection_ray.load_ar()
    print('length of data: {} rows'.format(len(rtd)))
    # rtd_ray.refresh_local_buffer()
    # rtd = rtd_ray.load_for_ml_model()