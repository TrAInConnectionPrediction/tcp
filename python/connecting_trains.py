import os, sys
from numpy.core.fromnumeric import sort

from pyarrow.hdfs import connect
from sqlalchemy import engine
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
import progressbar
from dask.diagnostics import ProgressBar
import dask.delayed
from helpers.RtdRay import RtdRay
import datetime
from helpers.StationPhillip import StationPhillip
import numpy as np
import concurrent.futures
from tqdm import tqdm
from config import CACHE_PATH

def separate_stations():
    rtd_ray = RtdRay()
    rtd = rtd_ray.load_for_ml_model(label_encode=False, return_times=True).compute() # .persist()
    # rtd = rtd.reset_index()
    # rtd['station'] = rtd['station'].cat.as_ordered()
    # rtd = rtd.set_index('station')
    # rtd.to_parquet(CACHE_PATH + '/station_rtd')
    storage_path = CACHE_PATH + '/station_rtd/part.{}.parquet'
    # stations = rtd['station'].cat.categories
    # rtd = rtd.groupby('station')
    # i = 0
    # for group in tqdm(stations):
    #     rtd.get_group(group).compute().to_parquet(storage_path.format(str(i)), engine='pyarrow')
    #     i += 1

    stations = rtd['station'].cat.categories
    print('separating stations')
    # station_rtd = []
    with progressbar.ProgressBar(max_value=len(stations)) as bar:
        for i, station in enumerate(stations):
            mask = rtd['station'] == station
            station_rtd = rtd.loc[mask, :]
            station_rtd.to_parquet(storage_path.format(str(i)), engine='pyarrow')
            # rtd = rtd.loc[~mask, :]
            bar.update(i)


def get_connecting_trains(df):
    df = df[~df.index.duplicated()]

    min_tranfer_time = datetime.timedelta(minutes=2)
    max_tranfer_time = datetime.timedelta(minutes=10)
    ar = []
    ar_index = []
    dp = []
    dp_index = []
    for index, row in df.iterrows():
        connecting_trains = df.loc[(df['dp_pt']).between(row['ar_pt'] + min_tranfer_time,
                                                            row['ar_pt'] + max_tranfer_time), 'ar_pt'].index
        for idx in connecting_trains:
            if idx != index:
                ar_index.append(index)
                dp_index.append(idx)
    if len(ar_index):
        ar = pd.DataFrame(columns=df.columns, index=ar_index)
        ar.loc[:, :] = df.loc[:, :]
        dp = pd.DataFrame(columns=df.columns, index=dp_index)
        dp.loc[:, :] = df.loc[:, :]
        return ar, dp
    else:
        return None, None


def save_connecting_trains(part):
    load_path = CACHE_PATH + '/station_rtd/part.{}.parquet'.format(part)
    ar, dp = get_connecting_trains(pd.read_parquet(load_path, engine='pyarrow'))
    if ar is not None:
        ar_path = CACHE_PATH + '/connecting_trains_{}/part.{}.parquet'.format('ar', part)
        dp_path = CACHE_PATH + '/connecting_trains_{}/part.{}.parquet'.format('dp', part)
        ar.to_parquet(ar_path, engine='pyarrow')
        dp.to_parquet(dp_path, engine='pyarrow')
    print(f"Saved {part}")

if __name__ == "__main__":
    from helpers import fancy_print_tcp
    from dask.distributed import Client
    client = Client()

    separate_stations()

    station_rtd = pd.read_parquet(CACHE_PATH + '/station_rtd/part.0.parquet')
    # get_connecting_trains(station_rtd)
    
    
    newpath = CACHE_PATH + '/connecting_trains_ar'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    newpath = CACHE_PATH + '/connecting_trains_dp'
    if not os.path.exists(newpath):
        os.makedirs(newpath)

    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(save_connecting_trains, range(7276))


    ar = dd.read_parquet(CACHE_PATH + '/connecting_trains_ar', engine='pyarrow').compute()
    ar = dd.from_pandas(ar, 100, sort=False)
    ar.to_parquet(CACHE_PATH + '/ar_connections', engine='pyarrow')

    dp = dd.read_parquet(CACHE_PATH + '/connecting_trains_dp', engine='pyarrow').compute()
    dp = dd.from_pandas(dp, 100, sort=False)
    dp.to_parquet(CACHE_PATH + '/dp_connections', engine='pyarrow')

    print(len(dd.read_parquet(CACHE_PATH + '/ar_connections', engine='pyarrow')))