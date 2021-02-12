import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
import datetime
from helpers.StationPhillip import StationPhillip
import numpy as np
import matplotlib.pyplot as plt
from sklearn import neighbors
from sklearn.preprocessing import minmax_scale
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from data_analysis.delay import load_with_delay
from helpers.RtdRay import RtdRay


def add_lon(rtd, station_list):
    stations = StationPhillip()
    for station in station_list:
        lon = stations.get_location(name=station)[0]
        rtd.loc[rtd['station'] == station, 'lon'] = lon
    return rtd['lon']


def add_lat(rtd, station_list):
    stations = StationPhillip()
    for station in station_list:
        lat = stations.get_location(name=station)[1]
        rtd.loc[rtd['station'] == station, 'lat'] = lat
    return rtd['lat']


if __name__ == '__main__':
    import fancy_print_tcp
    from dask.distributed import Client
    client = Client()
    rtd_ray = RtdRay()
    rtd = rtd_ray.load_data(columns=['station', 'date_id'])
    # create needed data
    # rtd = load_with_delay(columns=['station'])
    
    stations = StationPhillip()
    unique_stations = rtd['station'].unique()
    rtd['lat'] = 0.0
    rtd['lat'] = rtd.map_partitions(add_lat, station_list=unique_stations, meta=rtd['lat'])
    rtd['lon'] = 0.0
    rtd['lon'] = rtd.map_partitions(add_lon, station_list=unique_stations, meta=rtd['lon'])

    # rtd = rtd[['lon', 'lat', 'ar_ct', 'ar_delay']]
    rtd.to_parquet('cache/nn_rtd', engine='pyarrow', write_metadata_file=False)
    print('saved parquet')
    # rtd_da = rtd[['lon', 'lat', 'ar_ct', 'ar_delay']].to_dask_array()


    # build model
    df = dd.read_parquet('cache/nn_rtd', engine='pyarrow').compute()
    df = df.loc[(df['ar_ct'] < datetime.datetime(2020, 11, 28)) & (df['ar_ct'] > datetime.datetime(2020, 11, 10)), :]
    df['ar_ct'] = df['ar_ct'].astype(int)
    data = df.to_numpy()
    del df
    labels = data[:, 3]
    scaler = MinMaxScaler()
    scaler.fit(data[:, :3])
    data = scaler.transform(data[:, :3])
    ten_min_distance = pd.DataFrame({'lat': [0, 0], 'lon': [0, 0], 'ar_ct': [datetime.datetime(2020, 11, 10, 00, 00), datetime.datetime(2020, 11, 10, 00, 10)]})
    ten_min_distance['ar_ct'] = ten_min_distance['ar_ct'].astype(int)
    ten_min_distance = ten_min_distance.to_numpy()
    ten_min_distance = scaler.transform(ten_min_distance)[:, 2]
    ten_min_distance = ten_min_distance[1] - ten_min_distance[0]

    X = data[:-3000, :3]
    y = labels[:-3000]

    T = data[-3000:, :3]
    Ty = labels[-3000:]
    print(Ty.max())

    knn = neighbors.RadiusNeighborsRegressor(ten_min_distance, weights='uniform')
    print('fitting')
    y_ = knn.fit(X, y).predict(T)

    plt.scatter(Ty, y_, color='red', label='prediction')
    Ty = Ty[~np.isnan(y_)]
    y_ = y_[~np.isnan(y_)]
    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(Ty.reshape(-1, 1), y_)
    reg_y = regr.predict(Ty.reshape(-1, 1))
    plt.plot(Ty, reg_y, color='blue', linewidth=3)

    # plt.axis('tight')
    plt.axis('scaled')
    plt.legend()

    plt.tight_layout()
    plt.show()

