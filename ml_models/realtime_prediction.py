import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
import datetime
import numpy as np
import matplotlib.pyplot as plt
from sklearn import neighbors
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from helpers import RtdRay


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    from dask.distributed import Client
    client = Client(n_workers=min(16, os.cpu_count()))
    rtd = RtdRay.load_data(columns=['station', 'date_id', 'lat', 'lon', 'ar_ct', 'ar_delay'])

    rtd = rtd.loc[(rtd['ar_ct'] < datetime.datetime(2020, 11, 28)) & (rtd['ar_ct'] > datetime.datetime(2020, 11, 10)), :]
    rtd['ar_ct'] = rtd['ar_ct'].astype(int)
    data = rtd.to_numpy()
    del rtd
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

