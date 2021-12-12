import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import RtdRay
import matplotlib.pyplot as plt
from config import CACHE_PATH, ENCODER_PATH
import pandas as pd
import numpy as np
import datetime
import pickle

import torch.optim as optim
from torch.autograd import Variable
import torch
import torch.nn as nn
import torch.nn.functional as F


def min_max_scaler(sr: pd.Series):
    return (sr - sr.min()) / (sr.max() - sr.min())


def normalize(df: pd.DataFrame, normalizer):
    return df.apply(normalizer)


class Datasets():
    def __init__(self):
        super().__init__()

        self.status_encoder = {}
        self.status_encoder["ar"] = pickle.load(
            open(ENCODER_PATH.format(encoder="ar_cs"), "rb")
        )
        self.status_encoder["dp"] = pickle.load(
            open(ENCODER_PATH.format(encoder="dp_cs"), "rb")
        )

        try:
            self.rtd = pd.read_pickle(CACHE_PATH + '/torch_data.pkl')
        except FileNotFoundError:
            self.rtd: pd.DataFrame = RtdRay.load_for_ml_model(
                return_status=True,
                min_date=datetime.datetime(2021, 2, 16),
                max_date=datetime.datetime(2021, 3, 25),
            ).compute()
            self.rtd = self.rtd[[
                'distance_to_start',
                'distance_to_end',
                'lat',
                'lon',
                'ar_delay',
                'dp_delay',
                'ar_cs',
                'dp_cs',
            ]]
            self.rtd.to_pickle(CACHE_PATH + '/torch_data.pkl')

    def get_sets(self, threshhold_minutes, ar_or_dp):
        """Generate train and test data

        Parameters
        ----------
        threshhold_minutes : int
            Threshhold for labels
        ar_or_dp : str
            Whether to make sets for arrival or departure

        Returns
        -------
        tuple (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame)
            (train_x, train_y, test_x, test_y)
        """
        split_index = int(len(self.rtd) * 0.8)
        train = self.rtd.iloc[:split_index]
        test = self.rtd.iloc[split_index:]

        if ar_or_dp == "ar":
            # filter out nans (not each datapoint contains arrival and departure)
            train = train.loc[
                ~train["ar_delay"].isna()
                | (train["ar_cs"] == self.status_encoder["ar"]["c"])
            ]
            test = test.loc[
                ~test["ar_delay"].isna()
                | (test["ar_cs"] == self.status_encoder["ar"]["c"])
            ]
            
            train_y = (train["ar_delay"] <= threshhold_minutes) & (
                train["ar_cs"] != self.status_encoder["ar"]["c"]
            )

            test_y = (test["ar_delay"] <= threshhold_minutes) & (
                test["ar_cs"] != self.status_encoder["ar"]["c"]
            )

        else:
            # filter out nans (not each datapoint contains arrival and departure)
            train = train.loc[
                ~train["dp_delay"].isna()
                | (train["dp_cs"] == self.status_encoder["dp"]["c"])
            ]
            test = test.loc[
                ~test["dp_delay"].isna()
                | (test["dp_cs"] == self.status_encoder["dp"]["c"])
            ]
            
            train_y = (train["dp_delay"] <= threshhold_minutes) & (
                train["dp_cs"] != self.status_encoder["dp"]["c"]
            )

            test_y = (test["dp_delay"] <= threshhold_minutes) & (
                test["dp_cs"] != self.status_encoder["dp"]["c"]
            )

        train_x = train.drop(
            columns=[
                "ar_delay",
                "dp_delay",
                "ar_cs",
                "dp_cs",
            ],
            axis=0,
        )
        test_x = test.drop(
            columns=[
                "ar_delay",
                "dp_delay",
                "ar_cs",
                "dp_cs",
            ],
            axis=0,
        )

        return train_x, train_y, test_x, test_y

    def np_normalized_sets(self, **kwargs):
        train_x, train_y, test_x, test_y = self.get_sets(**kwargs)

        train_x = normalize(train_x, min_max_scaler)
        test_x = normalize(test_x, min_max_scaler)

        return train_x.to_numpy(dtype=np.float32), train_y.to_numpy(dtype=np.float32), test_x.to_numpy(dtype=np.float32), test_y.to_numpy(dtype=np.float32)


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(4, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 1)

    def forward(self, x):
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


def batchify(x: torch.tensor, y: torch.tensor, batch_size: int):
    for i in range(0, x.size()[0], batch_size):
        yield x[i:i+batch_size], y[i:i+batch_size]


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    datasets = Datasets()

    torch.device('cuda')

    net = Net()

    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)

    train_x, train_y, test_x, test_y = datasets.np_normalized_sets(threshhold_minutes=0, ar_or_dp='ar')
    train_x, train_y = torch.from_numpy(train_x), torch.from_numpy(train_y)

    for epoch in range(20):  # loop over the dataset multiple times
        running_loss = 0.0
        for i, data in enumerate(batchify(train_x, train_y, 1000)):
            # get the inputs; data is a list of [inputs, labels]
            inputs, labels = data

            # zero the parameter gradients
            optimizer.zero_grad()

            # forward + backward + optimize
            outputs = net(inputs)
            loss = criterion(outputs, labels.unsqueeze(1))
            loss.backward()
            optimizer.step()

            # print statistics
            running_loss += loss.item()
            if i % 2000 == 1999:    # print every 2000 mini-batches
                print('[%d, %5d] loss: %.3f' %
                    (epoch + 1, i + 1, running_loss / 2000))
                running_loss = 0.0

    print('Finished Training')
