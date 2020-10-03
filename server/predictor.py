import xgboost as xgb
import pickle
import pandas as pd
import datetime
import numpy as np
from pytz import timezone
from helpers.StreckennetzSteffi import StreckennetzSteffi


def from_unix(unix):
    """
    Convert from unix timestamp to GMT+1
    Removes millisecons (/1000) add one hour (+3600) and set timezone

    Args:
        unix (int): UNIX timestamp with milliseconds

    Returns:
        datetime: a datetime object with GMT+1 Timezone set
    """

    return datetime.datetime.utcfromtimestamp(float(unix) / 1000 + 3600).replace(
        tzinfo=timezone("Europe/Berlin")
    )


class Predictor:
    def __init__(self):
        self.streckennetz = StreckennetzSteffi()
        self.cat_encoders = {}
        for cat in ["o", "c", "n", "station"]:
            self.cat_encoders[cat] = pickle.load(
                open("data_buffer/LabelEncoder_{}.pkl".format(cat), "rb")
            )
        self.models = {}
        for model in [
            "ar_0",
            "ar_5",
            "ar_10",
            "ar_15",
            "dp_0",
            "dp_5",
            "dp_10",
            "dp_15",
        ]:
            self.models[model] = pickle.load(
                open("data_buffer/model_{}.pkl".format(model), "rb")
            )

    def predict(self, data, which):
        predicions = {}
        for model in [
            "ar_0",
            "ar_5",
            "ar_10",
            "ar_15",
            "dp_0",
            "dp_5",
            "dp_10",
            "dp_15",
        ]:
            predicions[model] = float(self.models[model].predict_proba(data)[which, 1])
        return predicions

    def predict_con(self, data1, data2, transfer_time):
        pred1 = self.predict(data1, 1)
        pred2 = self.predict(data2, 0)

        con_score = 0
        if transfer_time > 17:
            con_score = 1  # if the transfer time is higher than our highest lable, we can only predict the connection as working

        elif transfer_time > 12:
            p1 = pred1["ar_15"] * (1 - pred2["dp_5"])
            con_score = 1 - p1

        elif transfer_time > 7:
            # If the arrival train has 10 min delay, and the departure one does not have 5 min delay
            p1 = (pred1["ar_10"] - pred1["ar_15"]) * (1 - pred2["dp_5"])

            # if the arrival train has 15 min delay, and the departure one does not have 10 min delay
            p2 = pred1["ar_15"] * (1 - pred2["dp_10"])
            con_score = 1 - (p1 + p2)

        else:
            p1 = (pred1["ar_5"] - pred1["ar_10"]) * (1 - pred2["dp_5"])
            p2 = (pred1["ar_10"] - pred1["ar_15"]) * (1 - pred2["dp_10"])
            p3 = pred1["ar_15"] * (1 - pred2["dp_15"])
            con_score = 1 - (p1 + p2 + p3)

        return con_score, pred1["ar_5"], pred2["dp_5"]

    def get_pred_data(self, connection):
        data = pd.DataFrame(
            columns=[
                "c",
                "n",
                "distance_to_start",
                "day",
                "hour",
                "station",
                "distance_to_end",
                "o",
            ]
        )
        # departure of segment
        data.loc[0, "station"] = self.streckennetz.get_name(
            eva=int(connection["segmentStart"]["id"])
        )
        trip = connection["full_trip"][
            : connection["full_trip"].index(
                self.streckennetz.get_eva(name=data.at[0, "station"])
            )
            + 1
        ]
        if len(trip) == 1:
            data.at[0, "distance_to_start"] = 0
        else:
            data.at[0, "distance_to_start"] = self.streckennetz.eva_route_length(trip)

        trip = connection["full_trip"][
            connection["full_trip"].index(
                self.streckennetz.get_eva(name=data.at[0, "station"])
            ) :
        ]
        if len(trip) == 1:
            data.at[0, "distance_to_end"] = 0
        else:
            data.at[0, "distance_to_end"] = self.streckennetz.eva_route_length(trip)
        time = from_unix(connection["departure"]["time"])
        data.at[0, "hour"] = time.hour
        data.at[0, "day"] = time.weekday()
        try:
            data.at[0, "c"] = self.cat_encoders["c"][connection["train"]["type"]]
        except KeyError:
            data.at[0, "c"] = 0
        try:
            data.at[0, "n"] = self.cat_encoders["n"][connection["train"]["number"]]
        except KeyError:
            data.at[0, "n"] = 0
        try:
            data.at[0, "o"] = self.cat_encoders["o"][
                connection["train"]["admin"].replace("_", "")
            ]
        except KeyError:
            data.at[0, "o"] = 0

        # arrival of segment
        data.loc[1, "station"] = self.streckennetz.get_name(
            eva=int(connection["segmentDestination"]["id"])
        )
        trip = connection["full_trip"][
            : connection["full_trip"].index(
                self.streckennetz.get_eva(name=data.at[1, "station"])
            )
            + 1
        ]
        if len(trip) == 1:
            data.at[1, "distance_to_start"] = 0
        else:
            data.at[1, "distance_to_start"] = self.streckennetz.eva_route_length(trip)

        trip = connection["full_trip"][
            connection["full_trip"].index(
                self.streckennetz.get_eva(name=data.at[1, "station"])
            ) :
        ]
        if len(trip) == 1:
            data.at[1, "distance_to_end"] = 0
        else:
            data.at[1, "distance_to_end"] = self.streckennetz.eva_route_length(trip)
        time = from_unix(connection["arrival"]["time"])
        data.at[1, "hour"] = time.hour
        data.at[1, "day"] = time.weekday()
        data.at[0, "station"] = self.cat_encoders["station"][data.at[0, "station"]]
        data.at[1, "station"] = self.cat_encoders["station"][data.at[1, "station"]]

        try:
            data.at[1, "c"] = self.cat_encoders["c"][connection["train"]["type"]]
        except KeyError:
            data.at[1, "c"] = 0
        try:
            data.at[1, "n"] = self.cat_encoders["n"][connection["train"]["number"]]
        except KeyError:
            data.at[1, "n"] = 0
        try:
            data.at[1, "o"] = self.cat_encoders["o"][
                connection["train"]["admin"].replace("_", "")
            ]
        except KeyError:
            data.at[1, "o"] = 0

        data["station"] = data["station"].astype("int64")
        data["hour"] = data["hour"].astype("int64")
        data["day"] = data["day"].astype("int64")
        data["o"] = data["o"].astype("int64")
        data["c"] = data["c"].astype("int64")
        data["n"] = data["n"].astype("int64")
        data["distance_to_start"] = data["distance_to_start"].astype("float64")
        data["distance_to_end"] = data["distance_to_end"].astype("float64")

        return data[self.models["ar_5"]._Booster.feature_names]


if __name__ == "__main__":
    pred = Predictor()
