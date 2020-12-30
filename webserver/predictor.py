import pickle
import pandas as pd
import datetime
import numpy as np
from pytz import timezone
from webserver import streckennetz


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
        # self.streckennetz = StreckennetzSteffi()
        self.cat_encoders = {}
        for cat in ["o", "c", "n", "station"]:
            self.cat_encoders[cat] = pickle.load(
                open("data_buffer/LabelEncoder_{}.pkl".format(cat), "rb")
            )
        self.ar_models = []
        self.dp_models = []
        for model in range(40):
            self.ar_models.append(pickle.load(open("data_buffer/ar_models/model_{}.pkl".format(model), "rb")))
            self.dp_models.append(pickle.load(open("data_buffer/dp_models/model_{}.pkl".format(model), "rb")))

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

    def predict_ar(self, features):
        prediction = np.empty(40)
        for model in range(40):
            prediction[model] = self.ar_models[model].predict_proba(features, validate_features=True)[0, 1]
        return prediction

    def predict_dp(self, features):
        prediction = np.empty(40)
        for model in range(40):
            prediction[model] = self.dp_models[model].predict_proba(features, validate_features=True)[0, 1]
        return prediction

    def predict(self, data, which):
        predictions = {'ar_0': 0.95, 'ar_5': 1 - 0.95, 'ar_10': 1 - 0.97, 'ar_15': 1 - 0.99,
                       'dp_0': 0.95, 'dp_5': 1 - 0.95, 'dp_10': 1 - 0.97, 'dp_15': 1 - 0.99,}
        if not data.empty:
            for model in predictions:
                predictions[model] = float(self.models[model].predict_proba(data)[which, 1])
        return predictions

    def predict_con(self, ar_features, dp_features, transfer_time):
        ar_prediction = self.predict_ar(ar_features.loc[0:1, :])
        dp_prediction = self.predict_dp(dp_features.loc[1:, :])
        if transfer_time > 39:
            return 1, ar_prediction[5], dp_prediction[5]
        con_score = ar_prediction[transfer_time - 2] * dp_prediction[0]
        con_score += np.sum((ar_prediction[transfer_time-1:] - ar_prediction[transfer_time-2:-1]) * dp_prediction[1:2-transfer_time or len(dp_prediction)])
        return con_score, ar_prediction[5], dp_prediction[5]

        # pred1 = self.predict(data1, 1)
        # pred2 = self.predict(data2, 0)
        #
        # con_score = 0
        # if transfer_time > 17:
        #     con_score = 1  # if the transfer time is higher than our highest label, we can only predict the connection as working
        #
        # elif transfer_time > 12:
        #     p1 = pred1["ar_15"] * (1 - pred2["dp_5"])
        #     con_score = 1 - p1
        #
        # elif transfer_time > 7:
        #     # If the arrival train has 10 min delay, and the departure one does not have 5 min delay
        #     p1 = (pred1["ar_10"] - pred1["ar_15"]) * (1 - pred2["dp_5"])
        #
        #     # if the arrival train has 15 min delay, and the departure one does not have 10 min delay
        #     p2 = pred1["ar_15"] * (1 - pred2["dp_10"])
        #     con_score = 1 - (p1 + p2)
        #
        # else:
        #     p1 = (pred1["ar_5"] - pred1["ar_10"]) * (1 - pred2["dp_5"])
        #     p2 = (pred1["ar_10"] - pred1["ar_15"]) * (1 - pred2["dp_10"])
        #     p3 = pred1["ar_15"] * (1 - pred2["dp_15"])
        #     con_score = 1 - (p1 + p2 + p3)
        #
        # return con_score, pred1["ar_5"], pred2["dp_5"]

    def get_pred_data(self, connection):
        try:
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
            data.loc[0, "station"] = streckennetz.get_name(eva=int(connection["segmentStart"]["id"]))
            trip = connection["full_trip"][
                : connection["full_trip"].index(
                    int(connection["segmentStart"]["id"])
                )
                + 1
            ]
            if len(trip) == 1:
                data.at[0, "distance_to_start"] = 0
            else:
                data.at[0, "distance_to_start"] = streckennetz.eva_route_length(trip)
    
            trip = connection["full_trip"][
                connection["full_trip"].index(
                    streckennetz.get_eva(name=data.at[0, "station"])
                ) :
            ]
            if len(trip) == 1:
                data.at[0, "distance_to_end"] = 0
            else:
                data.at[0, "distance_to_end"] = streckennetz.eva_route_length(trip)

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
            data.loc[1, "station"] = streckennetz.get_name(
                eva=int(connection["segmentDestination"]["id"])
            )
            trip = connection["full_trip"][
                : connection["full_trip"].index(
                    int(connection["segmentDestination"]["id"])
                )
                + 1
            ]
            if len(trip) == 1:
                data.at[1, "distance_to_start"] = 0
            else:
                data.at[1, "distance_to_start"] = streckennetz.eva_route_length(trip)
    
            trip = connection["full_trip"][
                connection["full_trip"].index(
                    int(connection["segmentDestination"]["id"])
                ) :
            ]
            if len(trip) == 1:
                data.at[1, "distance_to_end"] = 0
            else:
                data.at[1, "distance_to_end"] = streckennetz.eva_route_length(trip)
            time = from_unix(connection["arrival"]["time"])
            data.at[1, "hour"] = time.hour
            data.at[1, "day"] = time.weekday()
            try:
                data.at[0, "station"] = self.cat_encoders["station"][data.at[0, "station"]]
            except KeyError:
                data.at[0, "station"] = 0
            try:
                data.at[1, "station"] = self.cat_encoders["station"][data.at[1, "station"]]
            except KeyError:
                data.at[1, "station"] = 0

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
        except KeyError:
            return pd.DataFrame()


if __name__ == "__main__":
    pred = Predictor()
