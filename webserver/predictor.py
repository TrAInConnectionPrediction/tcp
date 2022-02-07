import pickle
import pandas as pd
import datetime
import numpy as np
import os
from pytz import timezone
from xgboost import XGBClassifier
from webserver import streckennetz
from helpers import RtdRay
from config import ENCODER_PATH, MODEL_PATH


def from_utc(utc_time: str) -> datetime.datetime:
    return datetime.datetime.strptime(
        utc_time,
        "%Y-%m-%dT%H:%M:%S.%f%z"
    ).astimezone(timezone("Europe/Berlin")).replace(tzinfo=None)


def train_model(self, train_x, train_y, **model_parameters):
    est = XGBClassifier(
        n_jobs=-1,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=0,
        tree_method="gpu_hist",
        use_label_encoder=False,
        **model_parameters,
    )
    est.fit(train_x, train_y)
    return est


parameters = {
    -1: {'learning_rate': 0.4, 'max_depth': 14, 'n_estimators': 100, 'gamma': 2.8, },
    0: {'learning_rate': 0.4, 'max_depth': 14, 'n_estimators': 100, 'gamma': 2.8, },
    1: {'learning_rate': 0.4, 'max_depth': 14, 'n_estimators': 100, 'gamma': 2.8, },
    2: {'learning_rate': 0.4, 'max_depth': 13, 'n_estimators': 100, 'gamma': 2.8, },
    3: {'learning_rate': 0.4, 'max_depth': 13, 'n_estimators': 100, 'gamma': 2.8, },
    4: {'learning_rate': 0.4, 'max_depth': 12, 'n_estimators': 100, 'gamma': 2.8, },
    5: {'learning_rate': 0.4, 'max_depth': 12, 'n_estimators': 100, 'gamma': 2.8, },
    6: {'learning_rate': 0.4, 'max_depth': 11, 'n_estimators': 100, 'gamma': 2.8, },
    7: {'learning_rate': 0.4, 'max_depth': 11, 'n_estimators': 100, 'gamma': 2.8, },
    8: {'learning_rate': 0.4, 'max_depth': 10, 'n_estimators': 100, 'gamma': 2.8, },
    9: {'learning_rate': 0.4, 'max_depth': 10, 'n_estimators': 100, 'gamma': 2.8, },
    10: {'learning_rate': 0.4, 'max_depth': 10, 'n_estimators': 100, 'gamma': 2.8, },
    11: {'learning_rate': 0.4, 'max_depth': 9, 'n_estimators': 100, 'gamma': 2.8, },
    12: {'learning_rate': 0.4, 'max_depth': 9, 'n_estimators': 100, 'gamma': 2.8, },
    13: {'learning_rate': 0.4, 'max_depth': 8, 'n_estimators': 100, 'gamma': 2.8, },
    14: {'learning_rate': 0.4, 'max_depth': 8, 'n_estimators': 100, 'gamma': 2.8, },
}


class Predictor:
    def __init__(self, n_models=40):
        self.n_models = n_models
        self.cat_encoders = {}
        for cat in ["o", "c", "n", "station", 'pp']:
            self.cat_encoders[cat] = pickle.load(
                open(ENCODER_PATH.format(encoder=cat), "rb")
            )
        self.ar_models = []
        self.dp_models = []
        for model in range(self.n_models):
            self.ar_models.append(pickle.load(
                open(MODEL_PATH.format('ar_' + str(model)), "rb")))
            self.dp_models.append(pickle.load(
                open(MODEL_PATH.format('dp_' + str(model)), "rb")))

    def predict_ar(self, features):
        features = features.to_numpy()
        prediction = np.empty((len(features), self.n_models))
        for model in range(self.n_models):
            prediction[:, model] = self.ar_models[model].predict_proba(
                features, validate_features=False)[:, 1]
        return prediction

    def predict_dp(self, features):
        features = features.to_numpy()
        prediction = np.empty((len(features), self.n_models))
        for model in range(self.n_models):
            prediction[:, model] = self.dp_models[model].predict_proba(
                features, validate_features=False)[:, 1]
        return prediction

    def predict_con(self, ar_prediction, dp_prediction, transfer_time):
        con_score = np.ones(len(transfer_time))
        for tra_time in range(self.n_models):
            mask = transfer_time == tra_time
            if mask.any():
                con_score[mask] = ar_prediction[mask, max(
                    tra_time - 2, 0)] * dp_prediction[mask, max(0, 2 - tra_time)]
                con_score[mask] = con_score[mask] + np.sum((
                    ar_prediction[mask, max(
                        tra_time-2, 0)+1:dp_prediction.shape[1] - max(0, 2 - tra_time)]
                    - ar_prediction[mask, max(tra_time-2, 0):dp_prediction.shape[1] - 1 - max(0, 2 - tra_time)])
                    * dp_prediction[mask, max(0, 2 - tra_time)+1:dp_prediction.shape[1] + min(2-tra_time, 0)], axis=1)
        return np.minimum(con_score, np.ones(len(con_score)))

    def get_pred_data(self, segments: list):
        dtypes = {
            'station': 'int',
            'lat': 'float',
            'lon': 'float',
            'o': 'int',
            'c': 'int',
            'n': 'int',
            'distance_to_start': 'float',
            'distance_to_end': 'float',
            'pp': 'int',
            'stop_id': 'int',
            'minute': 'int',
            'day': 'int',
            'stay_time': 'float'
        }
        ar_data = pd.DataFrame(
            columns=dtypes.keys,
            index=range(len(segments))
        )
        dp_data = ar_data.copy()
        for i, segment in enumerate(segments):
            # Encode categoricals
            for cat in self.cat_encoders:
                try:
                    if cat == 'pp':
                        ar_data.at[i, cat] = self.cat_encoders[cat][segment['ar_' + 'cp']]
                    else:
                        ar_data.at[i, cat] = self.cat_encoders[cat][segment['ar_' + cat]]
                except KeyError:
                    ar_data.at[i, cat] = -1
                    print('unknown {cat}: {value}'.format(
                        cat=cat, value=segment['ar_' + cat]))
                try:
                    if cat == 'pp':
                        dp_data.at[i, cat] = self.cat_encoders[cat][segment['dp_' + 'cp']]
                    else:
                        dp_data.at[i, cat] = self.cat_encoders[cat][segment['dp_' + cat]]
                except KeyError:
                    dp_data.at[i, cat] = -1
                    print('unknown {cat}: {value}'.format(
                        cat=cat, value=segment['dp_' + cat]))

            ar_data.at[i, 'lat'] = segment['ar_lat']
            ar_data.at[i, 'lon'] = segment['ar_lon']
            dp_data.at[i, 'lat'] = segment['dp_lat']
            dp_data.at[i, 'lon'] = segment['dp_lon']

            ar_data.at[i, 'stop_id'] = segment['ar_stop_id']
            dp_data.at[i, 'stop_id'] = segment['dp_stop_id']

            ar_data.at[i, 'distance_to_start'] = streckennetz.route_length(
                segment['full_trip'][:ar_data.at[i, 'stop_id'] + 1],
                date=segment['dp_pt']
            )
            ar_data.at[i, 'distance_to_end'] = streckennetz.route_length(
                segment['full_trip'][ar_data.at[i, 'stop_id']:],
                date=segment['dp_pt']
            )
            dp_data.at[i, 'distance_to_start'] = streckennetz.route_length(
                segment['full_trip'][:dp_data.at[i, 'stop_id'] + 1],
                date=segment['dp_pt']
            )
            dp_data.at[i, 'distance_to_end'] = streckennetz.route_length(
                segment['full_trip'][dp_data.at[i, 'stop_id']:],
                date=segment['dp_pt']
            )

            ar_data.at[i, 'minute'] = segment['ar_ct'].time(
            ).minute + segment['ar_ct'].time().hour * 60
            ar_data.at[i, 'day'] = segment['ar_ct'].weekday()
            dp_data.at[i, 'minute'] = segment['dp_ct'].time(
            ).minute + segment['dp_ct'].time().hour * 60
            dp_data.at[i, 'day'] = segment['dp_ct'].weekday()

            ar_data.at[i, 'stay_time'] = segment['stay_times'][ar_data.at[i, 'stop_id']]
            dp_data.at[i, 'stay_time'] = segment['stay_times'][dp_data.at[i, 'stop_id']]

        return ar_data.astype(dtypes), dp_data.astype(dtypes)

    def train_models(self, min_date=datetime.datetime.today() - datetime.timedelta(days=7 * 4), **load_parameters):
        train = RtdRay.load_for_ml_model(
            min_date=min_date,
            long_distance_only=False,
            return_status=True,
            **load_parameters).compute()

        # We need this in order to set canceld trains as delayed
        status_encoder = {}
        status_encoder["ar"] = pickle.load(
            open(ENCODER_PATH.format(encoder="ar_cs"), "rb"))
        status_encoder["dp"] = pickle.load(
            open(ENCODER_PATH.format(encoder="dp_cs"), "rb"))

        ar_train = train.loc[~train["ar_delay"].isna()]
        dp_train = train.loc[~train["dp_delay"].isna()]
        del train

        ar_labels = {}
        dp_labels = {}
        for label in range(self.n_models):
            ar_labels[label] = (ar_train["ar_delay"] <= label) & (
                ar_train["ar_cs"] != status_encoder["ar"]["c"]
            )
            dp_labels[label + 1] = (dp_train["dp_delay"] >= (label + 1)) & (
                dp_train["dp_cs"] != status_encoder["dp"]["c"]
            )

        del ar_train["ar_delay"]
        del ar_train["dp_delay"]
        del ar_train["ar_cs"]
        del ar_train["dp_cs"]

        del dp_train["ar_delay"]
        del dp_train["dp_delay"]
        del dp_train["ar_cs"]
        del dp_train["dp_cs"]

        newpath = "cache/models"
        if not os.path.exists(newpath):
            os.makedirs(newpath)

        for label in range(self.n_models):
            model_name = f"ar_{label}"
            print("training", model_name)
            self.ar_models[label] = self.train_model(
                    dp_train, dp_labels[label], **parameters[label-1]
                )
            pickle.dump(
                self.ar_models[label],
                open(MODEL_PATH.format(model_name), "wb"),
            )

            model_name = f"dp_{label}"
            print("training", model_name)
            self.dp_models[label] = self.train_model(
                    dp_train, dp_labels[label], **parameters[label-1]
                )
            pickle.dump(
                self.dp_models[label],
                  # **parameters[label] # n_estimators=50, max_depth=6
                open(MODEL_PATH.format(model_name), "wb"),
            )


if __name__ == "__main__":
    pred = Predictor()
