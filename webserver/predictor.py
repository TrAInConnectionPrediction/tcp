import pickle
import pandas as pd
import datetime
import numpy as np
from pytz import timezone
from webserver import streckennetz
from config import ENCODER_PATH, MODEL_PATH


def from_utc(utc_time: str) -> datetime.datetime:
    return datetime.datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone("Europe/Berlin"))


class Predictor:
    def __init__(self):
        self.cat_encoders = {}
        for cat in ["o", "c", "n", "station", 'pp']:
            self.cat_encoders[cat] = pickle.load(
                open(ENCODER_PATH.format(encoder=cat), "rb")
            )
        self.ar_models = []
        self.dp_models = []
        for model in range(40):
            self.ar_models.append(pickle.load(open(MODEL_PATH.format('ar_' + str(model)), "rb")))
            self.dp_models.append(pickle.load(open(MODEL_PATH.format('dp_' + str(model)), "rb")))

    def predict_ar(self, features):
        prediction = np.empty((len(features), 40))
        for model in range(40):
            prediction[:, model] = self.ar_models[model].predict_proba(features, validate_features=True)[:, 1]
        return prediction

    def predict_dp(self, features):
        prediction = np.empty((len(features), 40))
        for model in range(40):
            prediction[:, model] = self.dp_models[model].predict_proba(features, validate_features=True)[:, 1]
        return prediction

    def predict_con(self, ar_prediction, dp_prediction, transfer_time):
        con_score = np.ones(len(transfer_time))
        for tra_time in range(40):
            mask = transfer_time == tra_time
            if mask.any():
                con_score[mask] = ar_prediction[mask, max(tra_time - 2, 0)] * dp_prediction[mask, max(0, 2 - tra_time)]
                con_score[mask] = con_score[mask] + np.sum((
                    ar_prediction[mask, max(tra_time-2, 0)+1:dp_prediction.shape[1] - max(0, 2 - tra_time)]
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
                    columns=[
                        'station',
                        'lat',
                        'lon',
                        'o',
                        'c',
                        'n',
                        'distance_to_start',
                        'distance_to_end',
                        'pp',
                        'stop_id',
                        'minute',
                        'day',
                        'stay_time'
                        ],
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
                    print('unknown {cat}: {value}'.format(cat=cat, value=segment['ar_' + cat]))
                try:
                    if cat == 'pp':
                        dp_data.at[i, cat] = self.cat_encoders[cat][segment['dp_' + 'cp']]
                    else:
                        dp_data.at[i, cat] = self.cat_encoders[cat][segment['dp_' + cat]]
                except KeyError:
                    dp_data.at[i, cat] = -1
                    print('unknown {cat}: {value}'.format(cat=cat, value=segment['dp_' + cat]))
            
            ar_data.at[i, 'lat'] = segment['ar_lat']
            ar_data.at[i, 'lon'] = segment['ar_lon']
            dp_data.at[i, 'lat'] = segment['dp_lat']
            dp_data.at[i, 'lon'] = segment['dp_lon']

            ar_data.at[i, 'stop_id'] = segment['ar_stop_id']
            dp_data.at[i, 'stop_id'] = segment['dp_stop_id']

            ar_data.at[i, 'distance_to_start'] = streckennetz.route_length(segment['full_trip'][:ar_data.at[i, 'stop_id'] + 1])
            ar_data.at[i, 'distance_to_end'] = streckennetz.route_length(segment['full_trip'][ar_data.at[i, 'stop_id']:])
            dp_data.at[i, 'distance_to_start'] = streckennetz.route_length(segment['full_trip'][:dp_data.at[i, 'stop_id'] + 1])
            dp_data.at[i, 'distance_to_end'] = streckennetz.route_length(segment['full_trip'][dp_data.at[i, 'stop_id']:])

            ar_data.at[i, 'minute'] = segment['ar_ct'].time().minute + segment['ar_ct'].time().hour * 60
            ar_data.at[i, 'day'] = segment['ar_ct'].weekday()
            dp_data.at[i, 'minute'] = segment['dp_ct'].time().minute + segment['dp_ct'].time().hour * 60
            dp_data.at[i, 'day'] = segment['dp_ct'].weekday()

            ar_data.at[i, 'stay_time'] = segment['stay_times'][ar_data.at[i, 'stop_id']]
            dp_data.at[i, 'stay_time'] = segment['stay_times'][dp_data.at[i, 'stop_id']]

        return ar_data.astype(dtypes), dp_data.astype(dtypes)


if __name__ == "__main__":
    pred = Predictor()
