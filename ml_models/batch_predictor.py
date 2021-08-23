import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pickle
import pandas as pd
import numpy as np


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
            self.ar_models.append(pickle.load(open("cache/models/model_{}.pkl".format('ar_' + str(model)), "rb")))
            self.dp_models.append(pickle.load(open("cache/models/model_{}.pkl".format('dp_' + str(model)), "rb")))

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
                con_score[mask] = ar_prediction[mask, tra_time - 2] * dp_prediction[mask, 0]
                con_score[mask] = con_score[mask] \
                                  + np.sum((ar_prediction[mask, tra_time-1:] - ar_prediction[mask, tra_time-2:-1])
                                           * dp_prediction[mask, 1:2-tra_time or dp_prediction.shape[1]], axis=1)
        return np.minimum(con_score, np.ones(len(con_score)))


if __name__ == "__main__":
    pred = Predictor()
    from helpers import ConnectionRay
    con_ray = ConnectionRay()
    ar_rtd = con_ray.load_ar(return_times=True)
    ar_rtd = ar_rtd.drop(columns=['dp_pt', 'dp_ct', 'ar_delay', 'dp_delay'], axis=0).head(10000).reset_index(drop=True)

    dp_rtd = con_ray.load_dp(return_times=True)
    dp_rtd = dp_rtd.drop(columns=['ar_pt', 'ar_ct', 'ar_delay', 'dp_delay'], axis=0).head(10000).reset_index(drop=True)

    transfer_time = ((dp_rtd['dp_pt'] - ar_rtd['ar_pt']) / pd.Timedelta(minutes=1)).astype('int')
    real_transfer_time = ((dp_rtd['dp_ct'] - ar_rtd['ar_ct']) / pd.Timedelta(minutes=1)).astype('int')

    ar_rtd = ar_rtd.drop(columns=['ar_pt', 'ar_ct'])
    dp_rtd = dp_rtd.drop(columns=['dp_pt', 'dp_ct'])

    ar_pred = pred.predict_ar(ar_rtd)
    dp_pred = pred.predict_dp(dp_rtd)
    
    con_score = pred.predict_con(ar_pred, dp_pred, transfer_time)

    mask = (transfer_time == 5) & (real_transfer_time < 60) & (real_transfer_time > -60)
    con_score = con_score[mask]
    real_transfer_time = real_transfer_time[mask]
    transfer_time = transfer_time[mask]

    import matplotlib.pyplot as plt
    fig1, ax1 = plt.subplots()
    ax1.set_xlabel('Verbindungsscore', fontsize=20)
    ax1.set_ylabel('Geplante Umsteigezeit in Min.', fontsize=20)
    ax1.set_xlim(con_score.min(), con_score.max())
    ax1.grid(True)
    ax1.set_ylim(real_transfer_time.min(), real_transfer_time.max())
    ax1.scatter(x=con_score, y=transfer_time, color="blue", alpha=0.05, edgecolor='none')
    # calc the trendline
    z = np.polyfit(con_score, transfer_time, 1)
    p = np.poly1d(z)
    ax1.plot([con_score.min(), con_score.max()], p([con_score.min(), con_score.max()]), color='red')
    # the line equation:
    print("y=%.6fx+(%.6f)"%(z[0],z[1]))

    fig2, ax2 = plt.subplots()
    ax2.set_xlabel('Verbindungsscore', fontsize=20)
    ax2.set_ylabel('Tatsächliche Umsteigezeit in Min.', fontsize=20)
    ax2.set_xlim(con_score.min(), con_score.max())
    ax2.grid(True)
    ax2.set_ylim(real_transfer_time.min(), real_transfer_time.max())
    ax2.scatter(x=con_score, y=real_transfer_time, color="blue", alpha=0.05, edgecolor='none')
    # calc the trendline
    z = np.polyfit(con_score, real_transfer_time, 1)
    p = np.poly1d(z)
    ax2.plot([con_score.min(), con_score.max()], p([con_score.min(), con_score.max()]), color='red')
    # the line equation:
    print("y=%.6fx+(%.6f)"%(z[0],z[1]))
    plt.show()
