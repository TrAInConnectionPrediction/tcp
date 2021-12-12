import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
from xgboost import XGBClassifier
import xgboost
import datetime
import pandas as pd
import optuna
import pickle
import dask.dataframe as dd
from database import DB_CONNECT_STRING
from config import ENCODER_PATH, RTD_CACHE_PATH


CLASSES_TO_COMPUTE = range(16)


class Datasets:
    def __init__(self):
        super().__init__()

        self.status_encoder = {}
        self.status_encoder["ar"] = pickle.load(
            open(ENCODER_PATH.format(encoder="ar_cs"), "rb")
        )
        self.status_encoder["dp"] = pickle.load(
            open(ENCODER_PATH.format(encoder="dp_cs"), "rb")
        )

        self.rtd = dd.read_parquet(RTD_CACHE_PATH + '_hyper_dataset', engine='pyarrow').persist()

    def get_sets(self, current_date, days_for_training, threshhold_minutes, ar_or_dp):
        """Generate train and test data

        Parameters
        ----------
        current_date : datetime.datetime
            Date to simulate the production training. The current
            date is used to split training and testing data. Dates befor
            current_date will be used for training, dates after current_date
            for testing
        days_for_training : int
            The number of days to use as training data.
        threshhold_minutes : int
            Threshhold for labels
        ar_or_dp : str
            Wether to make sets for arrival or departure

        Returns
        -------
        tuple (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame)
            (train_x, train_y, test_x, test_y)
        """
        min_train_date = current_date - datetime.timedelta(days=days_for_training)
        train_filter = (
            (self.rtd["ar_pt"] >= min_train_date)
            | (self.rtd["dp_pt"] >= min_train_date)
        ) & ((self.rtd["ar_pt"] < current_date) | (self.rtd["dp_pt"] < current_date))

        max_test_date = current_date + datetime.timedelta(days=14)
        test_filter = (
            (self.rtd["ar_pt"] >= current_date) | (self.rtd["dp_pt"] >= current_date)
        ) & ((self.rtd["ar_pt"] < max_test_date) | (self.rtd["dp_pt"] < max_test_date))


        if ar_or_dp == "ar":
            train_filter = train_filter & (
                ~self.rtd["ar_delay"].isna()
                | (self.rtd["ar_cs"] == self.status_encoder["ar"]["c"])
            )
            test_filter = test_filter & (
                ~self.rtd["ar_delay"].isna()
                | (self.rtd["ar_cs"] == self.status_encoder["ar"]["c"])
            )
            train_y = (self.rtd.loc[train_filter, "ar_delay"] <= threshhold_minutes) & (
                self.rtd.loc[train_filter, "ar_cs"] != self.status_encoder["ar"]["c"]
            )

            test_y = (self.rtd.loc[test_filter, "ar_delay"] <= threshhold_minutes) & (
                self.rtd.loc[test_filter, "ar_cs"] != self.status_encoder["ar"]["c"]
            )

        else:
            train_filter = train_filter & (
                ~self.rtd["dp_delay"].isna()
                | (self.rtd["dp_cs"] == self.status_encoder["dp"]["c"])
            )
            test_filter = test_filter & (
                ~self.rtd["dp_delay"].isna()
                | (self.rtd["dp_cs"] == self.status_encoder["dp"]["c"])
            )
            train_y = (self.rtd.loc[train_filter, "dp_delay"] >= threshhold_minutes) & (
                self.rtd.loc[train_filter, "dp_cs"] != self.status_encoder["dp"]["c"]
            )

            test_y = (self.rtd.loc[test_filter, "dp_delay"] >= threshhold_minutes) & (
                self.rtd.loc[test_filter, "dp_cs"] != self.status_encoder["dp"]["c"]
            )

        train_x = self.rtd.loc[train_filter]
        test_x = self.rtd.loc[test_filter]

        train_x = train_x.drop(
            columns=[
                "ar_delay",
                "dp_delay",
                "ar_cs",
                "dp_cs",
                "ar_pt",
                "ar_ct",
                "dp_pt",
                "dp_ct",
            ],
            axis=0,
        )
        test_x = test_x.drop(
            columns=[
                "ar_delay",
                "dp_delay",
                "ar_cs",
                "dp_cs",
                "ar_pt",
                "ar_ct",
                "dp_pt",
                "dp_ct",
            ],
            axis=0,
        )

        return train_x.compute(), train_y.compute(), test_x.compute(), test_y.compute()


def objective(trial, threshhold_minutes, ar_or_dp):
    global datasets

    param = {
        "verbosity": 0,
        "objective": "binary:logistic",
        "n_jobs": -1,
        "random_state": 0,
        "tree_method": "gpu_hist",
        "use_label_encoder": False,
        "n_estimators": trial.suggest_int("n_estimators", 20, 100),
        "max_depth": trial.suggest_int("max_depth", 1, 20, log=True),
        "gamma": trial.suggest_float("gamma", 1e-8, 10, log=True),
        "learning_rate": trial.suggest_float("learning_rate", 1e-3, 1.0, log=True),
    }
    days_for_training = 28 # trial.suggest_int("days_for_training", 2, 28)
    scores = []
    for current_date in pd.date_range(
        datetime.datetime(2021, 1, 28), datetime.datetime(2021, 2, 17), freq="4D"
    ):
        print(current_date)
        train_x, train_y, test_x, test_y = datasets.get_sets(
            current_date, days_for_training, threshhold_minutes, ar_or_dp
        )
        est = XGBClassifier(**param)
        est.fit(train_x, train_y)
        pred_y = est.predict(test_x)
        scores.append((pred_y == test_y).sum() / len(pred_y))
    return sum(scores) / len(scores)


if __name__ == "__main__":
    from helpers import fancy_print_tcp

    from dask.distributed import Client

    client = Client(n_workers=min(16, os.cpu_count()))
    datasets = Datasets()
    print("created datasets")

    for ar_or_dp in ["ar", "dp"]:
        for threshhold in CLASSES_TO_COMPUTE:
            train_x, train_y, test_x, test_y = datasets.get_sets(
                datetime.datetime(2021, 2, 17), 28, threshhold, ar_or_dp
            )
            baseline = train_y.sum() / len(train_y)
            print(ar_or_dp, threshhold, '\t', baseline, '\t', 1 - baseline)

    for ar_or_dp in ["ar", "dp"]:
        for threshhold in CLASSES_TO_COMPUTE:

            current_objective = lambda trial: objective(trial, threshhold, ar_or_dp)

            study = optuna.create_study(
                direction="maximize",
                study_name=str(threshhold) + "_" + ar_or_dp,
                sampler=optuna.samplers.TPESampler(seed=0),
            )
            study.optimize(
                current_objective,
                # n_trials=1,
                n_trials=100,
                n_jobs=6,
                catch=(xgboost.core.XGBoostError,),
                gc_after_trial=True,
            )

            print("Number of finished trials: ", len(study.trials))
            print("Best trial:")
            trial = study.best_trial

            print("  Value: {}".format(trial.value))
            print("  Params: ")
            for key, value in trial.params.items():
                print("    {}: {}".format(key, value))
            print({**trial.params, "value": trial.value, "trials": len(study.trials)})
            pd.DataFrame(
                {**trial.params, "value": trial.value, "trials": len(study.trials)},
                index=[str(threshhold) + "_" + ar_or_dp],
            ).to_sql("hyperparametertuning_2", con=DB_CONNECT_STRING, if_exists="append")
