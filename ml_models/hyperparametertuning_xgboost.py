import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from xgboost import XGBClassifier
import xgboost
from helpers.RtdRay import RtdRay
import datetime
import pandas as pd
import optuna
from database.engine import engine


class Datasets(RtdRay):
    def __init__(self):
        super().__init__()
        self.CLASSES_TO_COMPUTE = [5, 8, 13, 20, 30] #range(40)
        self.CACHE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) \
                         + '/cache/Datasets/'
        if not os.path.isdir(self.CACHE_DIR):
            os.mkdir(self.CACHE_DIR)

        self.ar_y = {}
        self.dp_y = {}

        try:
            self.ar_rtd = pd.read_pickle(self.CACHE_DIR + 'ar_rtd.pkl')
            self.dp_rtd = pd.read_pickle(self.CACHE_DIR + 'dp_rtd.pkl')

        except FileNotFoundError:
            self.rtd = self.load_for_ml_model(return_date_id=True,
                                              min_date=datetime.datetime(2020, 10, 3),
                                              max_date=datetime.datetime(2020, 11, 10))
            self.ar_rtd = self.rtd.dropna(subset=['ar_delay']).compute()
            self.dp_rtd = self.rtd.dropna(subset=['dp_delay']).compute()
            self.ar_rtd.to_pickle(self.CACHE_DIR + 'ar_rtd.pkl')
            self.dp_rtd.to_pickle(self.CACHE_DIR + 'dp_rtd.pkl')
            del self.rtd

        for label in self.CLASSES_TO_COMPUTE:
            self.ar_y[label] = (self.ar_rtd['ar_delay'] <= label)
            self.dp_y[label] = (self.dp_rtd['dp_delay'] >= label)

        del self.ar_rtd['ar_delay']
        del self.ar_rtd['dp_delay']
        del self.dp_rtd['ar_delay']
        del self.dp_rtd['dp_delay']

    def get_sets(self, current_date, threshhold_minutes, ar_or_dp):
        """Generate train and test data

        Parameters
        ----------
        current_date : datetime.datetime
            Date to simulate the production training (Use the 2 weeks
            before current_date to train and 2 days after it to test )
        threshhold_minutes : int
            Threshhold for labels
        ar_or_dp : str
            Wether to make sets for arrival or departure

        Returns
        -------
        tuple (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame)
            (train_x, train_y, test_x, test_y)
        """
        if ar_or_dp == 'ar':
            train_mask = (self.ar_rtd['date_id'] <= current_date) & \
                         (self.ar_rtd['date_id'] >= current_date - datetime.timedelta(days=7*2))
            train_x = self.ar_rtd.loc[train_mask, :]
            train_y = self.ar_y[threshhold_minutes].loc[train_mask]

            test_mask = (self.ar_rtd['date_id'] > current_date) & \
                         (self.ar_rtd['date_id'] <= current_date + datetime.timedelta(days=3))
            test_x = self.ar_rtd.loc[test_mask, :]
            test_y = self.ar_y[threshhold_minutes].loc[test_mask]
        else:
            train_mask = (self.dp_rtd['date_id'] <= current_date) & \
                         (self.dp_rtd['date_id'] >= current_date - datetime.timedelta(days=7*2))
            train_x = self.dp_rtd.loc[train_mask, :]
            train_y = self.dp_y[threshhold_minutes].loc[train_mask]

            test_mask = (self.dp_rtd['date_id'] > current_date) & \
                         (self.dp_rtd['date_id'] <= current_date + datetime.timedelta(days=3))
            test_x = self.dp_rtd.loc[test_mask, :]
            test_y = self.dp_y[threshhold_minutes].loc[test_mask]
        del train_x['date_id']
        del test_x['date_id']
        return train_x, train_y, test_x, test_y


def objective(trial, threshhold_minutes, ar_or_dp):
    global datasets

    param = {
        "verbosity": 0,
        "objective": "binary:logistic",
        'n_jobs': -1,
        'random_state': 0,
        'tree_method': 'gpu_hist',
        'use_label_encoder': False,

        'n_estimators': trial.suggest_int("n_estimators", 10, 80),
        'max_depth': trial.suggest_int("max_depth", 2, 30, log=True),
        "gamma": trial.suggest_float("gamma", 1e-8, 10, log=True),
        "learning_rate": trial.suggest_float("learning_rate", 1e-3, 1.0, log=True),
    }

    scores = []
    for current_date in pd.date_range(datetime.datetime(2020, 10, 18), datetime.datetime(2020, 11, 6), freq='4D'): # , datetime.datetime(2020, 11, 6)
        train_x, train_y, test_x, test_y = datasets.get_sets(current_date,
                                                             threshhold_minutes,
                                                             ar_or_dp)
        est = XGBClassifier(**param)
        est.fit(train_x, train_y)
        scores.append(est.score(test_x, test_y))
    return sum(scores) / len(scores)


if __name__ == '__main__':
    from helpers import fancy_print_tcp

    from dask.distributed import Client
    client = Client()
    datasets = Datasets()
    print('created datasets')

    for ar_or_dp in ['ar', 'dp']:
        for threshhold in [0, 2, 5, 8, 13, 20, 30]:

            current_objective = lambda trial: objective(trial, threshhold, ar_or_dp)

            study = optuna.create_study(direction="maximize",
                                        study_name=str(threshhold) + '_' + ar_or_dp,
                                        sampler=optuna.samplers.TPESampler(seed=0))
            study.optimize(current_objective,
                           n_trials=100,
                           n_jobs=8,
                           catch=(xgboost.core.XGBoostError,),
                           gc_after_trial=True)

            print("Number of finished trials: ", len(study.trials))
            print("Best trial:")
            trial = study.best_trial

            print("  Value: {}".format(trial.value))
            print("  Params: ")
            for key, value in trial.params.items():
                print("    {}: {}".format(key, value))
            print({**trial.params, 'value': trial.value, 'trials': len(study.trials)})
            pd.DataFrame({**trial.params, 'value': trial.value, 'trials': len(study.trials)}, index=[str(threshhold) + '_' + ar_or_dp])\
                .to_sql('hyperparametertuning', con=engine, if_exists='append')
