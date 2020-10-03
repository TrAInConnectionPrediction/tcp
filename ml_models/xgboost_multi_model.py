import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pickle
from xgboost import XGBClassifier
from helpers.RtdRay import RtdRay
from data_analisys.delay import load_for_ml_model

if __name__ == '__main__':
    import fancy_print_tcp
    from dask.distributed import Client

    client = Client()
    rtd = RtdRay()
    rtd_df = load_for_ml_model()
    print('loaded data')

    # Split into training and testing data
    train, test = rtd_df.random_split([0.9, 0.1])
    print('split data')
    train = train.compute()

    labels = {}
    labels['ar_0'] = train['ar_delay'] <= 5
    labels['ar_5'] = train['ar_delay'] > 5
    labels['ar_10'] = train['ar_delay'] > 10
    labels['ar_15'] = train['ar_delay'] > 15

    labels['dp_0'] = train['dp_delay'] <= 5
    labels['dp_5'] = train['dp_delay'] > 5
    labels['dp_10'] = train['dp_delay'] > 10
    labels['dp_15'] = train['dp_delay'] > 15

    del train['ar_delay']
    del test['ar_delay']
    del train['dp_delay']
    del test['dp_delay']

    models = {}
    for key in labels:
        est = XGBClassifier(n_estimators=70, max_depth=12, n_jobs=-1,
                            random_state=0)
        est.fit(train, labels[key])
        print('trained', key)
        pickle.dump(est, open("data_buffer/model_{}.pkl".format(key), "wb"))
        print('saved', key)

    print('you are using these columns for training:')
    print(len(train), len(train.columns), train.head())
