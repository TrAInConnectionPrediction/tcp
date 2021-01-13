import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pickle
from xgboost import XGBClassifier
from helpers.RtdRay import RtdRay
from data_analisys.delay import load_for_ml_model
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CACHE_PATH = 'cache/ar_models/model_{}.pkl'


def train_models():
    from dask.distributed import Client

    client = Client()
    rtd = RtdRay()
    rtd_df = load_for_ml_model(max_date=datetime.datetime.today(), min_date=datetime.datetime.today() - datetime.timedelta(days=7*3))
    print('loaded data')

    CLASSES_TO_COMPUTE = range(40)

    train = rtd_df
    del rtd_df
    train = train.compute()
    ar_train = train.dropna(subset=['ar_delay'])
    dp_train = train.dropna(subset=['dp_delay'])
    del train

    ar_labels = {}
    dp_labels = {}
    for label in CLASSES_TO_COMPUTE:
        ar_labels[label] = (ar_train['ar_delay'] <= label)
        dp_labels[label] = (dp_train['dp_delay'] >= label)

    del ar_train['ar_delay']
    del ar_train['dp_delay']
    del dp_train['ar_delay']
    del dp_train['dp_delay']

    models = {}
    for label in CLASSES_TO_COMPUTE:
        est = XGBClassifier(n_estimators=50, max_depth=6, n_jobs=-1, objective='binary:logistic',
                            random_state=0, tree_method='gpu_hist', gpu_id=0)
        est.fit(ar_train, ar_labels[label])
        pickle.dump(est, open(CACHE_PATH.format(label), "wb"))
        print('trained', label)

        est = XGBClassifier(n_estimators=50, max_depth=6, n_jobs=-1, objective='binary:logistic',
                            random_state=0, tree_method='gpu_hist', gpu_id=0)
        est.fit(dp_train, dp_labels[label])
        pickle.dump(est, open(CACHE_PATH.format(label), "wb"))
        print('trained', label)


def test_model(model, x_test, y_test, model_number):
    from sklearn.dummy import DummyClassifier
    from sklearn.metrics import roc_curve, auc

    clf = DummyClassifier(strategy='most_frequent', random_state=0)
    clf.fit(x_test, y_test)
    baseline = clf.score(x_test, y_test)
    print('Majority baseline:\t', round(baseline * 100, 4), '%')

    prediction = model.predict(x_test)
    model_score = np.sum(prediction == y_test) / len(y_test)
    print('Model accuracy:\t\t', round(model_score * 100, 4), '%')

    print('Model improvement:\t', round((model_score - baseline) * 100, 4), '%')

    prediction = model.predict_proba(x_test)[:, 1]
    fpr, tpr, thresholds = roc_curve(y_test, prediction, pos_label=1)
    roc_auc = auc(fpr, tpr)

    lw = 2
    axs[model_number % 5, model_number // 5].plot(fpr, tpr, color='darkorange',
        lw=lw, label='ROC curve (area = %0.2f)' % roc_auc)
    axs[model_number % 5, model_number // 5].plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    axs[model_number % 5, model_number // 5].set_xlim([0.0, 1.0])
    axs[model_number % 5, model_number // 5].set_ylim([0.0, 1.05])
    axs[model_number % 5, model_number // 5].set_xlabel('False Positive Rate')
    axs[model_number % 5, model_number // 5].set_ylabel('True Positive Rate')
    axs[model_number % 5, model_number // 5].set_title('Receiver operating characteristic model {}'.format(model_number))
    axs[model_number % 5, model_number // 5].legend(loc="lower right")

    # fig1, ax1 = plt.subplots()
    # ax1.set_title('Predictions')
    # ax1.boxplot(prediction)
    # plt.show()

    # plt.scatter(prediction, y_test, color='red', label='prediction')
    # from sklearn import linear_model
    # # Create linear regression object
    # regr = linear_model.LinearRegression()

    # # Train the model using the training sets
    # regr.fit(prediction.reshape(-1, 1), y_test)
    # reg_y = regr.predict(prediction.reshape(-1, 1))
    # plt.plot(prediction, reg_y, color='blue', linewidth=3)

    # # plt.axis('tight')
    # plt.axis('scaled')
    # plt.legend()

    # plt.tight_layout()
    # plt.show()

    # from sklearn.metrics import precision_recall_curve
    # from sklearn.metrics import plot_precision_recall_curve

    # disp = plot_precision_recall_curve(model, x_test, y_test)
    # disp.ax_.set_title('2-class Precision-Recall curve')
    # plt.show()


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    # train_models()

    # TODO: Load with RtdRay
    rtd_df = pd.read_pickle('cache/training_rtd.pkl')# .sample(frac=0.01)
    rtd_df = rtd_df.dropna(subset=['ar_delay'])
    test_x = rtd_df.copy()
    del test_x['ar_delay']
    del test_x['dp_delay']
    fig, axs = plt.subplots(5, 8)

    for model_number in range(40):
        print('test_results for model {}'.format(model_number))
        test_y = rtd_df['ar_delay'] <= model_number
        model = pickle.load(open(CACHE_PATH.format(model_number), "rb"))
        test_model(model, test_x, test_y, model_number)
        print('')
    plt.show()
