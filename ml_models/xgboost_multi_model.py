import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pickle
from xgboost import XGBClassifier
from helpers.RtdRay import RtdRay
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from config import MODEL_PATH

CACHE_PATH = "cache/models/model_{}.pkl"
CLASSES_TO_COMPUTE = [0, 2, 5, 8]  # range(10)


def train_model(train_x, train_y, **model_parameters):
    est = XGBClassifier(
        n_jobs=-1,
        objective="binary:logistic",
        random_state=0,
        tree_method="gpu_hist",
        use_label_encoder=False,
        **model_parameters,
    )
    est.fit(train_x, train_y)
    return est


def test_model(model, x_test, y_test, model_name):
    from sklearn.dummy import DummyClassifier

    print("Model:", model_name)

    clf = DummyClassifier(strategy="most_frequent", random_state=0)
    clf.fit(x_test, y_test)
    baseline = clf.score(x_test, y_test)
    print("Majority baseline:\t", round(baseline * 100, 4), "%")

    prediction = model.predict(x_test)
    model_score = np.sum(prediction == y_test) / len(y_test)
    print("Model accuracy:\t\t", round(model_score * 100, 4), "%")

    print("Model improvement:\t", round((model_score - baseline) * 100, 4), "%")


def model_roc(model, x_test, y_test, model_name):
    from sklearn.metrics import (
        precision_recall_curve,
        plot_precision_recall_curve,
        auc,
        roc_curve,
    )

    prediction = model.predict_proba(x_test)[:, 1]
    fpr, tpr, thresholds = roc_curve(y_test, prediction, pos_label=1)
    roc_auc = auc(fpr, tpr)

    lw = 2
    fig, ax = plt.subplots()
    ax.plot(
        fpr, tpr, color="darkorange", lw=lw, label="ROC curve (area = %0.2f)" % roc_auc
    )
    ax.plot([0, 1], [0, 1], color="navy", lw=lw, linestyle="--")
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title("Receiver operating characteristic model {}".format(model_name))
    ax.legend(loc="lower right")

    # fig1, ax1 = plt.subplots()
    # ax1.set_title('Predictions')
    # ax1.boxplot(prediction)
    plt.show()

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


def train_models(**load_parameters):
    rtd_ray = RtdRay()
    train = rtd_ray.load_for_ml_model(**load_parameters).compute()

    ar_train = train.dropna(subset=["ar_delay"])
    dp_train = train.dropna(subset=["dp_delay"])
    del train

    ar_labels = {}
    dp_labels = {}
    for label in CLASSES_TO_COMPUTE:
        ar_labels[label] = ar_train["ar_delay"] <= label
        dp_labels[label] = dp_train["dp_delay"] >= label

    del ar_train["ar_delay"]
    del ar_train["dp_delay"]
    del dp_train["ar_delay"]
    del dp_train["dp_delay"]

    newpath = "cache/models"
    if not os.path.exists(newpath):
        os.makedirs(newpath)

    parameters = pickle.load(open("cache/hyperparameters.pkl", "rb"))

    for label in CLASSES_TO_COMPUTE:
        pickle.dump(
            train_model(ar_train, ar_labels[label], **parameters[label]),
            open(MODEL_PATH.format("ar_" + str(label)), "wb"),
        )
        print("trained", "ar_" + str(label))

        pickle.dump(
            train_model(dp_train, dp_labels[label], **parameters[label]),
            open(MODEL_PATH.format("dp_" + str(label)), "wb"),
        )
        print("trained", "dp_" + str(label))


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    train_models(
        max_date=datetime.datetime(2021, 2, 1),
        # min_date=datetime.datetime(2021, 2, 1) - datetime.timedelta(days=7*4),
        long_distance_only=True,
    )

    rtd_ray = RtdRay()
    test = rtd_ray.load_for_ml_model(
        max_date=datetime.datetime(2021, 2, 1) + datetime.timedelta(days=7 * 4),
        min_date=datetime.datetime(2021, 2, 1),
        long_distance_only=True,
    ).compute()
    test_x = test.drop(columns=["ar_delay", "dp_delay"], axis=0)

    for model_number in CLASSES_TO_COMPUTE:
        model_name = f"ar_{model_number}"
        print("test_results for model {}".format(model_name))
        test_y = test["ar_delay"] <= model_number
        model = pickle.load(open(MODEL_PATH.format(model_name), "rb"))
        test_model(model, test_x, test_y, model_name)
