import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pickle
from xgboost import XGBClassifier
from sklearn.dummy import DummyClassifier
from helpers.RtdRay import RtdRay
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from config import MODEL_PATH, ENCODER_PATH, CACHE_PATH

# CACHE_PATH = "cache/models/model_{}.pkl"
CLASSES_TO_COMPUTE = [0, 2, 5, 8]  # range(10)


def train_model(train_x, train_y, **model_parameters):
    print("Majority Baseline during training:", majority_baseline(train_x, train_y))
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


def train_models(**load_parameters):
    rtd_ray = RtdRay()
    train = rtd_ray.load_for_ml_model(**load_parameters).compute()
    status_encoder = {}
    status_encoder["ar"] = pickle.load(open(ENCODER_PATH.format(encoder="ar_cs"), "rb"))
    status_encoder["dp"] = pickle.load(open(ENCODER_PATH.format(encoder="dp_cs"), "rb"))

    ar_train = train.loc[
        ~train["ar_delay"].isna() | (train["ar_cs"] == status_encoder["ar"]["c"])
    ]
    dp_train = train.loc[
        ~train["dp_delay"].isna() | (train["dp_cs"] == status_encoder["dp"]["c"])
    ]
    del train

    ar_labels = {}
    dp_labels = {}
    for label in CLASSES_TO_COMPUTE:
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

    parameters = pickle.load(open(CACHE_PATH + "/hyperparameters.pkl", "rb"))

    for label in CLASSES_TO_COMPUTE:
        model_name = f"ar_{label}"
        print("training", model_name)
        pickle.dump(
            train_model(ar_train, ar_labels[label], **parameters[label]),
            open(MODEL_PATH.format(model_name), "wb"),
        )

        label += 1
        model_name = f"dp_{label}"
        print("training", model_name)
        pickle.dump(
            train_model(
                dp_train, dp_labels[label], **parameters[label-1]
            ),  # **parameters[label] # n_estimators=50, max_depth=6
            open(MODEL_PATH.format(model_name), "wb"),
        )


def majority_baseline(x, y):
    clf = DummyClassifier(strategy="most_frequent", random_state=0)
    clf.fit(x, y)
    return round(clf.score(x, y), 6)

def model_score(model, x, y):
    return model.score(x, y)


def test_model(model, x_test, y_test, model_name):
    baseline = majority_baseline(x_test, y_test)
    model_score = model.score(x_test, y_test)
    
    print("Model:", model_name)
    print("Majority baseline:\t", round(baseline * 100, 6))
    print("Model accuracy:\t\t", round(model_score * 100, 6))
    print("Model improvement:\t", round((model_score - baseline), 6))


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


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    # train_models(
    #     max_date=datetime.datetime(2021, 2, 1),
    #     min_date=datetime.datetime(2021, 2, 1) - datetime.timedelta(days=7 * 2),
    #     long_distance_only=False,
    #     return_status=True,
    # )
 
    status_encoder = {}
    status_encoder["ar"] = pickle.load(open(ENCODER_PATH.format(encoder="ar_cs"), "rb"))
    status_encoder["dp"] = pickle.load(open(ENCODER_PATH.format(encoder="dp_cs"), "rb"))

    rtd_ray = RtdRay()
    test = rtd_ray.load_for_ml_model(
        max_date=datetime.datetime(2021, 2, 1),
        min_date=datetime.datetime(2021, 2, 1) - datetime.timedelta(days=7 * 2),
        long_distance_only=False,
        return_status=True,
    ).compute()
    ar_test = test.loc[
        ~test["ar_delay"].isna() | (test["ar_cs"] == status_encoder["ar"]["c"]),
        ["ar_delay", "ar_cs"],
    ]
    dp_test = test.loc[
        ~test["dp_delay"].isna() | (test["dp_cs"] == status_encoder["dp"]["c"]),
        ["dp_delay", "dp_cs"],
    ]
    # ar_test = test[['ar_delay', 'ar_cs']].dropna(subset=["ar_delay"])
    # dp_test = test[['dp_delay', 'dp_cs']].dropna(subset=["dp_delay"])

    ar_test_x = test.loc[
        ~test["ar_delay"].isna() | (test["ar_cs"] == status_encoder["ar"]["c"])
    ].drop(columns=["ar_delay", "dp_delay", "ar_cs", "dp_cs"], axis=0)
    dp_test_x = test.loc[
        ~test["dp_delay"].isna() | (test["dp_cs"] == status_encoder["dp"]["c"])
    ].drop(columns=["ar_delay", "dp_delay", "ar_cs", "dp_cs"], axis=0)
    del test

    for model_number in CLASSES_TO_COMPUTE:
        model_name = f"ar_{model_number}"
        print("test_results for model {}".format(model_name))
        test_y = (ar_test["ar_delay"] <= model_number) & (
            ar_test["ar_cs"] != status_encoder["ar"]["c"]
        )
        model = pickle.load(open(MODEL_PATH.format(model_name), "rb"))
        test_model(model, ar_test_x, test_y, model_name)

        model_number += 1
        model_name = f"dp_{model_number}"
        print("test_results for model {}".format(model_name))
        test_y = (dp_test["dp_delay"] >= model_number) & (
            dp_test["dp_cs"] != status_encoder["dp"]["c"]
        )
        model = pickle.load(open(MODEL_PATH.format(model_name), "rb"))
        test_model(model, dp_test_x, test_y, model_name)
