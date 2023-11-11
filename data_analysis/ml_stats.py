import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MODEL_PATH, ENCODER_PATH
from database import cached_table_fetch
import matplotlib.pyplot as plt
import pandas as pd
import time
import datetime
from helpers import RtdRay, ttl_lru_cache
from sklearn.dummy import DummyClassifier
import pickle


def majority_baseline(x, y):
    clf = DummyClassifier(strategy="most_frequent", random_state=0)
    clf.fit(x, y)
    return round((clf.predict(x) == y.to_numpy()).sum() / len(x), 6)


def model_score(model, x, y):
    return model.score(x, y)


def test_model(model, x_test, y_test, model_name) -> dict[str, float]:
    baseline = majority_baseline(x_test, y_test)
    model_score = (model.predict(x_test) == y_test).sum() / len(y_test)

    stats = {}

    stats["model"] = model_name
    stats["baseline"] = round(baseline * 100, 6)
    stats["accuracy"] = round(model_score * 100, 6)
    stats["improvement"] = round((model_score - baseline)*100, 6)
    print(stats)
    return stats


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
    ax.set_title(
        "Receiver operating characteristic model {}".format(model_name))
    ax.legend(loc="lower right")

    # fig1, ax1 = plt.subplots()
    # ax1.set_title('Predictions')
    # ax1.boxplot(prediction)
    plt.show()

def generate_stats(n_models = range(15), date = datetime.today()) -> pd.DataFrame:
    """
    Generates stats for the machine learning models for a specific day

    Parameters
    ----------
    n_models : list
        The models for which to compute the stats
    date : datetime
        The day for which to compute the stats

    Returns
    -------
    pd.DataFrame
        DataFrame containing the generated stats
    """
    status_encoder = {}
    status_encoder["ar"] = pickle.load(
        open(ENCODER_PATH.format(encoder="ar_cs"), "rb"))
    status_encoder["dp"] = pickle.load(
        open(ENCODER_PATH.format(encoder="dp_cs"), "rb"))

    # Get midnight
    midnight = datetime.combine(date, time.min)
    test = RtdRay.load_for_ml_model(
        min_date = midnight - datetime.timedelta(days=1),
        max_date = midnight,
        long_distance_only = False,
        return_status = True,
    ).compute()

    ar_test = test.loc[~test["ar_delay"].isna(), ["ar_delay", "ar_cs"]]
    dp_test = test.loc[~test["dp_delay"].isna(), ["dp_delay", "dp_cs"]]
    # ar_test = test[['ar_delay', 'ar_cs']].dropna(subset=["ar_delay"])
    # dp_test = test[['dp_delay', 'dp_cs']].dropna(subset=["dp_delay"])

    ar_test_x = test.loc[~test["ar_delay"].isna()].drop(columns=["ar_delay", "dp_delay", "ar_cs", "dp_cs"], axis=0)
    dp_test_x = test.loc[~test["dp_delay"].isna()].drop(columns=["ar_delay", "dp_delay", "ar_cs", "dp_cs"], axis=0)

    ar_test_x.drop(columns=["obstacles_priority_24", "obstacles_priority_37", "obstacles_priority_63", "obstacles_priority_65", "obstacles_priority_70", "obstacles_priority_80"], inplace = True)
    dp_test_x.drop(columns=["obstacles_priority_24", "obstacles_priority_37", "obstacles_priority_63", "obstacles_priority_65", "obstacles_priority_70", "obstacles_priority_80"], inplace = True)

    del test

    stats = []
    for model_number in n_models:
        model_name = f"ar_{model_number}"

        test_y = (ar_test["ar_delay"] <= model_number) & (
            ar_test["ar_cs"] != status_encoder["ar"]["c"]
        )

        model = pickle.load(open(MODEL_PATH.format(model_name), "rb"))
        
        stats.append(test_model(model, ar_test_x, test_y, model_name))

        # model_number += 1
        model_name = f"dp_{model_number}"
        test_y = (dp_test["dp_delay"] >= model_number) & (
            dp_test["dp_cs"] != status_encoder["dp"]["c"]
        )

        model = pickle.load(open(MODEL_PATH.format(model_name), "rb"))
        stats.append(test_model(model, dp_test_x, test_y, model_name))

    stats = pd.DataFrame(stats)
    stats["date"] = midnight - datetime.timedelta(days=1)

    return stats


@ttl_lru_cache(maxsize=1, seconds_to_live=60*60)
def load_stats(**kwargs) -> dict:
    """Loads stats from database or local

    Parameters
    ----------
    **kwargs : 
        passed to `cached_table_fetch`. See its docstring for more info.

    Returns
    -------
    dict
        Loaded stats
    """
    stats = cached_table_fetch('ml_model_stats', if_exists='append' **kwargs)

    return stats.iloc[0].to_dict()

if __name__ == '__main__':
    stats = load_stats(
        table_generator=generate_stats,
        generate=True,
    )

    print(stats)