import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import request, jsonify, current_app, Blueprint
from flask.helpers import send_file

from datetime import datetime
import numpy as np

from webserver.connection import (
    datetimes_to_text,
    get_connections,
)
from webserver import predictor, streckennetz, per_station_time
from webserver.db_logger import log_activity
from data_analysis import data_stats, ml_stats
from config import CACHE_PATH

bp = Blueprint("api", __name__, url_prefix="/api")


def analysis(connection: dict):
    """
    Analyses/evaluates/rates a given connection using machine learning

    Parameters
    ----------
    connection : dict
        The connection to analyze

    Returns
    -------
    dict
        The connection with the evaluation/rating
    """
    ar_data, dp_data = predictor.get_pred_data(connection["segments"])
    ar_prediction = predictor.predict_ar(ar_data)
    dp_prediction = predictor.predict_dp(dp_data)
    transfer_time = np.array(
        [segment["transfer_time"] for segment in connection["segments"][:-1]]
    )
    con_scores = predictor.predict_con(ar_prediction[:-1], dp_prediction[1:], transfer_time)
    connection["summary"]["score"] = int(round(con_scores.prod() * 100))
    for i in range(len(connection["segments"]) - 1):
        connection["segments"][i]["score"] = int(round(con_scores[i] * 100))
    for i in range(len(connection["segments"])):
        connection["segments"][i]["ar_delay"] = ar_prediction[i, 0]
        connection["segments"][i]["dp_delay"] = 1 - dp_prediction[i, 1]
    return connection


@bp.route("/connect", methods=["GET"])
@log_activity
def connect():
    """
    Gets called when the website is loaded
    And gets some data from and about the user
    It returns the trainstations for the autofill forms

    Returns
    -------
    flask generated json
        list: a list of strings with all the known train stations
    """
    resp = jsonify({"stations": streckennetz.sta_list})
    # resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/trip", methods=["POST"])
@log_activity
def trip():
    """
    Gets a connection from `startbhf` to `zielbhf` at a given date `date`
    using marudors HAFAS api. And rates the connection

    Parameters
    ----------
    start : str
        (from request) the station name where to start
    destination : str
        (from request) the station name of the destination
    date  : str
        (from request) the date and time at which the trip should take place in format `%d.%m.%Y %H:%M`

    Returns
    -------
    flask generated json
        All the possible connections
    """
    start = request.json['start']
    destination = request.json['destination']
    date = datetime.strptime(request.json['date'], "%d.%m.%Y %H:%M")

    # optional:
    search_for_departure = request.json['search_for_departure'] if 'search_for_departure' in request.json else True

    current_app.logger.info(
        "Getting connections from " + start + " to " + destination + ", " + str(date)
    )
    connections = get_connections(
        start=start,
        destination=destination,
        date=date,
        search_for_departure=search_for_departure,
    )

    for i in range(len(connections)):
        connections[i] = analysis(connections[i])
        connections[i] = datetimes_to_text(connections[i])

    resp = jsonify(connections)
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/stats")
@log_activity
def stats():
    """
    Returns the stats stored in `{cache}/stats.json`
    Usually the stats of our train data

    Parameters
    ----------

    Returns
    -------
    flask generated json
        The stats
    """
    return data_stats.load_stats()


@bp.route("/stationplot/<string:date_range>.webp")
@log_activity
def station_plot(date_range):
    """
    Generates a plot that visualizes all the delays
    between the two dates specified in the url.

    Parameters
    ----------
    date_range : string
        The date range to generate the plot of in format `%d.%m.%Y, %H:%M-%d.%m.%Y, %H:%M`

    Returns
    -------
    image/webp
        The generated plot
    """

    if date_range in per_station_time.DEFAULT_PLOTS:
        path_to_plot = per_station_time.generate_default(title=date_range)
    else:
        date_range = date_range.split("-")
        path_to_plot = per_station_time.generate_plot(
            datetime.strptime(date_range[0], "%d.%m.%Y"),
            datetime.strptime(date_range[1], "%d.%m.%Y"),
            use_cached_images=True,
        )

    current_app.logger.info(f"Returning plot: {path_to_plot}")
    # For some fucking reason flask searches the file from inside webserver so we have to go back a bit
    # even though os.path.isfile('cache/plot_cache/'+ plot_name + '.png') works
    return send_file(
        path_to_plot, mimetype="image/webp"
    )

@bp.route("/stationplot/limits")
@log_activity
def limits():
    """
    Returns the current datetime limits between which we can generate plots.

    Returns
    -------
    {
        "min": <min_date>,
        "max": <max_date>
    }
    """
    limits = per_station_time.limits()
    limits['min'] = limits['min'].date().isoformat()
    limits['max'] = limits['max'].date().isoformat()
    return limits

@bp.route("/obstacleplot/<string:date_range>.png")
@log_activity
def obstacle_plot(date_range):
    """
    Generates a plot that visualizes all the delays
    between the two dates specified in the url.

    Parameters
    ----------
    date_range : string
        The date range to generate the plot of in format `%d.%m.%Y, %H:%M-%d.%m.%Y, %H:%M`

    Returns
    -------
    flask generated image/png
        The generated plot
    """

    if date_range in per_station_time.DEFAULT_PLOTS:
        plot_name = date_range
    else:
        date_range = date_range.split("-")
        plot_name = per_station_time.generate_plot(
            datetime.strptime(date_range[0], "%d.%m.%Y %H:%M"),
            datetime.strptime(date_range[1], "%d.%m.%Y %H:%M"),
            use_cached_images=True,
        )

    current_app.logger.info(f"Returning plot: cache/plot_cache/{plot_name}.png")
    # For some fucking reason flask searches the file from inside webserver so we have to go back a bit
    # even though os.path.isfile('cache/plot_cache/'+ plot_name + '.png') works
    return send_file(
        f"{CACHE_PATH}/plot_cache/{plot_name}.png", mimetype="image/png"
    )

@bp.route("/ml_stats")
@log_activity
def stats_ml():
    """
    Retrives stats for the machine learning models

    Parameters
    ----------

    Returns
    -------
    The statisics for the ml models
    """

    ml_stats.load_stats()
