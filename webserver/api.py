import os
import sys

from datetime import datetime
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import request, jsonify, current_app, Blueprint
from flask.helpers import send_file

from webserver.connection import (
    datetimes_to_text,
    get_connections,
)
from webserver import pred, streckennetz, per_station_time
from webserver.db_logger import log_activity
from config import CACHE_PATH

bp = Blueprint("api", __name__, url_prefix="/api")


def analysis(connection):
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
    ar_data, dp_data = pred.get_pred_data(connection["segments"])
    ar_prediction = pred.predict_ar(ar_data)
    dp_prediction = pred.predict_dp(dp_data)
    transfer_time = np.array(
        [segment["transfer_time"] for segment in connection["segments"][:-1]]
    )
    con_scores = pred.predict_con(ar_prediction[:-1], dp_prediction[1:], transfer_time)
    connection["summary"]["score"] = int(round(con_scores.prod() * 100))
    for i in range(len(connection["segments"]) - 1):
        connection["segments"][i]["score"] = int(round(con_scores[i] * 100))
    for i in range(len(connection["segments"])):
        connection["segments"][i]["ar_delay"] = ar_prediction[i, 0]
        connection["segments"][i]["dp_delay"] = 1 - dp_prediction[i, 1]
    return connection


@log_activity
def calc_con(start, destination, date):
    """
    Gets a connection from `start` to `destination` at a given date `date`
    using marudors HAFAS api. And evaluates the connection.

    Parameters
    ----------
    startbhf : str
        Start train station
    zielbhf : str
        Destination train station
    date : str
        Datetime in format `%d.%m.%Y %H:%M`

    Returns
    -------
    list
        a list with different connections
    """
    current_app.logger.info(
        "Getting connections from " + start + " to " + destination + ", " + date
    )
    connections = get_connections(
        start, destination, datetime.strptime(date, "%d.%m.%Y %H:%M")
    )

    for i in range(len(connections)):
        connections[i] = analysis(connections[i])
        connections[i] = datetimes_to_text(connections[i])
    return connections


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
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/trip", methods=["POST"])
def api():
    """
    Gets a connection from `startbhf` to `zielbhf` at a given date `date`
    using marudors HAFAS api. And rates the connection

    Parameters
    ----------
    startbhf : string
        (from request) the trainstation from which to start
    zielbhf : string
        (from request) the trainstation, which is the destination
    date  : string
        (from request: the date and time at which the trip should take place in format `%d.%m.%Y %H:%M`

    Returns
    -------
    flask generated json
        All the possible connections
    """
    data = calc_con(
        request.json["start"], request.json["destination"], request.json["date"]
    )
    resp = jsonify(data)
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/stationplot/<string:date_range>.jpg")
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
    flask generated image/jpg
        The generated plot
    """

    if date_range == 'default':
        return send_file(f"../{CACHE_PATH}/plot_cache/default.jpg", mimetype="image/jpg")
        
    date_range = date_range.split("-")
    plot_name = per_station_time.generate_plot(
        datetime.strptime(date_range[0], "%d.%m.%Y %H:%M"),
        datetime.strptime(date_range[1], "%d.%m.%Y %H:%M"),
    )

    current_app.logger.info(
        "Generated plot named cache/plot_cache/" + plot_name + ".jpg"
    )
    # For some fucking reason flask searches the file from inside webserver so we have to go back a bit
    # even though os.path.isfile('cache/plot_cache/'+ plot_name + '.jpg') works
    return send_file(f"../{CACHE_PATH}/plot_cache/{plot_name}.jpg", mimetype="image/jpg")
