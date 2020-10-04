import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flask import Flask, render_template, request, jsonify, Response, current_app
from flask import Blueprint
from datetime import datetime, timedelta
from pytz import timezone
import json
import pandas as pd
import logging
import os
import subprocess

from helpers.StationPhillip import StationPhillip
from server.connection import get_connection, clean_data, get_trips_of_trains
from server.predictor import Predictor

bp = Blueprint("api", __name__, url_prefix="/api")

logger = logging.getLogger(__name__)
basepath = os.path.dirname(os.path.realpath(__file__))

stations = StationPhillip()
pred = Predictor()


def fromUnix(unix):
    """
    Convert from unix timestamp to GMT+1
    Removes millisecons (/1000) add one hour (+3600) and set timezone

    Args:
        unix (int): UNIX timestamp with milliseconds

    Returns:
        datetime: a datetime object with GMT+1 Timezone set
    """

    return datetime.utcfromtimestamp(float(unix) / 1000 + 3600).replace(
        tzinfo=timezone("Europe/Berlin")
    )


def analysis(connection):
    """
    Analyses/evaluates/rates a given connection using machine learning
    Args:
        connection (dict/json): The connection

    Returns:
        dict: the connection with the evaluation/rating
    """
    total_score = 0
    # change between scheduled and predicted time
    time = "scheduledTime"  # 'time'
    connection[-1]["totaltime"] = 0
    connection[-1]["transfers"] = len(connection) - 2

    # sometimes the first segment is a Fußweg. We cannot predict delays for that
    index1 = 1 if (connection[0]["train"]["name"] == "Fußweg") else 0
    index2 = 3 if (connection[-2]["train"]["name"] == "Fußweg") else 2

    # add adelay5 to first and last station
    fs_data = pred.get_pred_data(connection[index1])
    ls_data = pred.get_pred_data(connection[-index2])
    _x, _x, _x, _x, _x, connection[index1]["ddelay5"], _x, _x = pred.predict(fs_data, 0).values()
    _x, connection[-index2]["adelay5"], _x, _x, _x, _x, _x, _x = pred.predict(
        ls_data, 1
    ).values()

    # there are two segments more than connections (= overall info at the end - 1 bc it is like that)
    for i in range(index1, len(connection) - index2):
        if connection[i]["train"]["name"] == "Fußweg":
            connection[-1]["transfers"] -= 1
            continue
        if connection[i + 1]["train"]["name"] != "Fußweg":
            data1 = pred.get_pred_data(connection[i])
            data2 = pred.get_pred_data(connection[i + 1])
            transtime = (
                (
                    fromUnix(connection[i + 1]["departure"][time])
                    - fromUnix(connection[i]["arrival"][time])
                ).seconds
                // 60
            ) % 60  # we just want minutes
            (
                connection[i]["con_score"],
                connection[i]["adelay5"],
                connection[i + 1]["ddelay5"],
            ) = pred.predict_con(data1, data2, transtime)
        else:
            data1 = pred.get_pred_data(connection[i])
            data2 = pred.get_pred_data(connection[i + 2])
            transtime = (
                (
                    fromUnix(connection[i + 2]["departure"][time])
                    - fromUnix(connection[i]["arrival"][time])
                ).seconds
                // 60
            ) % 60  # we just want minutes
            (
                connection[i]["con_score"],
                connection[i]["adelay5"],
                connection[i + 2]["ddelay5"],
            ) = pred.predict_con(data1, data2, transtime)
        if total_score == 0:
            total_score = connection[i]["con_score"]
        else:
            total_score *= connection[i]["con_score"]

    # Calculate total time from start to end
    totaltime = fromUnix(connection[-2]["arrival"][time]) - fromUnix(
        connection[0]["departure"][time]
    )
    # we strip the seconds off the back
    connection[-1]["totaltime"] = str(totaltime)[:-3]

    # When there ist no connection we always give 100% score
    if len(connection) == 2:
        connection[-1]["total_score"] = 100
    else:
        connection[-1]["total_score"] = int(total_score * 100)

    # logger.debug("Verbindungsscore:" + str(total_score))

    for i in range(len(connection) - 1):
        if "ICE" in connection[i]["train"]["name"]:
            connection[i]["train"]["d_type"] = "ice"
        elif (
            "IC" in connection[i]["train"]["name"]
            or "EC" in connection[i]["train"]["name"]
        ):
            connection[i]["train"]["d_type"] = "ic"
        elif (
            "RE" in connection[i]["train"]["name"]
            or "RB" in connection[i]["train"]["name"]
        ):
            connection[i]["train"]["d_type"] = "re"
        elif "S" in connection[i]["train"]["name"]:
            connection[i]["train"]["d_type"] = "s"
        else:
            connection[i]["train"]["d_type"] = "unknown"
    return connection


def calc_con(startbhf, zielbhf, date):
    """
    Gets a connection from ```startbhf``` to ```zielbhf``` at a given date ```date```
    using marudors HAFAS api. And rates the connection.

    Args:
        startbhf (str): Start train station
        zielbhf (str): Destination train station
        date (str): Datetime in format : ```%d.%m.%Y %H:%M```

    Returns:
        dict: a dict with different connections
    """
    logger.info("Getting connections from " + startbhf + " to " + zielbhf + ", " + date)
    connections = get_connection(
        startbhf, zielbhf, datetime.strptime(date, "%d.%m.%Y %H:%M")
    )
    connections = json.loads(connections)["routes"]
    connections = get_trips_of_trains(connections)
    connections = clean_data(connections)

    for i in range(len(connections)):
        connections[i] = analysis(connections[i])
    return connections


@bp.route("/connect", methods=["POST"])
def connect():
    """
    Gets called when the website is loaded
    And gets some data from and about the user
    It returns the trainstations for the autofill forms

    Args:
        screen (from request): the users screensize
        ip (from request): the users public ip
        User-Agent (from request headers): the useragent, which the user uses

    Returns:
        list: a list of strings with all the known train stations
    """
    try:
        logger.info("Screensize: " + request.form["screen"])
        logger.info("IP: " + request.form["ip"])
        logger.info("User-Agent: " + request.headers.get("User-Agent"))
    except:
        # the user doesn't have to send us data
        pass
    data = {"bhf": stations.sta_list}
    resp = jsonify(data)
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/trip", methods=["POST"])
def api():
    """
    Gets a connection from ```startbhf``` to ```zielbhf``` at a given date ```date```
    using marudors HAFAS api. And rates the connection

    Args:
        startbhf (from request): the trainstation from which to start
        zielbhf (from request): the trainstation, which is the destination
        date (from request): the date and time at which the trip should take place

    Returns:
        json: All the possible connections
    """

    # add check for right datetime here

    data = calc_con(
        request.form["startbhf"], request.form["zielbhf"], request.form["date"]
    )
    resp = jsonify(data)
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp


@bp.route("/deploy", methods=["POST"])
def deploy():
    """
    Pulls the newest changes from Github
    And then restarts the systemctl service

    Can be triggered by using for ex.: ```curl --data 'key=DEPLOY_KEY' http://IP/api/deploy```

    Args:
        key (from request): Deploy-key for authenticating the server

    Returns:
        resp: What happend
        code: Specific codes for each outcome
    """

    if request.form["key"] == current_app.config["DEPLOY_KEY"]:
        git = subprocess.run(
            ["/bin/bash", basepath + "/checkgit.sh"], stdout=subprocess.PIPE
        ).stdout.decode("utf-8")

        if git == "1":
            logger.warning("Deploy was requested, but no need to, since I'm up to date")

            return jsonify({"resp": "no need to pull", "code": 1})

        elif git == "2":
            logger.warning("Deploy was requested, and I'm behind, so pulling")
            # I went from git pull and reset hard to first fetch and
            # then merge because i can just overwrite local stuff with the merge
            if "dev" not in request.form and not current_app.debug:
                # ok maybe i still need a reset, when I delete a commit or smth,
                # but without the hard flag
                logger.warning("Reseting git repo since not using the dev flag")
                reset = subprocess.run(
                    ["/usr/bin/git", "-C", basepath, "reset", "--hard", "HEAD^"],
                    stdout=subprocess.PIPE,
                ).stdout.decode("utf-8")
            fetch = subprocess.run(
                ["/usr/bin/git", "-C", basepath, "fetch"], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")
            merge = subprocess.run(
                [
                    "/usr/bin/git",
                    "-C",
                    basepath,
                    "merge",
                    "-s",
                    "recursive",
                    "-X",
                    "theirs",
                    "--no-commit",
                ],
                stdout=subprocess.PIPE,
            ).stdout.decode("utf-8")
            logger.warning("git merge said: " + merge)
            git = subprocess.run(
                ["/bin/bash", basepath + "/checkgit.sh"], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")

            if git == "1":
                logger.warning("Pull was succesfull restarting webserver...")

                response = jsonify(
                    {"resp": "pull was succesfull restarting webserver", "code": 0}
                )

                @response.call_on_close
                def on_close():
                    logger.warning(
                        subprocess.run(
                            ["/bin/bash", basepath + "/restart.sh"],
                            stdout=subprocess.PIPE,
                        ).stdout.decode("utf-8")
                    )
                    return

                return response
            else:
                return jsonify({"resp": "pull did't succeed", "code": -2})

    else:
        return jsonify({"resp": "wrong key", "code": -1})

    return jsonify({"resp": "something went wrong", "code": -3})


@bp.route("/gitid", methods=["POST"])
def gitid():
    """
    Returns the last commit id the repository is on

    Args:
        key (from request): Deploy-key for authenticating the server

    Returns:
        resp: The commitid
        code: Specific codes for each outcome
    """

    if request.form["key"] == current_app.config["DEPLOY_KEY"]:
        git = (
            subprocess.run(
                ["/usr/bin/git", "-C", basepath, "rev-parse", "@"],
                stdout=subprocess.PIPE,
            )
            .stdout.decode("utf-8")
            .replace("\n", "")
        )
        resp = jsonify({"resp": git, "code": 0})
    else:
        resp = jsonify({"resp": "", "code": -1})

    return resp
