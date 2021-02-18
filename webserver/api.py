import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flask import request, jsonify, current_app
from flask import Blueprint
from datetime import datetime
from pytz import timezone
import json
import os
import subprocess
import numpy as np

from webserver.connection import datetimes_to_text, get_connections # , clean_data, get_trips_of_trains
from webserver import pred, streckennetz, basepath
from flask import current_app
from webserver.predictor import from_utc
from webserver.db_logger import log_activity

bp = Blueprint("api", __name__, url_prefix="/api")

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
    ar_data, dp_data = pred.get_pred_data(connection['segments'])
    ar_prediction = pred.predict_ar(ar_data)
    dp_prediction = pred.predict_dp(dp_data)
    transfer_time = np.array([segment['transfer_time']
                             for segment
                             in connection['segments'][:-1]])
    con_scores = pred.predict_con(ar_prediction[:-1], dp_prediction[1:], transfer_time)
    connection['summary']['score'] = int(round(con_scores.prod() * 100))
    for i in range(len(connection['segments']) - 1):
        connection['segments'][i]['score'] = int(round(con_scores[i] * 100))
    for i in range(len(connection['segments'])):
        connection['segments'][i]['ar_delay'] = ar_prediction[i, 0]
        connection['segments'][i]['dp_delay'] = 1 - dp_prediction[i, 1]
    return connection

@log_activity
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
    current_app.logger.info("Getting connections from " + startbhf + " to " + zielbhf + ", " + date)
    connections = get_connections(
        startbhf, zielbhf, datetime.strptime(date, "%d.%m.%Y %H:%M")
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

    Returns:
        list: a list of strings with all the known train stations
    """
    return {"stations": streckennetz.sta_list}


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
    data = calc_con(
        request.json['start'], request.json['destination'], request.json['date']
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
            current_app.logger.warning("Deploy was requested, but no need to, since I'm up to date")

            return jsonify({"resp": "no need to pull", "code": 1})

        elif git == "2":
            current_app.logger.warning("Deploy was requested, and I'm behind, so pulling")
            # I went from git pull and reset hard to first fetch and
            # then merge because i can just overwrite local stuff with the merge
            if "dev" not in request.form and not current_app.debug:
                # ok maybe i still need a reset, when I delete a commit or smth,
                # but without the hard flag
                current_app.logger.warning("Reseting git repo since not using the dev flag")
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
            current_app.logger.warning("git merge said: " + merge)
            git = subprocess.run(
                ["/bin/bash", basepath + "/checkgit.sh"], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")

            if git == "1":
                current_app.logger.warning("Pull was succesfull restarting webserver...")

                response = jsonify(
                    {"resp": "pull was succesfull restarting webserver", "code": 0}
                )

                @response.call_on_close
                def on_close():
                    current_app.logger.warning(
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
