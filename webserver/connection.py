import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functools import lru_cache
import requests
from datetime import datetime, timedelta
from pytz import timezone
import logging
from helpers.StationPhillip import StationPhillip

logger = logging.getLogger(__name__)

stations = StationPhillip()


def to_local(dt):
    """ Setzt automatich in die Zeitzone in unsere und convertiert die Zeit"""
    return dt.astimezone(timezone("Europe/Berlin"))


def to_utc(dt):
    """ Setzt automatich in die Zeitzone"""
    return dt.astimezone(timezone("UTC"))


def set_to_utc(dt, diff):
    """ Setzt die Zeitzone des Datetime objects auf utc, diff ist die differenz zu utc"""
    # also um bsp Berlin ist die dif 1 weil wir in GMT+1 leben und
    # somit um auf ut zu kommen müssen -1 h machen
    return dt.replace(tzinfo=timezone("UTC")) - timedelta(hours=diff)


def get_connection(start, destination, time):
    """ Get connections using marudor hafas api \n
        string ```start```: start station name \\
        string ```destination```: destination station name \\
        datetime ```time```: time of departure \n
        \bReturn: text of the request
        """
    # marudor will unix zeit in UTC zeit aber .timestamp macht des
    # automatich wenn die Zeitzone gesetzt ist
    json = {
        "start": str(stations.get_eva(name=start)),
        "destination": str(stations.get_eva(name=destination)),
        "time": set_to_utc(time, 1).timestamp() * 1000,
        "maxChanges": "-1",
        "transferTime": "0",
        "hafasProfile": "db",
    }
    logger.debug(json)
    r = requests.post(
        "https://marudor.de/api/hafas/v2/tripSearch?profile=db", json=json
    )
    logger.debug(r.status_code)
    return r.text


def get_trips_of_trains(connection):
    for i in range(len(connection)):
        for n in range(len(connection[i]["segments"])):
            try:
                jid = connection[i]["segments"][n]["jid"]
                connection[i]["segments"][n]["full_trip"] = get_trip_of_train(jid)
            except KeyError:
                pass  # Fußweg has no jid
    return connection


@lru_cache
def get_trip_of_train(jid):
    r = requests.get(
        "https://marudor.de/api/hafas/v1/journeyDetails?jid={}?profile=db".format(
            jid
        )
    )
    trip = r.json()["stops"]
    trip = [int(stop["station"]["id"]) for stop in trip]
    return trip


def clean_data(connection):
    """ Remove unneded content"""
    for i in range(len(connection)):
        # we need the segmentTypes
        connection[i]["segments"].append(
            {"segmentTypes": connection[i]["segmentTypes"]}
        )
        connection[i] = connection[i]["segments"]
        for n in range(len(connection[i])):
            if "wings" in connection[i][n]:
                del connection[i][n]["wings"]
            if "messages" in connection[i][n]:
                del connection[i][n]["messages"]
            if "jid" in connection[i][n]:
                del connection[i][n]["jid"]
            if "type" in connection[i][n]:
                del connection[i][n]["type"]
            if "stops" in connection[i][n]:
                del connection[i][n]["stops"]
            if "finalDestination" in connection[i][n]:
                del connection[i][n]["finalDestination"]
            connection[i][-1]["segmentTypes"] = list(
                dict.fromkeys(connection[i][-1]["segmentTypes"])
            )
    return connection
