import os
import sys
from webserver.db_logger import log_activity

from webserver.predictor import from_utc

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functools import lru_cache
import requests
import datetime
from pytz import timezone
from webserver import streckennetz
from concurrent.futures import ThreadPoolExecutor


def get_connections(start, destination, time, max_changes=-1, transfer_time=0, hafas_profile='db'):
    """ Get connections using marudor hafas api

        Parameters
        ----------
        start : string
            start station name
        destination : string
            destination station name
        time : datetime.datetime
            time of departure \n
        
        Returns
        -------
            list : Parsed connections
        """
    json = {
        "start": str(streckennetz.get_eva(name=start)),
        "destination": str(streckennetz.get_eva(name=destination)),
        "time": time.replace(tzinfo=timezone("CET")).isoformat(),
        "maxChanges": max_changes,
        "transferTime": transfer_time,
        "hafasProfile": hafas_profile,
    }
    r = requests.post(
        "https://marudor.de/api/hafas/v3/tripSearch?profile=db", json=json
    )
    connections = parse_connections(r.json())
    return connections


def datetimes_to_text(connection):
    connection['summary']['dp_pt'] = connection['summary']['dp_pt'].strftime("%H:%M")
    connection['summary']['ar_pt'] = connection['summary']['ar_pt'].strftime("%H:%M")

    connection['summary']['dp_ct'] = connection['summary']['dp_ct'].strftime("%H:%M")
    connection['summary']['ar_ct'] = connection['summary']['ar_ct'].strftime("%H:%M")

    for i in range(len(connection['segments'])):
        connection['segments'][i]['dp_pt'] = connection['segments'][i]['dp_pt'].strftime("%H:%M")
        connection['segments'][i]['ar_pt'] = connection['segments'][i]['ar_pt'].strftime("%H:%M")

        connection['segments'][i]['dp_ct'] = connection['segments'][i]['dp_ct'].strftime("%H:%M")
        connection['segments'][i]['ar_ct'] = connection['segments'][i]['ar_ct'].strftime("%H:%M")

    return connection

def parse_connection(connection):
    summary = {}
    segments = []
    summary['dp_station'] = streckennetz.get_name(eva=int(connection['segments'][0]['stops'][0]['station']['id']))
    summary['dp_pt'] = from_utc(connection['segments'][0]['stops'][0]['departure']['scheduledTime'])
    summary['dp_ct'] = from_utc(connection['segments'][0]['stops'][0]['departure']['time'])
    summary['ar_station'] = streckennetz.get_name(eva=int(connection['segments'][-1]['stops'][-1]['station']['id']))
    summary['ar_pt'] = from_utc(connection['segments'][-1]['stops'][-1]['arrival']['scheduledTime'])
    summary['ar_ct'] = from_utc(connection['segments'][-1]['stops'][-1]['arrival']['time'])
    summary['transfers'] = len(connection['segments']) - 1
    summary['train_categories'] = list(set(connection['segmentTypes'])) # get unique categories
    summary['duration'] = str(summary['ar_ct'] - summary['dp_ct'])[:-3]
    segments = []
    for segment in connection['segments']:
        if segment['type'] == 'WALK':
            # Add walking time to last segment and skip walk segment
            segments[-1]['walk'] = (from_utc(segment['arrival']['time'])
                                    - from_utc(segment['departure']['time'])).seconds \
                                    // 60
            # We don't want to count the walk segments as transfers
            summary['transfers'] = summary['transfers'] - 1
            continue
        parsed_segment = {
            'dp_station': streckennetz.get_name(eva=int(segment['stops'][0]['station']['id'])),
            'dp_lat': segment['stops'][0]['station']['coordinates']['lat'],
            'dp_lon': segment['stops'][0]['station']['coordinates']['lng'],
            'dp_pt': from_utc(segment['stops'][0]['departure']['scheduledTime']),
            'dp_ct': from_utc(segment['stops'][0]['departure']['time']),
            'dp_pp': segment['stops'][0]['departure']['scheduledPlatform'] if 'scheduledPlatform' in segment['stops'][0]['departure'] else None,
            'dp_cp': segment['stops'][0]['departure']['platform'] if 'platform' in segment['stops'][0]['departure'] else None,
            'ar_station': streckennetz.get_name(eva=int(segment['stops'][-1]['station']['id'])),
            'ar_lat': segment['stops'][-1]['station']['coordinates']['lat'],
            'ar_lon': segment['stops'][-1]['station']['coordinates']['lng'],
            'ar_pt': from_utc(segment['stops'][-1]['arrival']['scheduledTime']),
            'ar_ct': from_utc(segment['stops'][-1]['arrival']['time']),
            'ar_pp': segment['stops'][-1]['arrival']['scheduledPlatform'] if 'scheduledPlatform' in segment['stops'][-1]['arrival'] else None,
            'ar_cp': segment['stops'][-1]['arrival']['platform'] if 'platform' in segment['stops'][-1]['arrival'] else None,
            'train_name': segment['train']['name'],
            'train_destination': segment['finalDestination'] if 'finalDestination' in segment else streckennetz.get_name(eva=int(segment['stops'][-1]['station']['id'])),
            'ar_c': segment['train']['type'],
            'ar_n': segment['train']['number'],
            'ar_o': segment['train']['admin'].replace('_', ''),
            'dp_c': segment['train']['type'],
            'dp_n': segment['train']['number'],
            'dp_o': segment['train']['admin'].replace('_', ''),
            'walk': 0
        }
        parsed_segment['full_trip'], parsed_segment['stay_times'] = get_trip_of_train(segment['jid'])
        parsed_segment['ar_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['dp_station'])
        parsed_segment['ar_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['ar_station'])
        parsed_segment['duration'] = str(parsed_segment['ar_ct'] - parsed_segment['dp_ct'])[:-3]
        segments.append(parsed_segment)

    # Add transfer times
    for segment in range(len(segments) - 1):
        segments[segment]['transfer_time'] = (segments[segment + 1]['dp_ct']
                                              - segments[segment]['ar_ct']).seconds // 60
    return {'summary': summary, 'segments': segments}

def parse_connections(connections):
    with ThreadPoolExecutor(max_workers=10) as executor:
        parsed = list(executor.map(parse_connection, connections['routes']))
    return parsed


@lru_cache
def get_trip_of_train(jid):
    r = requests.get(
        "https://marudor.de/api/hafas/v2/journeyDetails?jid={}?profile=db".format(
            jid
        )
    )
    trip = r.json()["stops"]
    waypoints = [streckennetz.get_name(eva=int(stop["station"]["id"])) for stop in trip]
    stay_times = [(from_utc(stop["departure"]["scheduledTime"]) \
                  - from_utc(stop["arrival"]["scheduledTime"])).seconds // 60
                  if 'arrival' in stop and 'departure' in stop 
                  else None 
                  for stop 
                  in trip]
    return waypoints, stay_times
