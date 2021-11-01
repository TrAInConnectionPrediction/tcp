import os
import sys

from webserver.predictor import from_utc

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from functools import lru_cache
import requests
from pytz import timezone
from webserver import streckennetz
from concurrent.futures import ThreadPoolExecutor
import datetime
from . import client


def get_connections(
    start: str,
    destination: str,
    date: datetime.datetime,
    max_changes: int=-1,
    transfer_time: int=0,
    hafas_profile: str='db',
    economic: bool=False,
    search_for_departure: bool=True,
) -> list:
    """[summary]

    Parameters
    ----------
    start : str
        start station name
    destination : str
        destination station name
    date : datetime.datetime
        date and time of departure \n
    max_changes : int, optional
        Maximum number of allowed changes, by default -1
    transfer_time : int, optional
        Minimal transfer time, by default 0
    hafas_profile : str, optional
        Hafas profile to use, by default 'db'
    economic : bool, optional
        True = not only fastest route, by default False
    search_for_departure : bool, optional
        False = time == arrival time, by default True

    Returns
    -------
        list : Parsed connections
    """    
    json = {
        "start": str(streckennetz.get_eva(name=start)),
        "destination": str(streckennetz.get_eva(name=destination)),
        "time": date.replace(tzinfo=timezone("CET")).isoformat(),
        "maxChanges": max_changes,
        "transferTime": transfer_time,
        "hafasProfile": hafas_profile,
        'economic': economic,
        'searchForDeparture': search_for_departure,
    }
    json['tarif'] = {'class': 2,'traveler':{"type": "E"}}

    r = requests.post(
        "https://marudor.de/api/hafas/v3/tripSearch?profile=db", json=json
    )
    # journeys = client.journeys(
    #     str(streckennetz.get_eva(name=start)),
    #     str(streckennetz.get_eva(name=destination)),
    #     time.replace(tzinfo=timezone("CET")),
    #     max_journeys=6,
    # )
    connections = parse_connections(r.json())
    return connections


def datetimes_to_text(connection):
    connection['summary']['dp_pt'] = connection['summary']['dp_pt'].isoformat()
    connection['summary']['ar_pt'] = connection['summary']['ar_pt'].isoformat()

    connection['summary']['dp_ct'] = connection['summary']['dp_ct'].isoformat()
    connection['summary']['ar_ct'] = connection['summary']['ar_ct'].isoformat()

    for i in range(len(connection['segments'])):
        connection['segments'][i]['dp_pt'] = connection['segments'][i]['dp_pt'].isoformat()
        connection['segments'][i]['ar_pt'] = connection['segments'][i]['ar_pt'].isoformat()

        connection['segments'][i]['dp_ct'] = connection['segments'][i]['dp_ct'].isoformat()
        connection['segments'][i]['ar_ct'] = connection['segments'][i]['ar_ct'].isoformat()

    return connection


def parse_connection(connection):
    summary = {}
    segments = []
    # If the first segment is a WALK, we remove it and let the connection start
    # from the next segment
    if connection['segments'][0]['type'] == 'WALK':
        del connection['segments'][0]

    try:
        summary['dp_station'] = streckennetz.get_name(
            eva=int(connection['segments'][0]['segmentStart']['id'])
        )
    except KeyError:
        summary['dp_station'] = ''
    summary['dp_station_display_name'] = connection['segments'][0]['segmentStart']['title']
    summary['dp_pt'] = from_utc(connection['segments'][0]['departure']['scheduledTime'])
    summary['dp_ct'] = from_utc(connection['segments'][0]['departure']['time'])
    try:
        summary['ar_station'] = streckennetz.get_name(
            eva=int(connection['segments'][-1]['segmentDestination']['id'])
        )
    except KeyError:
        summary['ar_station'] = ''
    summary['ar_station_display_name'] = connection['segments'][-1]['segmentDestination']['title']
    summary['ar_pt'] = from_utc(connection['segments'][-1]['arrival']['scheduledTime'])
    summary['ar_ct'] = from_utc(connection['segments'][-1]['arrival']['time'])
    summary['transfers'] = len(connection['segments']) - 1
    summary['train_categories'] = list(set(connection['segmentTypes'])) # set to get unique categories
    summary['duration'] = str(summary['ar_ct'] - summary['dp_ct'])[:-3]
    summary['price'] = connection['tarifSet'][0]['fares'][0]['price'] if 'tarifSet' in connection else -1
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
            'dp_station_display_name': segment['segmentStart']['title'],
            'dp_lat': segment['stops'][0]['station']['coordinates']['lat'],
            'dp_lon': segment['stops'][0]['station']['coordinates']['lng'],
            'dp_pt': from_utc(segment['departure']['scheduledTime']),
            'dp_ct': from_utc(segment['departure']['time']),
            'dp_pp': segment['departure']['scheduledPlatform'] if 'scheduledPlatform' in segment['departure'] else None,
            'dp_cp': segment['departure']['platform'] if 'platform' in segment['departure'] else None,
            'ar_station_display_name': segment['segmentDestination']['title'],
            'ar_lat': segment['stops'][-1]['station']['coordinates']['lat'],
            'ar_lon': segment['stops'][-1]['station']['coordinates']['lng'],
            'ar_pt': from_utc(segment['arrival']['scheduledTime']),
            'ar_ct': from_utc(segment['arrival']['time']),
            'ar_pp': segment['arrival']['scheduledPlatform'] if 'scheduledPlatform' in segment['arrival'] else None,
            'ar_cp': segment['arrival']['platform'] if 'platform' in segment['arrival'] else None,
            'train_name': segment['train']['name'],
            'ar_c': segment['train']['type'],
            'ar_n': segment['train']['number'],
            'ar_o': segment['train']['admin'].replace('_', ''),
            'dp_c': segment['train']['type'],
            'dp_n': segment['train']['number'],
            'dp_o': segment['train']['admin'].replace('_', ''),
            'walk': 0
        }
        try:
            parsed_segment['dp_station'] = streckennetz.get_name(
                eva=int(segment['segmentStart']['id'])
            )
        except KeyError:
            parsed_segment['dp_station'] = ''
        try:
            parsed_segment['ar_station'] = streckennetz.get_name(
                eva=int(segment['segmentDestination']['id'])
            )
        except KeyError:
            parsed_segment['ar_station'] = ''
        try:
            parsed_segment['train_destination'] = segment['finalDestination'] \
                if 'finalDestination' in segment \
                else segment['segmentDestination']['title']
        except KeyError:
            parsed_segment['train_destination'] = ''
        
        parsed_segment['full_trip'], parsed_segment['stay_times'] = get_trip_of_train(segment['jid'])
        try:
            parsed_segment['dp_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['dp_station'])
        except ValueError:
            try:
                parsed_segment['dp_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['dp_station_display_name'])
            except ValueError:
                # Sometimes, none of the stations is in the trip and we have no clue why
                print('Did not find station in trip')
                print('trip', parsed_segment['full_trip'])
                print('station', parsed_segment['dp_station_display_name'])
                parsed_segment['dp_stop_id'] = -1
        try:
            parsed_segment['ar_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['ar_station'])
        except ValueError:
            parsed_segment['ar_stop_id'] = parsed_segment['full_trip'].index(parsed_segment['ar_station_display_name'])
        parsed_segment['duration'] = str(parsed_segment['ar_ct'] - parsed_segment['dp_ct'])[:-3]
        segments.append(parsed_segment)

    # Add transfer times
    for segment in range(len(segments) - 1):
        if segments[segment + 1]['dp_ct'] < segments[segment]['ar_ct']:
            segments[segment]['transfer_time'] = -(
                (segments[segment + 1]['ar_ct']
                - segments[segment]['dp_ct']).seconds
                // 60
            )
        else:
            segments[segment]['transfer_time'] = (
                (segments[segment + 1]['dp_ct']
                - segments[segment]['ar_ct']).seconds
                // 60
            )
    return {'summary': summary, 'segments': segments}


def parse_connections(connections):
    with ThreadPoolExecutor(max_workers=10) as executor:
        parsed = list(executor.map(parse_connection, connections['routes']))

    # add unique id used for rendering in vue
    for i in range(len(parsed)):
        parsed[i]['id'] = i

    return parsed


@lru_cache
def get_trip_of_train(jid):
    trip = client.trip(jid)
    waypoints = [stopover.stop.name for stopover in trip.stopovers]
    stay_times = [
        (stopover.departure - stopover.arrival).seconds // 60
        if stopover.departure is not None and stopover.arrival is not None
        else None
        for stopover
        in trip.stopovers
    ]
    return waypoints, stay_times
