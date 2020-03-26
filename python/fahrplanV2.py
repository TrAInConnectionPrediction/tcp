import pandas as pd
import requests
import re
import os.path
from datetime import datetime
import clipboard
from bs4 import BeautifulSoup
from io import StringIO
from progress.bar import Bar
from missing_stations import *
import json
from pytz import timezone



#this function extracts the train type (e.g. ICE 1, ICE T, IC, ...) from the zugfinder.de html
def get_train_type_from_html(html):
    soup = BeautifulSoup(html, 'lxml')
    train_type = soup.find("div", {"class": "zugbild", 'content': True})['content']
    train_type = train_type.replace('.jpg','')
    train_type = train_type.replace('https://www.zugfinder.de/images/','')
    return train_type

def add_timetable_to_train_data(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs
    path = trainData_basepath + zugname + '.csv'
    path2 = fahrplan_basepath + zugname + '.csv'
    #remove_false_stations(zugname)
    try:
        fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    except:
        print('the timetable for ' + zugname + ' could not be opened')
        return
    try:
        zug_daten = pd.read_csv(path, sep=',', index_col=False)
    except:
        print('the zugdaten for ' + zugname + ' could not be opened')
        return

    for _index, station in fahrplan.iterrows():
        this_station = zug_daten['bhf'] == station['bhf']
        zug_daten.loc[this_station, 'arr'] = station['arr']
        zug_daten.loc[this_station, 'dep'] = station['dep']
        zug_daten.loc[this_station, 'type'] = station['type']
        zug_daten.loc[this_station, 'time_since_last_station'] = station['time_since_last_station']
        zug_daten.loc[this_station, 'time_since_first_station'] = station['time_since_first_station']
        zug_daten.loc[this_station, 'stay_time'] = station['stay_time']
        zug_daten.loc[this_station, 'station_number'] = station['station_number']
        zug_daten.loc[this_station, 'lon'] = station['lon']
        zug_daten.loc[this_station, 'lat'] = station['lat']

        zug_daten.loc[this_station, 'start_lat'] =  station['start_lat']
        zug_daten.loc[this_station, 'start_lon'] = station['start_lon']
        zug_daten.loc[this_station, 'destination_lat'] = station['destination_lat']
        zug_daten.loc[this_station, 'destination_lon'] = station['destination_lon']
        
        zug_daten.loc[this_station, 'total_time'] = station['total_time']
        zug_daten.loc[this_station, 'delta_lon'] = station['delta_lon']
        zug_daten.loc[this_station, 'delta_lat'] = station['delta_lat']

        #zug_daten.loc[this_station, 'track_length'] = station['track_length']
        #zug_daten.loc[this_station, 'track_length_since_start'] = station['track_length_since_start']
        #zug_daten.loc[this_station, 'total_lenth'] = station['total_lenth']
    zug_daten.to_csv('data/data2019/' + zugname + '.csv', index=False)

def get_json(url):
    """ 
    Preform a GET-Request and convert it into a JSON-Object/dict.
    This function executes the GET-Request 3 times.
    Args:
        url (string): URL for the Requeset
    
    Returns:
        dict/json: Response convertet to a JSON-Object

    Raises:
        ValueError: When the Request doesen't succseed.
    """
    for _1 in range(3):
        try:
            resp = requests.get(url)
            if (resp.status_code != 200 or resp.text == '[]' or resp.text.startswith('{"customError":true')):
                raise ValueError('Something went wrong while doing requests.get(' + url + ') status code: ' + str(resp.status_code))
            return json.loads(resp.text)
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue


def fromUnix(unix):
    """
    Convert from unix timestamp to GMT+1
    Removes millisecons (/1000) add one hour (+3600) and set timezone

    Args:
        unix (int): UNIX timestamp with milliseconds

    Returns:
        datetime: a datetime object with GMT+1 Timezone set
    """

    return datetime.utcfromtimestamp(float(unix)/1000 + 3600).replace(tzinfo=timezone('Europe/Berlin'))

def toUnix(str, format):
    """
    Convert from a Datestring unsing the format to a unix timestamp in milliseconds
    !!! Im not setting any timezone

    Args:
        str (str): Datestring

    Returns:
        int: a integer with the unix timestamp in milliseconds
    """
    return round(datetime.strptime(str,format).timestamp() * 1000)


def get_timetable_from_marudor(zugname, replace=False, old=False):
    zugname = zugname.replace('.csv','')
    path = trainData_basepath + zugname + '.csv'
    path2 = fahrplan_basepath + zugname + '.csv'
    if (replace or not os.path.isfile(path2)):
        if(old):
            resp = get_json("https://marudor.de/api/hafas/v1/details/" + zugname.replace('_','') + "?date=" + str(toUnix("2019-11-11","%Y-%m-%d")))
        else:   
            resp = get_json("https://marudor.de/api/hafas/v1/details/" + zugname.replace('_',''))
        #row = pd.DataFrame(columns=['arr','dep','lat','lon','operator','stop_id','bhf','name','type','station_number','time_since_last_station','time_since_first_station','stay_time'])
        fahrplan = pd.DataFrame(columns=['arr','dep','lat','lon','operator','stop_id','bhf','name','type','station_number','time_since_last_station','time_since_first_station','stay_time','start_lat','start_lon','destination_lat','destination_lon','total_time','delta_lon','delta_lat'])
        i = 0
        for station in resp['stops']:
            #arr
            
            try:
                fahrplan.at[i, 'arr'] = fromUnix(station['arrival']['scheduledTime']).strftime("%H:%M")
            except KeyError:
                if 'departure' in station:
                    fahrplan.at[i, 'arr'] = fromUnix(station['departure']['scheduledTime']).strftime("%H:%M")
            try:
                fahrplan.at[i, 'dep'] = fromUnix(station['departure']['scheduledTime']).strftime("%H:%M")
            except KeyError:
                if 'arrival' in station:
                    fahrplan.at[i, 'dep'] = fromUnix(station['arrival']['scheduledTime']).strftime("%H:%M")
            fahrplan.at[i, 'lon'] = station['station']['coordinates']['lng']
            fahrplan.at[i, 'lat'] = station['station']['coordinates']['lat']
            fahrplan.at[i, 'operator'] = "DB" #Why do we even have this
            fahrplan.at[i, 'stop_id'] = station['station']['id']
            fahrplan.at[i, 'bhf'] = station['station']['title']
            fahrplan.at[i, 'name'] = resp['train']['name'].replace(' ', '_')
            fahrplan.at[i, 'type'] = resp['train']['type']
            fahrplan.at[i, 'station_number'] = i
            try:
                fahrplan.at[i, 'time_since_last_station'] = ((fromUnix(station['arrival']['scheduledTime']) - fromUnix(resp['stops'][i-1]['departure']['scheduledTime'])).seconds//60)%60 #we just want minutes
            except KeyError:
                fahrplan.at[i, 'time_since_last_station'] = 0     
            try:
               fahrplan.at[i, 'time_since_first_station'] = ((fromUnix(station['arrival']['scheduledTime']) - fromUnix(resp['departure']['scheduledTime'])).seconds//60)%60 #we just want minutes 
            except KeyError:
                fahrplan.at[i, 'time_since_first_station'] = 0
            try:
                fahrplan.at[i, 'stay_time'] = ((fromUnix(station['departure']['scheduledTime']) - fromUnix(station['arrival']['scheduledTime'])).seconds//60)%60 #we just want minutes
            except KeyError:
                fahrplan.at[i, 'stay_time'] = 0
            
            
            fahrplan.at[i, 'start_lat'] = resp['stops'][0]['station']['coordinates']['lat']
            fahrplan.at[i, 'start_lon'] = resp['stops'][0]['station']['coordinates']['lng']
            fahrplan.at[i, 'destination_lat'] = resp['stops'][len(resp['stops'])-1]['station']['coordinates']['lat']
            fahrplan.at[i, 'destination_lon'] = resp['stops'][len(resp['stops'])-1]['station']['coordinates']['lng']
            fahrplan.at[i, 'delta_lat'] = round(fahrplan.at[i, 'destination_lat'] - fahrplan.at[i, 'start_lat'], 6)
            fahrplan.at[i, 'delta_lon'] = round(fahrplan.at[i, 'destination_lon'] - fahrplan.at[i, 'start_lon'], 6)
            try:
                fahrplan.at[i, 'total_time'] = resp['duration']/60000 #/1000 (millisecunden) / 60 (seconds) = minutes
            except KeyError:
                return False 

            i+=1


        fahrplan.to_csv(path2, index=False)

streckendaten_path = 'data/streckendaten/abstand_letzter_bahnhof.csv'
trainData_basepath = 'data/trainData2019/'
fahrplan_basepath = 'data/streckendaten2019/'

if __name__ == '__main__':
        
    all_files = os.listdir(trainData_basepath)
    all_files.remove('.DS_Store')
    hh=0
    unknown = []
    print(all_files[658])

    bar = Bar('processing', max=len(all_files))
    for filename in all_files:
        #print(filename)
        try:
            #get_timetable_from_marudor(filename, replace=True, old=True)
            add_timetable_to_train_data(filename)
        except ValueError:
            unknown.append(filename)
            #print(unknown)
        bar.next()
    print(unknown)
