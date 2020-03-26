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

streckendaten_path = 'data/streckendaten/abstand_letzter_bahnhof.csv'
trainData_basepath = 'data/trainData2019/'
fahrplan_basepath = 'data/streckendaten2019/'

#this function is used to get html from zugfinder.de for a specific train
def get_html(zugname):
    payload = {'zugnr':zugname}
    jar = requests.cookies.RequestsCookieJar()
    # TODO add config.py data
    jar.set('cook', '', domain='www.zugfinder.de', path='/')
    url = 'https://www.zugfinder.de/zuginfo.php'
    for _1 in range(3):
        try:
            r = requests.get(url, cookies=jar, params=payload)
            if (r.status_code != 200 or r.text == '[]'):
                raise ValueError('Something went wrong while doing requests.get(' + r.url + ') status code: ' + str(r.status_code))
            return r.text
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue

#this function extracts the train type (e.g. ICE 1, ICE T, IC, ...) from the zugfinder.de html
def get_train_type_from_html(html):
    soup = BeautifulSoup(html, 'lxml')
    train_type = soup.find("div", {"class": "zugbild", 'content': True})['content']
    train_type = train_type.replace('.jpg','')
    train_type = train_type.replace('https://www.zugfinder.de/images/','')
    return train_type

#this  function calculates a timedelta in minutes between to daytimes
def timedelta(time_now, time_last):
    time_now = datetime.strptime(time_now, '%H:%M')
    time_last = datetime.strptime(time_last, '%H:%M')
    time_since_last = time_now - time_last
    return time_since_last.seconds / 60

#this header contains the login information to the api
# TODO add config.py data
headers = {
}

#this function is used to get a dataframe from the fahrplan api (https://developer.deutschebahn.com/store/apis/info?name=Fahrplan&version=v1&provider=DBOpenData) while doing basic error things. If the connection breaks, it will retry 2 more times.
def get_df_from_url(url):
    for _1 in range(3):
        try:
            resp = requests.get(url, headers=headers)
            if (resp.status_code != 200 or resp.text == '[]'):
                raise ValueError('Something went wrong while doing requests.get(' + url + ') status code: ' + str(resp.status_code))
            return pd.read_json(resp.text)
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue

#this function is used to add timedeltas between stations to the timetable
def add_time_since_last_station(fahrplan):
    #this is used to calculate the timedelta to the first station
    first_dep = fahrplan.loc[0, 'dep']

    for i in range(len(fahrplan)):
        arr = fahrplan.at[i, 'arr']
        dep = fahrplan.at[i, 'dep']
        if (i == 0): #first location = no time since last one
            fahrplan.at[i, 'time_since_last_station'] = 0
            fahrplan.at[i, 'time_since_first_station'] = 0
        else:
            arr_before = fahrplan.at[i - 1, 'arr']
            dep_before = fahrplan.at[i - 1, 'dep']

            if (pd.isna(arr)):
                fahrplan.at[i, 'time_since_last_station'] = timedelta(dep, dep_before) if pd.notna(dep_before) else timedelta(dep, arr_before)
                fahrplan.at[i, 'stay_time'] = 0
            elif (pd.isna(dep)):
                fahrplan.at[i, 'time_since_last_station'] = timedelta(arr, dep_before) if pd.notna(dep_before) else timedelta(arr, arr_before)
                fahrplan.at[i, 'stay_time'] = 0
            else:
                fahrplan.at[i, 'time_since_last_station'] = timedelta(arr, dep_before) if pd.notna(dep_before) else timedelta(arr, arr_before)
                fahrplan.at[i, 'stay_time'] = timedelta(dep, arr)
            fahrplan.at[i, 'time_since_first_station'] = timedelta(arr, first_dep) if pd.notna(arr) else timedelta(dep, first_dep)
    return fahrplan

#this function helps to add the distances between stations using PFFEB.exe (https://www.evg-online.org/deine-vorteile/services/service-meldungen/entfernungsrechner-fuer-fahrverguenstigungen/)
def add_track_information_to_timetable(fahrplan):
    # zugname = zugname.replace('.csv','')#to make it compatiple with more inputs
    streckendaten = pd.read_csv(streckendaten_path, sep=",", index_col=False)
    # path2 = fahrplan_basepath + zugname + '.csv'
    # fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    fahrplan.at[0, 'track_length'] = 0
    for i in range(len(fahrplan['bhf']) - 1):
        bhf1 = fahrplan.at[i, 'bhf']
        bhf2 = fahrplan.at[(i + 1), 'bhf']
        if (bhf1 == bhf2):
            fahrplan.loc[i+1, 'track_length'] = fahrplan.loc[i, 'track_length']
            continue

        thing = streckendaten[streckendaten['bhf1'].isin([bhf1]) | streckendaten['bhf2'].isin([bhf1])]
        thing = thing[thing['bhf1'].isin([bhf2]) | thing['bhf2'].isin([bhf2])]

        if (len(thing['track_length']) == 1):
            fahrplan.loc[i+1, 'track_length'] = thing.reset_index().loc[0, 'track_length']
        else:
            clipboard.copy(bhf1)
            input("Streckenlänge von " + bhf1)
            clipboard.copy(bhf2)
            strecke = input(" bis " + bhf2 + ": ")

            print("\nEingaben: ", "\nBahnhof1: ", bhf1, "\nBahnhof2: ", bhf2, "\nStreckenlänge: ", strecke)
            fahrplan.loc[i+1, 'track_length'] = strecke

            neu = pd.DataFrame([[bhf1, bhf2, strecke]], columns=['bhf1', 'bhf2', 'track_length'])
            streckendaten = streckendaten.append(neu, ignore_index=True, sort=False)
            #print(streckendaten)
            streckendaten.to_csv(streckendaten_path, index=False) 

    #Add track_legth_since_start
    #fahrplan.to_csv(path2, index=False)
    #fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    for i in range(len(fahrplan['bhf'])):
        fahrplan.loc[i, "track_length_since_start"] = fahrplan.loc[:i, 'track_length'].sum()

    return fahrplan
    #save data
    #fahrplan.to_csv(path2, index=False)
    #streckendaten.to_csv(streckendaten_path, index=False)

def add_general_train_info(fahrplan):
    start_lat = fahrplan.at[0, 'lat']
    start_lon = fahrplan.at[0, 'lon']

    #len(fahrplan)-1 selects the last row. There's no "nice way" to do this in pandas
    destination_lat = fahrplan.at[len(fahrplan)-1, 'lat']
    destination_lon = fahrplan.at[len(fahrplan)-1, 'lon']
    total_lenth = fahrplan.at[len(fahrplan)-1, 'track_length_since_start']
    total_time = fahrplan.at[len(fahrplan)-1, 'time_since_first_station']

    delta_lon = start_lon - destination_lon
    delta_lat = start_lat - destination_lat

    for i in range(len(fahrplan)):
        #fill those na's that are present by desing
        if pd.isna(fahrplan.at[i, 'arr']):
            fahrplan.at[i, 'arr'] = fahrplan.at[i, 'dep']
        if pd.isna(fahrplan.at[i, 'dep']):
            fahrplan.at[i, 'dep'] = fahrplan.at[i, 'arr']
        if pd.isna(fahrplan.at[i, 'stay_time']):
            fahrplan.at[i, 'stay_time'] = 0

        fahrplan.at[i, 'start_lat'] = start_lat
        fahrplan.at[i, 'start_lon'] = start_lon
        fahrplan.at[i, 'destination_lat'] = destination_lat
        fahrplan.at[i, 'destination_lon'] = destination_lon
        fahrplan.at[i, 'total_lenth'] = total_lenth
        fahrplan.at[i, 'total_time'] = total_time
        fahrplan.at[i, 'delta_lon'] = delta_lon
        fahrplan.at[i, 'delta_lat'] = delta_lat

    return fahrplan


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
        zug_daten.loc[this_station, 'track_length'] = station['track_length']
        zug_daten.loc[this_station, 'track_length_since_start'] = station['track_length_since_start']
        zug_daten.loc[this_station, 'station_number'] = station['station_number']
        zug_daten.loc[this_station, 'lon'] = station['lon']
        zug_daten.loc[this_station, 'lat'] = station['lat']

        zug_daten.loc[this_station, 'start_lat'] =  station['start_lat']
        zug_daten.loc[this_station, 'start_lon'] = station['start_lon']
        zug_daten.loc[this_station, 'destination_lat'] = station['destination_lat']
        zug_daten.loc[this_station, 'destination_lon'] = station['destination_lon']
        zug_daten.loc[this_station, 'total_lenth'] = station['total_lenth']
        zug_daten.loc[this_station, 'total_time'] = station['total_time']
        zug_daten.loc[this_station, 'delta_lon'] = station['delta_lon']
        zug_daten.loc[this_station, 'delta_lat'] = station['delta_lat']
    zug_daten.to_csv(path, index=False)

#1. Bahnhofsid rausfinden https://api.deutschebahn.com/freeplan/v1/location/K%C3%B6ln%20Hbf
#2. Zugid bei jedem rausfinden https://api.deutschebahn.com/freeplan/v1/departureBoard/8000266?date=2019-04-01T10%3A43
#3. Alle informationen zeitlich ordnen und zusammenbauen.

#this function is used to get an json file from an api while doing basic error things. If the connection breaks, it will retry 2 more times.
def get_json(url):
    for _1 in range(3):
        try:
            resp = requests.get(url, headers=headers)
            if (resp.status_code != 200 or resp.text == '[]'):
                raise ValueError('Something went wrong while doing requests.get(' + url + ') status code: ' + str(resp.status_code))
            return resp.text
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue

#this function is used to get the location_id from the api or from the buffer
def get_location_id(location_name):
    #this has to be used due to the badly desinged api that can't handle '()' correctly
    special_station_names = pd.read_csv('data/streckendaten/special_station_names.csv', sep=",", index_col=False)

    #this is used to buffer the station ids in order to speed up the programm
    buffer = pd.read_csv('data/streckendaten/station_ids.csv', sep=",", index_col=False)

    ids = buffer.loc[buffer['name'].isin([location_name])].reset_index(drop=True)
    if (ids.empty): #if the "buffer" does not contain the id, then get it from the api
        try:
            url = 'https://api.deutschebahn.com/fahrplan-plus/v1/location/' + location_name
            locations = get_df_from_url(url)

            locations = locations.loc[locations['name'].isin([location_name])].reset_index(drop=True)
            if (locations.empty):
                raise ValueError('There is no location id', location_name)
            location_id = locations.loc[0, 'id']
            buffer = buffer.append(locations[['id', 'name']], ignore_index=True, sort=True)
            buffer.to_csv('data/streckendaten/station_ids.csv', index=False)
            return location_id
        except ValueError:
            station_name = special_station_names.loc[special_station_names['name'].isin([location_name])].reset_index(drop=True)
            if (station_name.empty):
                print('please enter the information for ' + location_name)
                search_name = input('search_name: ')
                find_name = input('find_name: ')
                station_name.loc[0, 'name'] = location_name
                station_name.loc[0, 'search_name'] = search_name
                station_name.loc[0, 'find_name'] = find_name
                special_station_names = special_station_names.append(station_name, ignore_index=True, sort=True)
                special_station_names.to_csv('data/streckendaten/special_station_names.csv', index=False)
            try:
                location_name = station_name.loc[0, 'name']
                search_name = station_name.loc[0, 'search_name']
                find_name = station_name.loc[0, 'find_name']

                url = 'https://api.deutschebahn.com/fahrplan-plus/v1/location/' + search_name
                locations = get_df_from_url(url)

                locations = locations.loc[locations['name'].isin([find_name])].reset_index(drop=True)
                if (locations.empty):
                    raise ValueError('There is no location id', location_name)
                locations.loc[0,'name'] = location_name
                location_id = locations.loc[0, 'id']
                buffer = buffer.append(locations[['id', 'name']], ignore_index=True, sort=True)
                buffer.to_csv('data/streckendaten/station_ids.csv', index=False)
                return location_id
            except ValueError:
                print('No information for location ' + location_name)
                return
    else:
        location_id = ids.loc[0, 'id']
        return location_id

#this function is used to get the id of a train to get the timetable later
def get_train_id(location_id, arr, dep, location_name, zugname, date):
    try:
        if (arr == '99:99'):
            raise ValueError('arr is 99:99 ' + zugname + ' in ' + location_name + ' (' + date + ' ' + arr + ' Uhr)')
        url = 'https://api.deutschebahn.com/fahrplan-plus/v1/arrivalBoard/' + str(location_id) + '?date=' + date + 'T' + arr
        arrivals = get_df_from_url(url)
        arrivals = arrivals.loc[arrivals['name'].isin([zugname])].reset_index()
        if (len(arrivals['name']) > 1):
            raise ValueError('There is more than one train', zugname, location_name)
        if (arrivals.empty):
            raise ValueError('There is no train', zugname, location_name)
        return arrivals.loc[0, 'detailsId']
    except ValueError:
        print('No information for ' + zugname + ' in ' + location_name + ' (' + date + ' ' + arr + ' Uhr)')
        if (dep == '99:99'):
            raise ValueError('dep is 99:99 '  + zugname + 'in: ' + location_name + ' (' + date + ' ' + dep + ' Uhr)')
        url = 'https://api.deutschebahn.com/fahrplan-plus/v1/departureBoard/' + str(location_id) + '?date=' + date + 'T' + dep
        departures = get_df_from_url(url)
        departures = departures.loc[departures['name'].isin([zugname])].reset_index(drop=True)
        if (len(departures['name']) > 1):
            raise ValueError('There is more than one train: ' + zugname + 'in: ' + location_name + ' (' + date + ' ' + dep + ' Uhr)')
        if (departures.empty):
            raise ValueError('There is no train', zugname, location_name)
        return departures.loc[0, 'detailsId']
    
    if (dep == '99:99'):
        raise ValueError('dep is 99:99 ' + zugname + 'in: ' + location_name + ' (' + date + ' ' + dep + ' Uhr)')
    url = 'https://api.deutschebahn.com/fahrplan-plus/v1/departureBoard/' + str(location_id) + '?date=' + date + 'T' + dep
    departures = get_df_from_url(url)
    departures = departures.loc[departures['name'].isin([zugname])].reset_index(drop=True)
    if (len(departures['name']) > 1):
        raise ValueError('There is more than one train: ' + zugname + 'in: ' + location_name + ' (' + date + ' ' + dep + ' Uhr)')
    if (departures.empty):
        raise ValueError('There is no train', zugname, location_name)
    return departures.loc[0, 'detailsId']

def get_timetable_from_api(zugname, replace=False):
    zugname = zugname.replace('.csv','')
    path = trainData_basepath + zugname + '.csv'
    path2 = fahrplan_basepath + zugname + '.csv'
    if (replace or not os.path.isfile(path2)): #or input('Do you want to update the timetable of ' + zugname + ' [y/n]') == 'y'
        zug_daten = pd.read_csv(path, index_col=False, engine='c')
        zugname = zugname.replace('_',' ')
        
        #this has to be used due to the badly desinged api that can't handle '()' correctly
        #special_station_names = pd.read_csv('data/streckendaten/special_station_names.csv', sep=",", index_col=False)

        #this is used to buffer the station ids in order to speed up the programm
        #station_ids = pd.read_csv('data/streckendaten/station_ids.csv', sep=",", index_col=False)

        fahrplan = pd.DataFrame()

        for i in range(100):
            location_name = zug_daten.at[i, 'bhf']
            date = zug_daten.at[i, 'date']
            dep = zug_daten.at[i, 'dep']
            arr = zug_daten.at[i, 'arr']

            location_id = get_location_id(location_name)
            try:
                train_id = get_train_id(location_id, arr, dep, location_name, zugname, date)
            except ValueError:
                print('retrying to find a train_id for' + zugname + 'in: ' + location_name + ' (' + date + ')')
                continue
        
            #due to some reason, you must replace '%' with '%25' in the url. I have no clou why.
            train_id = train_id.replace('%', '%25')
            url =  'https://marudor.de/api/hafas/v1/journeyDetails/' + train_id
            #url = 'https://api.deutschebahn.com/fahrplan-plus/v1/journeyDetails/' + train_id
            try:
                fahrplan = get_df_from_url(url)
            except ValueError:
                print('retrying to find a timetable for ' + zugname + ' in: ' + location_name + ' (' + date + ')')
                continue
            break
        fahrplan = fahrplan.drop(['notes'], axis=1)
        fahrplan = fahrplan.replace({'&#x0028;':'(', '&#x0029;':')'}, regex=True)
        fahrplan = fahrplan.rename(columns={'arrTime': 'arr', 'depTime': 'dep', 'stopId': 'stop_id', 'stopName':'bhf', 'train':'name'})
        fahrplan['name'] = fahrplan['name'].replace({' ':'_'}, regex=True)
        fahrplan['station_number'] = fahrplan.index
        fahrplan = add_time_since_last_station(fahrplan)
        try:
            html = get_html(zugname)
            train_type = get_train_type_from_html(html)
        except:
            train_type = input('what kind of train is the ' + fahrplan.loc[0, 'name'] + '?')
        fahrplan['type'] = train_type
    else:
        fahrplan = pd.read_csv(path2, sep=",", index_col=False)
    
    fahrplan = add_track_information_to_timetable(fahrplan)
    fahrplan = add_general_train_info(fahrplan)

    fahrplan.to_csv(path2, index=False)
















def get_station_from_html(html):
    soup = BeautifulSoup(html, 'lxml')
    if (soup.find("span", {"id": "Zwischenhalte"}) == None):
        raise ValueError('No timetable found')
    route = soup.find("span", {"id": "Zwischenhalte"}).string
    route = route.replace('Fahrplanmäßige Route: ', 'bhf\n')
    route = route.replace(' » ', '\n')
    stations = pd.read_csv(StringIO(route), sep=',', index_col=False)
    stations['arr_or_dep'] = 'arr'
    #duplicate the stations in order to have one for arrival and one for departure
    stations_dep = stations.reset_index(drop=True)
    stations_dep['arr_or_dep'] = 'dep'
    stations = stations.append(stations_dep, ignore_index=True, sort=True)
    return stations

def add_time_since_last_station_old(fahrplan):
    stations = fahrplan['bhf']
    time_at_first_station = fahrplan.loc[0, 'time']
    for i in range(len(stations)):
        time_at_station = fahrplan.loc[i, 'time']
        if (i == 0): #first location = no time since last one
            fahrplan.loc[i, 'time_since_last_station'] = 0
            fahrplan.loc[i, 'time_since_first_station'] = 0

        else:
            if (fahrplan.loc[i, 'arr_or_dep'] == 'arr'):
                x = 1
            elif (fahrplan.loc[i, 'arr_or_dep'] == 'dep'):
                x = 1 if (fahrplan.loc[i - 1, 'arr_or_dep'] == 'dep' or i < 2) else 2
            time_at_station = fahrplan.loc[i, 'time']
            time_at_last_station = fahrplan.loc[i - x, 'time']
            time_since_last_station = timedelta(time_at_station, time_at_last_station)
            time_since_first_station = timedelta(time_at_station, time_at_first_station)
            fahrplan.loc[i, 'time_since_last_station'] = time_since_last_station
            fahrplan.loc[i, 'time_since_first_station'] = time_since_first_station
    return fahrplan

def add_track_information_to_timetable_old(zugname):
    zugname = zugname.replace('.csv','')#to make it compatiple with more inputs
    streckendaten = pd.read_csv(streckendaten_path, sep=",", index_col=False)
    path2 = fahrplan_basepath + zugname + '.csv'
    fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    fahrplan.loc[0, 'track_length'] = 0
    for i in range(len(fahrplan['bhf']) - 1):
        bhf1 = fahrplan.loc[i, 'bhf']
        bhf2 = fahrplan.loc[(i + 1), 'bhf']
        if (bhf1 == bhf2):
            fahrplan.loc[i+1, 'track_length'] = fahrplan.loc[i, 'track_length']
            continue

        thing = streckendaten[streckendaten['bhf1'].isin([bhf1]) | streckendaten['bhf2'].isin([bhf1])]
        thing = thing[thing['bhf1'].isin([bhf2]) | thing['bhf2'].isin([bhf2])]

        if (len(thing['track_length']) == 1):
            fahrplan.loc[i+1, 'track_length'] = thing.reset_index().loc[0, 'track_length']
        else:
            clipboard.copy(bhf1)
            input("Streckenlänge von " + bhf1)
            clipboard.copy(bhf2)
            strecke = input(" bis " + bhf2 + ": ")

            print("\nEingaben: ", "\nBahnhof1: ", bhf1, "\nBahnhof2: ", bhf2, "\nStreckenlänge: ", strecke)
            fahrplan.loc[i+1, 'track_length'] = strecke

            neu = pd.DataFrame([[bhf1, bhf2, strecke]], columns=['bhf1', 'bhf2', 'track_length'])
            streckendaten = streckendaten.append(neu, ignore_index=True, sort=False)
            print(streckendaten)
            streckendaten.to_csv(streckendaten_path, index=False) 

    #Add track_legth_since_start
    fahrplan.to_csv(path2, index=False)
    fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    fahrplan_part = fahrplan.drop(['arr_or_dep', 'time', 'time_since_last_station', 'time_since_first_station'], axis=1).drop_duplicates()
    for i in range(len(fahrplan['bhf'])):
        fahrplan.loc[i, "track_legth_since_start"] = fahrplan_part.loc[:i, 'track_length'].sum()

    fahrplan.to_csv(path2, index=False)
    streckendaten.to_csv(streckendaten_path, index=False)

def get_timetable_from_zugfinder(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs
    path = trainData_basepath + zugname + '.csv'
    path2 = fahrplan_basepath + zugname + '.csv'
    try:
        zug_daten = pd.read_csv(path, sep=',', index_col=False)
    except pd.errors.EmptyDataError:
        print('No timetable for ' + zugname)
        return 0

    html = get_html(zugname)
    try:
        fahrplan = get_station_from_html(html)
    except ValueError:
        print('No timetable for ' + zugname)
        return 0
    train_type = get_train_type_from_html(html)
    fahrplan['type'] = train_type
    fahrplan['name'] = zugname
    for i in range(len(fahrplan)):
        try:
            this_station = zug_daten.loc[zug_daten['bhf'].isin([fahrplan.loc[i, 'bhf']])].reset_index(drop=True)
            if (this_station.empty):
                raise ValueError('station not in train data')
            fahrplan.loc[i, 'time'] = '99:99' if this_station.empty else this_station.loc[0,fahrplan.loc[i, 'arr_or_dep']]
        except ValueError:
            print('No timetable for ' + zugname)
            return 0
    fahrplan = fahrplan.replace('99:99', pd.NaT)
    fahrplan = fahrplan.dropna().reset_index(drop=True)
    fahrplan = fahrplan.sort_values(by=['time']).reset_index(drop=True)
    fahrplan = add_time_since_last_station(fahrplan)
    fahrplan.to_csv(path2, index=False)




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

def get_timetable_from_marudor(zugname, replace=False):
    zugname = zugname.replace('.csv','')
    path = trainData_basepath + zugname + '.csv'
    path2 = fahrplan_basepath + zugname + '.csv'
    if (replace or not os.path.isfile(path2)):
        #zug_daten = pd.read_csv(path, index_col=False, engine='c')
            
        resp = get_json("https://marudor.de/api/hafas/v1/details/" + zugname.replace('_',''))
        resp = json.loads(resp)
        #row = pd.DataFrame(columns=['arr','dep','lat','lon','operator','stop_id','bhf','name','type','station_number','time_since_last_station','time_since_first_station','stay_time'])
        fahrplan = pd.DataFrame(columns=['arr','dep','lat','lon','operator','stop_id','bhf','name','type','station_number','time_since_last_station','time_since_first_station','stay_time','start_lat','start_lon','destination_lat','destination_lon','total_time','delta_lon','delta_lat'])
        i = 0
        for station in resp['stops']:
            #arr
            try:
                fahrplan.at[i, 'arr'] = fromUnix(station['arrival']['scheduledTime']).strftime("%H:%M")
            except KeyError:
                fahrplan.at[i, 'arr'] = fromUnix(station['departure']['scheduledTime']).strftime("%H:%M")
            try:
                fahrplan.at[i, 'dep'] = fromUnix(station['departure']['scheduledTime']).strftime("%H:%M")
            except KeyError:
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
            fahrplan.at[i, 'total_time'] = resp['duration']/60000 #/1000 (millisecunden) / 60 (seconds) = minutes 

            i+=1


    fahrplan.to_csv(path2, index=False)

if __name__ == '__main__':
        
    all_files = os.listdir("data/trainData2019/")
    all_files.remove('.DS_Store')
    hh=0
    unknown = []
    bar = Bar('processing', max=len(all_files))
    for filename in all_files:
        try:
            get_timetable_from_marudor(filename, replace=True)
        except:
            unknown.append(filename)
            print(unknown)
        bar.next()
