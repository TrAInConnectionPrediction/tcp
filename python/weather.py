import requests
import pandas as pd
import re
import numpy as np
from missing_stations import remove_false_stations
from progress.bar import Bar
from datetime import datetime
import os

# import cProfile, pstats, io

# def profile(fnc):
    
#     """A decorator that uses cProfile to profile a function"""
    
#     def inner(*args, **kwargs):
        
#         pr = cProfile.Profile()
#         pr.enable()
#         retval = fnc(*args, **kwargs)
#         pr.disable()
#         s = io.StringIO()
#         sortby = 'cumulative'
#         ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#         ps.print_stats()
#         print(s.getvalue())
#         return retval

#     return inner

dummy = pd.DataFrame(data={'date': [''],'temperature_c': [''],'air_pressure_hpa': [''],'relative_humidity': [''],'dew_point_c': [''],'wind_speed_kmh': [''],'location': [''],'weather_condition': ['']})

def replace_img(html, url):
    ####-replace-gifs-with-text-to-get-more-information-#### (there are each 14 gifs for day and night)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n1.gif\' class=imageleer alt=\'Wetterzustand: klar\' title=\'Wetterzustand: klar\' ></nobr>','klar', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n2.gif\' class=imageleer alt=\'Wetterzustand: heiter\' title=\'Wetterzustand: heiter\' ></nobr>','heiter', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n3.gif\' class=imageleer alt=\'Wetterzustand: wolkig\' title=\'Wetterzustand: wolkig\' ></nobr>','wolkig', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n4.gif\' class=imageleer alt=\'Wetterzustand: stark bewölkt\' title=\'Wetterzustand: stark bewölkt\' ></nobr>','stark_bewölkt', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n5.gif\' class=imageleer alt=\'Wetterzustand: bedeckt\' title=\'Wetterzustand: bedeckt\' ></nobr>','bedeckt', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n6.gif\' class=imageleer alt=\'Wetterzustand: Regenschauer\' title=\'Wetterzustand: Regenschauer\' ></nobr>','regenschauer', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n7.gif\' class=imageleer alt=\'Wetterzustand: Regen\' title=\'Wetterzustand: Regen\' ></nobr>','regen', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n8.gif\' class=imageleer alt=\'Wetterzustand: unterschiedlich bewölkt, vereinzelt Schauer und Gewitter\' title=\'Wetterzustand: unterschiedlich bewölkt, vereinzelt Schauer und Gewitter\' ></nobr>','gewitter', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n9.gif\' class=imageleer alt=\'Wetterzustand: Schneeschauer\' title=\'Wetterzustand: Schneeschauer\' ></nobr>','schneeschauer', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n10.gif\' class=imageleer alt=\'Wetterzustand: Schneefall\' title=\'Wetterzustand: Schneefall\' ></nobr>','schneefall', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n11.gif\' class=imageleer alt=\'Wetterzustand: Schneeregen\' title=\'Wetterzustand: Schneeregen\' ></nobr>','schneeregen', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n12.gif\' class=imageleer alt=\'Wetterzustand: Nebel\' title=\'Wetterzustand: Nebel\' ></nobr>','nebel', html)
    #html = re.sub('','')
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/n14.gif\' class=imageleer alt=\'Wetterzustand: Sprühregen\' title=\'Wetterzustand: Sprühregen\' ></nobr>','sprühregen', html)
    
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t1.gif\' class=imageleer alt=\'Wetterzustand: sonnig\' title=\'Wetterzustand: sonnig\' ></nobr>','sonnig', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t2.gif\' class=imageleer alt=\'Wetterzustand: heiter\' title=\'Wetterzustand: heiter\' ></nobr>','heiter', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t3.gif\' class=imageleer alt=\'Wetterzustand: wolkig\' title=\'Wetterzustand: wolkig\' ></nobr>','wolkig', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t4.gif\' class=imageleer alt=\'Wetterzustand: stark bewölkt\' title=\'Wetterzustand: stark bewölkt\' ></nobr>','stark_bewölkt', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t5.gif\' class=imageleer alt=\'Wetterzustand: bedeckt\' title=\'Wetterzustand: bedeckt\' ></nobr>','bedeckt', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t6.gif\' class=imageleer alt=\'Wetterzustand: Regenschauer\' title=\'Wetterzustand: Regenschauer\' ></nobr>','regenschauer', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t7.gif\' class=imageleer alt=\'Wetterzustand: Regen\' title=\'Wetterzustand: Regen\' ></nobr>','regen', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t8.gif\' class=imageleer alt=\'Wetterzustand: unterschiedlich bewölkt, vereinzelt Schauer und Gewitter\' title=\'Wetterzustand: unterschiedlich bewölkt, vereinzelt Schauer und Gewitter\' ></nobr>','gewitter', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t9.gif\' class=imageleer alt=\'Wetterzustand: Schneeschauer\' title=\'Wetterzustand: Schneeschauer\' ></nobr>','schneeschauer', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t10.gif\' class=imageleer alt=\'Wetterzustand: Schneefall\' title=\'Wetterzustand: Schneefall\' ></nobr>','schneefall', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t11.gif\' class=imageleer alt=\'Wetterzustand: Schneeregen\' title=\'Wetterzustand: Schneeregen\' ></nobr>','schneeregen', html)
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t12.gif\' class=imageleer alt=\'Wetterzustand: Nebel\' title=\'Wetterzustand: Nebel\' ></nobr>','nebel', html)
    #html = re.sub('','')
    html = re.sub('<nobr><img src=\'https://www.proplanta.de/wetterdaten/images/symbole/t14.gif\' class=imageleer alt=\'Wetterzustand: Sprühregen\' title=\'Wetterzustand: Sprühregen\' ></nobr>','sprühregen', html)

    #there are two gifs we know the link of but not the name. If one of those should ever come, this will trigger
    if('https://www.proplanta.de/wetterdaten/images/symbole/t13.gif' in html or 'https://www.proplanta.de/wetterdaten/images/symbole/n13.gif' in html):
        print('unknow gif found in html: ' + url)
    return html
    ####------------------------------------------------####

def get_html(url):
    for _1 in range(3):
        try:
            resp = requests.get(url)
            if (resp.status_code != 200 or resp.text == '[]'):
                raise ValueError('Something went wrong while doing requests.get(' + url + ') status code: ' + str(resp.status_code))
            return resp.text
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue
        except ValueError:
            print('retrying...')
            continue
    else:
        raise ValueError('Something went wrong while doing requests.get(' + url + ') status code: ' + str(resp.status_code))

def get_weather(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs

    #cur_path = os.path.dirname(__file__)
    path = 'data/trainData2019/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'
    weather_path = 'data/wetterdaten/weather.csv'
    bahnhof_to_weather_location_path = 'data/wetterdaten/bahnhof_to_weather_location.csv'

    all_weather = pd.read_csv(weather_path, sep=",", index_col=False, engine='c')

    all_weather = all_weather.drop_duplicates().reset_index(drop=True)
    all_weather = all_weather.sort_values(by=['location', 'date'])

    bahnhof_to_weather_location = pd.read_csv(bahnhof_to_weather_location_path, sep=",", index_col=False, engine='c')
    bahnhof_to_weather_location = bahnhof_to_weather_location.drop_duplicates().reset_index(drop=True)

    remove_false_stations(zugname)

    try:
        fahrplan = pd.read_csv(path2, sep=',', index_col=False, engine='c',encoding='utf-8')
    except:
        print('the timetable for ' + zugname + ' could not be opened')
        return
    try:
        zug_daten = pd.read_csv(path, sep=',', index_col=False, engine='c')
    except pd.errors.EmptyDataError:
        print('No train data for ' + zugname)
        return 0
    bar = Bar('processing', max=len(zug_daten['date'])/100)

    zug_daten['temperature_c'] = ''
    zug_daten['air_pressure_hpa'] = ''
    zug_daten['relative_humidity'] = ''
    zug_daten['dew_point_c'] = ''
    zug_daten['wind_speed_kmh'] = ''
    zug_daten['weather_condition'] = ''

    for index, station in fahrplan.iterrows():
        bhf = station['bhf']
        station_zug_daten = zug_daten[zug_daten['bhf'] == bhf]
        #In timetables from the db, border crosses are marked like stations. This if statemant avoids those
        if(station_zug_daten.empty):
            continue

        #get location (location != bhf)
        if (len(bahnhof_to_weather_location[bahnhof_to_weather_location['bhf'].isin([bhf])])):
            location = bahnhof_to_weather_location.loc[bahnhof_to_weather_location['bhf'].isin([bhf]), 'location'].reset_index(drop=True)[0]
        else:
            location = input("location for: " + bhf + ": ")
            bahnhof_to_weather_location.at[len(bahnhof_to_weather_location['bhf']), 'bhf'] = bhf
            bahnhof_to_weather_location.at[len(bahnhof_to_weather_location['bhf']) - 1, 'location'] = location
            bahnhof_to_weather_location.to_csv(weather_path, index=False)
        if(location == 'no-data'):#currently, we can only acces weather from Germany. If a location is outside of Germany, we mark it as 'no-data'. In futher versions, we may include weather from Austria, Switzerland and France.
            #print('no-data')
            continue
        
        station_weather = all_weather[all_weather['location'] == location]
        
        for i, x in station_zug_daten.iterrows():
            date = datetime.strptime(station_zug_daten.at[i,'date'], '%Y-%m-%d')

            #check if the data is already in our database
            data = station_weather[station_weather['date'].astype('datetime64[D]').isin([date])]
            data = data[data['location'].isin([location])]

            if(data.empty):
                #download the data, if it's not in our database
                date2 = station_zug_daten['date'].astype('datetime64[D]').loc[i].strftime('%d-%m-%Y')
                url = 'https://www.proplanta.de/Agrar-Wetter/' + location + '_Rueckblick_' + date2 + '_AgrarWetter.html'
                html = get_html(url)
                #replace links to status images with text to store it
                html = replace_img(html, url)

                ####-Convet-html-page-to-tables-####
                weather_list = pd.read_html(html)
                if len(weather_list) <= 36:
                    print("\n---------------no data for " + date2 + " in " + location + ' (' + zugname + ")---------------")
                    weather = dummy
                    weather['date'] = date.strftime('%Y-%m-%d %H:%M:%S')
                    weather['location'] = location
                    all_weather = pd.concat([all_weather, weather], ignore_index=True, sort=False)
                    station_weather = pd.concat([station_weather, weather], ignore_index=True, sort=False)
                    continue
                
                for top in range(len(weather_list)-1,32, -1):
                    #print("top: " + str(top) +" len: " + str(len(weather_list[top].index)))
                    if len(weather_list[top].index) == 22:
                        weather_list = weather_list[34:(top+1)]
                        #print("range 33:" + str(top) + "\n\n")
                        break
                else:
                    print("looks like something went wrong")
                a = 0
                for this_weather in weather_list:
                    weather_list[a] = this_weather.set_index(0).drop([1], axis=1).dropna().transpose()
                    if (weather_list[a].empty):
                        #print('one element of list is empty for ' + url)
                        del weather_list[a]
                    else:
                        a += 1
                if not weather_list:
                    print('the weather list is empty for ' + url)
                    continue
                weather = pd.concat(weather_list, ignore_index=True, sort=False)
                ####---------------------------####

                #Add date and time togehter so that we have an official datetime format
                weather['Datum'] = pd.to_datetime(weather['Datum']+' '+weather['Uhrzeit'], format='%d.%m.%Y %H.%M Uhr').dt.strftime('%Y-%m-%d %H:%M:%S')
                #Delete unneded columns
                weather = weather.drop(columns=['Höhe derWolkenuntergrenze', 'Uhrzeit', 'Sichtweite'], axis=1)

                #rename columns to be in english and add unit. Then remove the units from the data
                weather = weather.rename(columns={"Datum": "date", "Temperatur": "temperature_c", "Luftdruck": "air_pressure_hpa", "relative Feuchte":"relative_humidity", "Taupunkt":"dew_point_c", "Windgeschwindigkeit":"wind_speed_kmh", "Wetterzustand":"weather_condition"})
                weather = weather.replace({',':'.', ' °C':'', ' %':'', ' km/h':'', 'km':'', ' hPa':'', 'über':'', '>':'', 'Keine Angabe':'', ', ':''}, regex=True)
                weather['location'] = location
                all_weather = pd.concat([all_weather, weather], ignore_index=True, sort=False)
                station_weather = pd.concat([station_weather, weather], ignore_index=True, sort=False)

                data = station_weather[station_weather['date'].astype('datetime64[D]').isin([date])]
                data = data[data['location'].isin([location])]

            ####-Add-weather-to-trainData-####
            x = data['date'].astype('datetime64[h]')

            # 1. nächste Uhrzeit finden
            arr_time = station_zug_daten.at[i, 'arr'] if station_zug_daten.at[i, 'arr'] != '99:99' else station_zug_daten.at[i, 'dep']
            try: #sometimes there are times like '14:99'. We are skipping those.
                arr_time = pd.to_datetime(date.strftime('%Y-%m-%d') + 'T' + arr_time)
            except:
                continue
            x = x.reset_index(drop=True)
            #Calculate the timedelta amount
            v = x[:]
            v = v.apply(lambda dep_time: dep_time - arr_time if dep_time > arr_time else arr_time - dep_time)

            #find nearest time
            date = x[v.idxmin()].strftime('%Y-%m-%d %H:%M:%S')
            
            #all data with matching date and time
            data =  data.loc[data['date'].isin([date]), :] #.astype('datetime64[ns]')
            data = data.reset_index(drop=True)
            # 2. daten eintragen
            station_zug_daten.at[i, 'temperature_c'] = data.at[0, 'temperature_c']
            station_zug_daten.at[i, 'air_pressure_hpa'] = data.at[0, 'air_pressure_hpa']
            station_zug_daten.at[i, 'relative_humidity'] = data.at[0, 'relative_humidity']
            station_zug_daten.at[i, 'dew_point_c'] = data.at[0, 'dew_point_c']
            station_zug_daten.at[i, 'wind_speed_kmh'] = data.at[0, 'wind_speed_kmh']
            station_zug_daten.at[i, 'weather_condition'] = data.at[0, 'weather_condition']
            
            ####--------------------------####

            #the progress bar would slow down the program when it would update each time
            if ((i % 100) == 0):
                bar.next()
        zug_daten[zug_daten['bhf'] == bhf] = station_zug_daten

    bar.finish()
    zug_daten.to_csv(path, index=False)
    all_weather.to_csv(weather_path, index=False)



all_files = os.listdir("data/trainData2019/")
all_files.remove('.DS_Store')
hh=0
for filename in all_files:
    print(filename, hh)
    get_weather(filename)
    hh += 1