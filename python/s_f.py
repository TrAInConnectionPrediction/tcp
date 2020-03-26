import pandas as pd
import os
import re
import numpy as np
from datetime import datetime, timedelta
from os.path import basename
from progress.bar import Bar
import glob
import matplotlib.pyplot as plt

def list_of_dir(dir, save_path):
    files = os.listdir(dir)
    files = pd.DataFrame(files)
    files.to_csv(save_path, index=False, header=False)

def create_stats():
    path = 'data/combinedData/na_droped_trains.csv'
    data = pd.read_csv(path, index_col=False, compression='zip')
    stats = pd.DataFrame(data={'bhf':[''], 'avg_adelay':[''], 'avg_ddelay':['']})
    i = 0
    stations = data['bhf'].unique()
    bar = Bar('Progress', max=len(stations))

    for station in stations:
        stats.at[i, 'bhf'] = station
        station_data = data[data['bhf'] == station]
        stats.at[i, 'avg_adelay'] = station_data['adelay'].mean()
        stats.at[i, 'avg_ddelay'] = station_data['ddelay'].mean()
        stats.at[i, 'med_adelay'] = station_data['adelay'].median()
        stats.at[i, 'med_ddelay'] = station_data['ddelay'].median()
        bar.next()
        i+=1
    bar.finish()
    stats.to_csv('data/stats.csv', index=False)
    stats['avg_adelay'].plot()
    stats['avg_ddelay'].plot()
    stats['med_adelay'].plot()
    stats['med_ddelay'].plot()
    plt.show()

def use_stats(stats):
    stats = stats.sort_values(by=['avg_adelay']).reset_index(drop=True)
    stats['avg_adelay'].plot()
    stats['avg_ddelay'].plot()
    stats['med_adelay'].plot()
    stats['med_ddelay'].plot()
    plt.show()
    for index, stat in stats.iterrows():
        print(stat['bhf'], stat['avg_adelay'], stat['avg_ddelay'], stat['med_adelay'], stat['med_ddelay'], sep='\t')


def isolate_2019_trains(date):
    names_path = 'data/trainData/train_names.csv'
    path = 'data/trainData/'

    date = datetime.strptime(date, '%Y-%m-%d')
    train_names = pd.read_csv('trainData/train_names.csv', index_col=False, header=None, names=['name'])
    for train in train_names['name']:
        zug_daten = pd.read_csv(path + train, sep=',', index_col=False)
        zug_daten_date = zug_daten.loc[zug_daten['date'].astype('datetime64[D]') > date]
        zug_daten_date.to_csv(path + '2019/' + train, index=False)

def städte():
    städte_path = 'data/wetterdaten/städte.csv'
    bahnhof_to_weather_location_path = 'data/wetterdaten/bahnhof_to_weather_location.csv'

    städte_csv = pd.read_csv(städte_path, sep=",", index_col=False)
    bahnhof_to_weather_location = pd.read_csv(bahnhof_to_weather_location_path, sep=",", index_col=False)
    for i in range(len(bahnhof_to_weather_location['bhf'])):
        land_df = städte_csv.loc[städte_csv['bhf'] == bahnhof_to_weather_location.loc[i, 'bhf']].reset_index(drop=True)
        if (land_df.empty):
            continue
        bahnhof_to_weather_location.loc[i, 'land'] = land_df.loc[0, 'bundesland']
    bahnhof_to_weather_location.to_csv(bahnhof_to_weather_location_path, index=False)

def get_weather_locations(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs
    bahnhof_to_weather_location_path = 'data/wetterdaten/bahnhof_to_weather_location.csv'


    bahnhof_to_weather_location = pd.read_csv(bahnhof_to_weather_location_path, sep=",", index_col=False)
    bahnhof_to_weather_location = bahnhof_to_weather_location.drop_duplicates().reset_index(drop=True)

    path = 'data/trainData/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'
    try:
        fahrplan = pd.read_csv(path2, sep=',', index_col=False)
    except:
        print('the timetable for ' + zugname + ' could not be opened')
        return
    try:
        zug_daten = pd.read_csv(path, sep=',', index_col=False)
    except pd.errors.EmptyDataError:
        print('No train data for ' + zugname)
        return 0

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
            bahnhof_to_weather_location.loc[len(bahnhof_to_weather_location['bhf']), 'bhf'] = bhf
            bahnhof_to_weather_location.loc[len(bahnhof_to_weather_location['bhf']) - 1, 'location'] = location
            bahnhof_to_weather_location.to_csv(bahnhof_to_weather_location_path, index=False)
        if(location == 'no-data'):#currently, we can only acces weather from Germany. If a location is outside of Germany, we mark it as 'no-data'. In futher versions, we may include weather from Austria, Switzerland and France.
            #print('no-data')
            continue

    bahnhof_to_weather_location.to_csv(bahnhof_to_weather_location_path, index=False)

def combine_trains():
    path = 'data/trainData/'
    path2 = 'data/combinedData/combined2020.csv'
    all_files = glob.glob(path + "/*.csv")
    print('combining trains from ' + path + ' to '+ path2)
    bar = Bar('progress', max=len(all_files))

    li = []
    for filename in all_files:
        bar.next()
        if not basename(filename):
            continue
        try:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)
        except pd.errors.EmptyDataError:
            print('error with ' + filename)

    frame = pd.concat(li, axis=0, ignore_index=True, sort=False)
    frame = frame.dropna()
    bar.finish()

    frame.to_csv(path2, index=False, compression = 'zip')


import cProfile, pstats, io

def profile(fnc):
    
    """A decorator that uses cProfile to profile a function"""
    
    def inner(*args, **kwargs):
        
        pr = cProfile.Profile()
        pr.enable()
        retval = fnc(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return inner


@profile
def get_connecting_trains():
    print('setting up \"global\" variables...', end='')
    min_stay_time = timedelta(minutes=3)
    max_stay_time = timedelta(minutes=30)
    print('done!')

    print('reading data file...', end='')

    path = 'data/combinedData/na_droped_trains.csv'
    path2 = 'data/connecting_trains.csv'

    dataset = pd.read_csv(path, index_col=False, compression='zip', usecols=['bhf', 'arr', 'dep', 'date', 'trainno', 'adelay', 'ddelay'], engine='c')
    dataset = dataset.sample(frac= 1)
    print('done!')

    print('converting \'arr\' to datetime64[m]...', end='')
    dataset['arr'] = dataset['arr'].astype('datetime64[m]')
    print('done!')
    print('converting \'dep\' to datetime64[m]...', end='')
    dataset['dep'] = dataset['dep'].astype('datetime64[m]')
    print('done!')
    print('setup completed!')

    print('sort dataset...', end='')
    dataset = dataset.sort_values(by=['bhf', 'date', 'arr'])
    print('done!')

    # create df to store connecting trains. If the index range is set to the expected
    # length of the df, it runs 350% faster. Keep that in mind
    columns = ['bhf','date', 'antrain', 'abtrain', 'arr', 'dep', 'adelay', 'ddelay']
    index = range(1500000)
    connecting_trains = pd.DataFrame(index=index, columns= columns)
    x = 0

    stations = dataset['bhf'].unique()
    bar = Bar('processing', max=len(stations))
    for station in stations:
        bar.next()
        print(' ct length: ', x ,end='', sep='')
        station_set = dataset[dataset['bhf'] == station].reset_index(drop=True)
        days = station_set['date'].unique()
        if (len(days) == len(station_set)):
            continue
        for day in days:
            day_set = station_set[station_set['date'] == day].reset_index(drop=True)
            if (len(day_set) < 2):
                continue
            for i in range(len(day_set)):
                date = day #day_set.at[i, 'date']
                time = day_set.at[i, 'arr']
                #subset = day_set[(day_set['date'] == date)]
                time_delta = (day_set['dep'] - time)
                trains_to_check = day_set[(time_delta > min_stay_time) &
                                          (time_delta < max_stay_time) &
                                          (day_set.at[i, 'trainno'] != day_set['trainno'])]
                for _index, row in trains_to_check.iterrows():
                    connecting_trains.at[x, 'bhf'] = station
                    connecting_trains.at[x, 'date'] = day
                    connecting_trains.at[x, 'antrain'] = day_set.at[i, 'trainno']
                    connecting_trains.at[x, 'abtrain'] = row['trainno']
                    connecting_trains.at[x, 'arr'] = time.strftime('%H:%M')
                    connecting_trains.at[x, 'dep'] = row['dep'].strftime('%H:%M')
                    connecting_trains.at[x, 'adelay'] = day_set.at[i, 'adelay']
                    connecting_trains.at[x, 'ddelay'] = row['ddelay']
                    x+=1
    
    bar.finish()
    connecting_trains.dropna()
    connecting_trains.to_csv(path2, index=False, compression='zip')

# combine_trains()

#list_of_dir('data/trainData2020/', 'data/trains_new2.csv')
#get_connecting_trains()

#create_stats()
#stats = pd.read_csv('data/stats.csv', index_col= False)
#use_stats(stats)