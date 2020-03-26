import requests
import json
from datetime import date, timedelta
import csv
import pandas as pd


def add_ferien(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs
    path = 'data/trainData/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'

    zug_daten = pd.read_csv(path, sep=",", index_col=False)
    zug_daten['ferien'] = "keine_ferien" #add Ferien column

    bahnhof_to_weather_location = pd.read_csv('data/wetterdaten/bahnhof_to_weather_location.csv', sep=",", index_col=False)
    stopps = zug_daten['bhf'].unique()
    for this in stopps:
        if (len(bahnhof_to_weather_location[bahnhof_to_weather_location['bhf'].isin([this])])):
            land = bahnhof_to_weather_location.loc[bahnhof_to_weather_location['bhf'].isin([this]), 'land'].reset_index(drop=True)[0]
        else:
            land = input("land for: " + this + ": ")
            bahnhof_to_weather_location.loc[len(bahnhof_to_weather_location['bhf']), 'bhf'] = this
            bahnhof_to_weather_location.loc[len(bahnhof_to_weather_location['bhf']) - 1, 'land'] = land
            bahnhof_to_weather_location.to_csv('data/wetterdaten/bahnhof_to_weather_location.csv', index=False)
        
        ferien_api = requests.get('https://ferien-api.de/api/v1/holidays/' + land)

        ####-Convert to list-####
        ferien_json = ferien_api.text.replace("T00:00", "") #entferne T00:00 um keine Formatprobleme zu bekommen (2020-02-24T00:00 -> 2020-02-24)
        ferien_pd = pd.read_json(ferien_json)
        ferien_pd = ferien_pd.drop(['name', 'slug', 'stateCode', 'year'], axis=1)
        ferien_pd = ferien_pd.astype('datetime64[D]')
        ferien_start = []
        ferien_mitte = []
        ferien_ende = []
        for i in range(len(ferien_pd['start'])):
            start = pd.date_range(start=(ferien_pd.loc[i, "start"] - pd.Timedelta(1, unit='d')), periods= 3, freq='D')
            ferien_start.extend(start.format(formatter=lambda x: x.strftime('%Y-%m-%d')))

            mitte = pd.date_range(start=ferien_pd.loc[i, "start"] + pd.Timedelta(2, unit='d'), end= ferien_pd.loc[i, "end"] - pd.Timedelta(2, unit='d'), freq='D')
            ferien_mitte.extend(mitte.format(formatter=lambda x: x.strftime('%Y-%m-%d')))

            ende = pd.date_range(start=ferien_pd.loc[i, "end"] - pd.Timedelta(2, unit='d'), periods= 2, freq='D')
            ferien_ende.extend(ende.format(formatter=lambda x: x.strftime('%Y-%m-%d')))
        ####-----------------####

        zug_daten.loc[zug_daten['date'].isin(ferien_start) & zug_daten['bhf'].isin([this]), 'ferien'] = 'start'
        zug_daten.loc[zug_daten['date'].isin(ferien_mitte) & zug_daten['bhf'].isin([this]), 'ferien'] = 'mitte'
        zug_daten.loc[zug_daten['date'].isin(ferien_ende) & zug_daten['bhf'].isin([this]), 'ferien'] = 'ende'

    zug_daten.to_csv(path, index=False)

# zugname = input("Zugname:")

#land = input("Land: ")

#https://ferien-api.de/
#https://de.wikipedia.org/wiki/ISO_3166-2:DE

#path = 'trainData/' + Zugname + '.csv'

# add_ferien(zugname)
