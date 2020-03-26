import requests
import pandas as pd


def add_feiertage(zugname):
    zugname = zugname.replace('.csv','')#to make it copatiple with more inputs
    path = 'data/trainData/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'

    zug_daten = pd.read_csv(path, sep=",", index_col=False)
    zug_daten['feiertag'] = "kein_feiertag" #add feiertag column

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

        #Land = input("Land: ")
        '''
        https://feiertage-api.de/
        https://de.wikipedia.org/wiki/ISO_3166-2:DE
        '''

        feiertage_api = requests.get('https://feiertage-api.de/api/?jahr=2019&nur_land=' + land)

        ####-Convert to list-####
        feiertage_json = feiertage_api.text
        feiertage_json.splitlines()
        feiertage_pd = pd.read_json(feiertage_json, orient='records')
        feiertage_pd = feiertage_pd.iloc[0]
        feiertage_list = feiertage_pd.tolist()
        ####-----------------####
        zug_daten.loc[zug_daten['date'].isin(feiertage_list) & zug_daten['bhf'].isin([this]), 'ferien'] = 'feiertag'

        zug_daten.to_csv(path, index=False)


#https://feiertage-api.de/
#https://de.wikipedia.org/wiki/ISO_3166-2:DE
#=======

# zugname = input("Zugname:")


# add_feiertage(zugname)