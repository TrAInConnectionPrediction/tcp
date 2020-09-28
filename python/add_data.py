import pandas as pd
from datetime import datetime
import json
import sys

frame = pd.read_csv('data/combinedData/alltrains2019.csv',encoding='UTF-8',compression='zip', engine='c')
bhfs = pd.read_csv('stations.csv',encoding='UTF-8')
bhfs = bhfs[bhfs.loc[:,'filename'].notnull()].set_index("filename")
frame = frame.set_index("bhf")

filename = sys.argv[1]
with open('data/wetterdaten/wetterdaten2019/' + filename) as json_file:
    weather_json = pd.DataFrame.from_dict(json.load(json_file)['history_1h'])
try:
    bhf = frame.loc[frame.index.isin(bhfs.loc[filename.replace(".json", ""), "name"].tolist())].reset_index()
except:
    print(str(sys.exc_info()[0]) + " Error " + bhfs.loc[filename.replace(".json", ""), "name"])
    bhf = frame.loc[frame2.index.isin([bhfs.loc[filename.replace(".json", ""), "name"]])].reset_index()

bhf = bhf.reset_index()
data = pd.DataFrame()
if len(bhf) < 1:
    exit()
#weather_json.set_index("time", inplace = True)
for i, x in bhf.iterrows():
    date = datetime.strptime(bhf.at[i,'date'], '%Y-%m-%d')
    ####-Add-weather-to-trainData-####

    # 1. nächste Uhrzeit finden
    arr_time = bhf.at[i, 'arr'] if bhf.at[i, 'arr'] != '99:99' else bhf.at[i, 'dep']
    try: #sometimes there are times like '14:99'. We are skipping those.
        arr_time = pd.to_datetime(date.strftime('%Y-%m-%d') + 'T' + arr_time)
    except:
        print("except")
        continue
    x = weather_json['time'].astype('datetime64[h]').reset_index(drop=True)



    #Calculate the timedelta amount
    v = x.apply(lambda dep_time: dep_time - arr_time if dep_time > arr_time else arr_time - dep_time)


    #find nearest time
    date = x[v.idxmin()].strftime('%Y-%m-%d %H:%M')
    #print(date)
    #all data with matching date and time
    data =  weather_json.loc[weather_json['time'].isin([date]), :] #.astype('datetime64[ns]')
    data = data.reset_index(drop=True)
    # 2. daten eintragen
    bhf.at[i, 'sealevelpressure'] = data.at[0, 'sealevelpressure']
    bhf.at[i, 'temperature'] = data.at[0, 'temperature']
    bhf.at[i, 'precipitation'] = data.at[0, 'precipitation']
    bhf.at[i, 'snowfraction'] = data.at[0, 'snowfraction']
    bhf.at[i, 'winddirection'] = data.at[0, 'winddirection']
    bhf.at[i, 'windspeed'] = data.at[0, 'windspeed']
    bhf.at[i, 'relativehumidity'] = data.at[0, 'relativehumidity']
    ####--------------------------####
    if ((i % 100) == 0):
        print('|', end='')
bhf.to_csv('data/wetterdaten/combined/' + filename.replace(".json", "") + '.csv',encoding='UTF-8')
