import requests
import json
from datetime import date, timedelta
import csv
import pandas as pd

from FerienApi import add_ferien
from FeiertagApi import add_feiertage
#from fahrplan import *

def get_json(zugname, day):
    payload = {'z':zugname,'d':day.strftime('%Y-%m-%d')}
    jar = requests.cookies.RequestsCookieJar()
    # TODO add config.py data
    jar.set('cook', '', domain='www.zugfinder.de', path='/')
    url = 'https://www.zugfinder.de/js/zuginfo_json.php'
    for _1 in range(3):
        try:
            r = requests.get(url, cookies=jar, params=payload)
            if (r.status_code != 200 or r.text == '[]'):
                raise ValueError('Something went wrong while doing requests.get(' + r.url + ') status code: ' + str(r.status_code))
            return r.json()
        except requests.exceptions.ChunkedEncodingError:
            print('retrying...')
            continue

def get_traindata(zugname, days, path):
    '''Gets the data of the Trains and writes it to a csv file'''
    path = path + zugname

    train_file = open(path, 'w', newline='')
    csvwriter = csv.writer(train_file)
    count = 0
    
    for i in range(0,days.days):
        
        day = date.today() - timedelta(days=i)
        print(day.strftime('%Y-%m-%d'))
        try:
            r_json = get_json(zugname, day)
        except ValueError:
            continue
        # payload = {'z':zugname,'d':day.strftime('%Y-%m-%d')}
        # jar = requests.cookies.RequestsCookieJar()
        # jar.set('cook', '', domain='www.zugfinder.de', path='/')
        # url = 'https://www.zugfinder.de/js/zuginfo_json.php'
        # r = requests.get(url, cookies=jar, params=payload)
        #if r.text != "":
        #data = r.json()
        for d in r_json:
            if count == 0:
        
                header = list(d.keys())
                header.insert(0,"date")
                header.append("isadelay")
                header.append("isddelay")
                header.append("dayname")
                header.append("daytype")
                header.append("trainno")
                csvwriter.writerow(header)
        
                count += 1
            
            value = list(d.values())
            value.insert(0,day.strftime('%Y-%m-%d')) #append Date
            #value[3] = adelay value[5] = ddelay

            #isadelay > 5
            if int(value[3]) > 5: 
                value.append(1)
            else:
                value.append(0)

            #isddelay > 5
            if int(value[5]) > 5: 
                value.append(1)
            else:
                value.append(0)
            #append dayname
            value.append(day.strftime("%A"))

            #append daytype (Weekday or not) 
            if day.weekday()<5:            
                value.append(0) #Weekday
            else:
                value.append(1) #Weekend

            #append zugname
            value.append(zugname)

            #Here we change the time variable from %H:%M to ~%H
            if int(value[6])%100 > 30:
                value[6] = round(int(value[6])/100) + 1
            else:
                value[6] = round(int(value[6])/100)

            #Some trainstations have '/' that leeds to errors so we delete it
            value[1] = value[1].replace("/", " ")

            csvwriter.writerow(value)   
    train_file.close()

#zugname = input("zugname: ")



land = 'BY' #input("Land: ")

'''
https://ferien-api.de/
https://de.wikipedia.org/wiki/ISO_3166-2:DE
'''
train_names = pd.read_csv('data/trains_new.csv', index_col=False)

days = date.today() - date(2019,12,15)

print(train_names.head())
for train in train_names['Zugnr']:
    
    train = train.replace(' ','_')
    train = train.replace('.csv','')
    
    print('Hole Zug: ' + train +'  für ' + str(days.days) + ' Tage')

    path = 'data/trainData2020/'
    get_traindata(train, days, path)#train
    
    # print('Feiertage:')
    # add_feiertage(path, land)
    
    # print('Ferien:')
    # add_ferien(path, land)

