import requests
from datetime import datetime, timedelta
from pytz import timezone
import logging

def toLocal(dt):
    ''' Setzt automatich in die Zeitzone in unsere und convertiert die Zeit'''
    return dt.astimezone(timezone('Europe/Berlin'))
def toUtc(dt):
    ''' Setzt automatich in die Zeitzone'''
    return dt.astimezone(timezone('UTC'))
def setToUTCTime(dt, diff):
    ''' Setzt die Zeitzone des Datetime objects auf utc, diff ist die differenz zu utc'''
    #also um bsp Berlin ist die dif 1 weil wir in GMT+1 leben und somit um auf ut zu kommen müssen -1 h machen
    return dt.replace(tzinfo=timezone('UTC')) - timedelta(hours=diff)

def get_station(station):
    ''' Gives the top 24 stations the station string \n
        string ```station```: name of the station\n
        \bReturn: list of stations with id
        '''
    url = "https://marudor.de/api/hafas/v1/station/" + station + "?profile=db"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return False

def get_connection(start, destination, time):
    ''' Get connections using marudor hafas api \n
        string ```start```: start station name \\
        string ```destination```: destination station name \\
        datetime ```time```: time of departure \n
        \bReturn: text of the request
        '''
    #marudor will unix zeit in UTC zeit aber .timestamp macht des automatich wenn die Zeitzone gesetzt ist
    json={"start":get_station(start)[0]['id'],"destination":get_station(destination)[0]['id'],"time":setToUTCTime(time, 1).timestamp()*1000,"maxChanges":"-1","transferTime":"0","hafasProfile":"db"}
    logging.debug(json)
    r = requests.post('https://marudor.de/api/hafas/v1/route?profile=db', json=json)
    logging.debug(r.status_code)
    return r.text


def clean_data(connection):
    ''' Remove unneded content'''
    for i in range(len(connection)):
        #we need the segmentTypes
        connection[i]['segments'].append({ 'segmentTypes': connection[i]['segmentTypes']})
        connection[i] = connection[i]['segments']
        for n in range(len(connection[i])):
            if 'wings' in connection[i][n]:
                del connection[i][n]['wings']
            if 'messages' in connection[i][n]:
                del connection[i][n]['messages']
            if 'jid' in connection[i][n]:
                del connection[i][n]['jid']
            if 'type' in connection[i][n]:
                del connection[i][n]['type']
            if 'stops' in connection[i][n]:
                del connection[i][n]['stops']
            if 'finalDestination' in connection[i][n]:
                del connection[i][n]['finalDestination']
            connection[i][-1]['segmentTypes'] = list(dict.fromkeys(connection[i][-1]['segmentTypes']))
    return connection
#nur nen modell, das Zeigt auch wenn wir in unserer Zeitzone sind mach des kein unterschied
#print(get_connection("Tübingen Hbf", "Köln Hbf", toLocal(setToUTCTime(datetime.now(), 0))))