import json
# import time
# from pprint import pprint

import requests

import pandas as pd
import networkx as nx 
import matplotlib.pyplot as plt 
import swagger_client
import trassenfinder_route_request_standarts
from helpers import station_phillip
from swagger_client.rest import ApiException

# create an instance of the API class
# api_instance = swagger_client.DefaultApi()
# body = trassenfinder_route_request_standarts.s_bahn # Routensuche_request_V1 | Die zu bearbeitende Routenanfrage
# infrastrukturId = 1 # Long | ID der Infrastruktur

# print(api_instance.get_infrastrukturen())

# try: 
#     # Durchführung einer Routensuche
#     # api_response = api_instance.suche(body, infrastrukturId)
# except ApiException as e:
#     print("Exception when calling DefaultApi->suche: %s\n" % e)

def train_type_to_class(train_type):
        ['S', 'RB', 'RE', 'NWB', 'ABR', 'Bus', 'ERB', 'ICE', 'ag', 'HzL',
        'RT', 'EB', 'BRB', 'AKN', 'VIA', 'IC', 'STB', 'CB', 'SBB', 'erx',
        'ME', 'BOB', 'WEG', 'VBG', 'NBE', 'M', 'SWE', 'WFB', 'OPB', 'RTB',
        'TL', 'IRE', 'WBA', 'EVB', 'ENO', 'HSB', 'ALX', 'BLB', 'TLX', 'EC',
        'EBx', 'PRE', 'VEN', 'FEG', 'Dab', 'SDG', 'as', 'neg', 'SAB',
        'FLX', 'R', 'REX', 'STx', 'D', 'NJ', '-', 'MBB', 'SOE', 'IR',
        'HBX', 'Os', 'THA', 'AS', 'P', 'TGV', 'AZS', 'BE', 'KD', 'EN',
        'RJX', 'EX', 'RJ', 'ECE', 'OPX', '-1', 'Sp', 'BTE', 'UEX']


    if train_type in ['S', 'RT', ]:
        return 's_bahn'

    if train_type in ['ICE', 'TGV',]:
        return 'fernverkehr_triebfahrzeug'

    if train_type in ['IC', 'ECE', 'FLX', 'EC', ]:
        return 'fernverkehr_lok'

    if train_type in ['HzL', 'WEG', 'NWB', 'ERB', 'EB', 'BRB', 'AKN', 'VIA', 'STB', ]:
        return 'nahverkehr_triebfahrzeug'

    if train_type in ['s']:
        return 'nahverkehr_lok'

    ['RB', 'RE', 'ABR', 'Bus', 'ag', 
        'CB', 'SBB', 'erx',
        'ME', 'BOB', 'VBG', 'NBE', 'M', 'SWE', 'WFB', 'OPB', 'RTB',
        'TL', 'IRE', 'WBA', 'EVB', 'ENO', 'HSB', 'ALX', 'BLB', 'TLX',
        'EBx', 'PRE', 'VEN', 'FEG', 'Dab', 'SDG', 'as', 'neg', 'SAB',
        'R', 'REX', 'STx', 'D', 'NJ', '-', 'MBB', 'SOE', 'IR',
        'HBX', 'Os', 'THA', 'AS', 'P', 'AZS', 'BE', 'KD', 'EN',
        'RJX', 'EX', 'RJ', 'OPX', '-1', 'Sp', 'BTE', 'UEX']

def find_route(start, destination, train_type):
    pass

s_bahn = json.dumps(trassenfinder_route_request_standarts.s_bahn)
response = requests.post('https://openapi.trassenfinder.de/3.7.6/api/v2/infrastrukturen/1/routen/suche', data=s_bahn, headers={'content-type':'application/json'},)
df = pd.DataFrame(response.json()['result']['gewichtete_route']['routenpunkte'])
# df.to_clipboard()
for i in range(len(df) - 1):
    for key in df.at[i, 'strecke_info']:
        df.at[i, key] = df.at[i, 'strecke_info'][key]
    for key in df.at[i, 'naechstes_streckensegment']:
        df.at[i, key] = df.at[i, 'naechstes_streckensegment'][key]
        
df = df.drop(columns=['wegpunkt_index', 'naechstes_streckensegment', 'technische_fahrzeit_info',
                        'haltart', 'halteplatz_sprungart', 'schiebelok_kupplungsart',
                        'verkehrshalt_trotz_fehlendem_bahnsteig', 'halteplatz_zu_kurz', 'marktsegmente',
                        'trassenpreis_euro', 'stationspreis_euro', 'zusatzkosten_euro', 'strecke_info'])
df.to_clipboard()

