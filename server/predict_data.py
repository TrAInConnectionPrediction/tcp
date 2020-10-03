import pandas as pd
from datetime import datetime
import pickle
import logging
import os

logger = logging.getLogger(__name__)


class prediction_data:
    def __init__(self):
        self.fahrplaene = {}
        self.fahrplan_basepath = os.path.dirname(os.path.realpath(__file__)) + '/static_data/streckendaten/'

        try:
            with open(self.fahrplan_basepath + 'missing_fahrplaene.csv', 'rb+') as fp:
                self.missing_fahrplaene = pickle.load(fp)
        except FileNotFoundError:
            logger.warning('missing_fahrplaene not found in ' + self.fahrplan_basepath + 'so i\m creating it')
            open(self.fahrplan_basepath + 'missing_fahrplaene', 'a').close()
            self.missing_fahrplaene = []

    def get_pred_data(self, bhf, zugname, date):
        zugname = zugname.replace(' ', '_')
        fahrplan = self.load_fahrplan(zugname)
        try:
            if (fahrplan == False):
                return False
        except ValueError:
            pass
        if not bhf in fahrplan.index:
            logger.warning("Bhf not in Timetable " + bhf)
            # if we don't have the bhf in the timetable, we can't get the data
            return False

        data = {'month': date.month, 'dayofweek': date.weekday(), 'hour': date.hour, 'track_length_since_start': 0,
                'time_since_first_station': 0, 'station_number': 0, 'lat': 0, 'lon': 0, 'track_length': 0,
                'stay_time': 0, 'time_since_last_station': 0, 'start_lat': 0, 'start_lon': 0, 'destination_lat': 0,
                'destination_lon': 0, 'total_lenth': 0, 'total_time': 0, 'delta_lon': 0, 'delta_lat': 0}

        try:
            data['track_length_since_start'] = fahrplan.at[bhf, 'track_length_since_start']
            data['time_since_last_station'] = fahrplan.at[bhf, 'time_since_last_station']
            data['station_number'] = fahrplan.at[bhf, 'station_number']
            data['lon'] = fahrplan.at[bhf, 'lon']
            data['lat'] = fahrplan.at[bhf, 'lat']
            data['track_length'] = fahrplan.at[bhf, 'track_length']
            data['stay_time'] = fahrplan.at[bhf, 'stay_time']
            data['start_lat'] = fahrplan.at[bhf, 'start_lat']
            data['start_lon'] = fahrplan.at[bhf, 'start_lon']
            data['destination_lat'] = fahrplan.at[bhf, 'destination_lat']
            data['destination_lon'] = fahrplan.at[bhf, 'destination_lon']
            data['total_lenth'] = fahrplan.at[bhf, 'total_lenth']
            data['total_time'] = fahrplan.at[bhf, 'total_time']
            data['delta_lon'] = fahrplan.at[bhf, 'delta_lon']
            data['delta_lat'] = fahrplan.at[bhf, 'delta_lat']
        except KeyError:
            logger.warning("For some reason" + zugname + " has a key error")

        try:  # sometimes data['stay_time'] is kind of an array.
            # I don't know why, but we catch it anyway.
            if pd.isna(data['stay_time']):
                data['stay_time'] = 0
        except ValueError:
            return False
        data['time_since_first_station'] = fahrplan.at[bhf, 'time_since_first_station']

        return data

    def load_fahrplan(self, zugname):
        try:
            fahrplan = self.fahrplaene[zugname]
            return fahrplan
        except KeyError:
            # if the fahrplan is not in memory, try to load it from disk
            try:
                fahrplan = pd.read_csv(self.fahrplan_basepath + zugname + '.csv',
                                       sep=",",
                                       index_col=False,
                                       engine='c')  # c engine is a little faster
                fahrplan = fahrplan.set_index('bhf')
                self.fahrplaene[zugname] = fahrplan
                # print('loaded timetable for ' + zugname)
                return fahrplan
            except (FileNotFoundError, UnicodeDecodeError):
                # If the file was not found we save the zugname to later get the timetable
                if not zugname in self.missing_fahrplaene:
                    self.missing_fahrplaene.append(zugname)
                    with open(self.fahrplan_basepath + 'missing_fahrplaene.csv', 'wb') as fp:
                        pickle.dump(self.missing_fahrplaene, fp)

                logger.warning("No Timetable for " + zugname)
                return False
