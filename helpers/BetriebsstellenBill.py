import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
import sqlalchemy

from config import db_database, db_password, db_server, db_username


class NoLocationError(Exception):
    pass


class BetriebsstellenBill:
    def __init__(self):
        cache_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/cache/'
        if not os.path.isdir(cache_dir):
            os.mkdir(cache_dir)

        self.CACHE_PATH = cache_dir + 'betriebsstellen.pkl'

        try:
            self.engine = sqlalchemy.create_engine(
                'postgresql://' + db_username + ':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require')
            self.betriebsstellen = pd.read_sql('SELECT * FROM betriebstellen', con=self.engine)
            self.betriebsstellen.to_pickle(self.CACHE_PATH)
            self.engine.dispose()
        except:
            try:
                self.betriebsstellen = pd.read_pickle(self.CACHE_PATH)
                print('Using Betriebstellen cache')
            except FileNotFoundError:
                raise FileNotFoundError('There is no connection to the database and no local cache')

        self.name_index_betriebsstellen = self.betriebsstellen.set_index('name')
        self.ds100_index_betriebsstellen = self.betriebsstellen.set_index('ds100')
        self.NoLocationError = NoLocationError

    def __len__(self):
        return len(self.betriebsstellen)

    def get_name(self, ds100):
        return self.ds100_index_betriebsstellen.at[ds100, 'name']

    def get_ds100(self, name):
        return self.name_index_betriebsstellen.at[name, 'ds100']

    def get_location(self, name=None, ds100=None):
        if name:
            return self.get_location(ds100=self.get_ds100(name=name))
        else:
            lon = self.ds100_index_betriebsstellen.at[ds100, 'lon']
            lat = self.ds100_index_betriebsstellen.at[ds100, 'lat']
            if type(lon) == np.ndarray:
                lon = lon[0]
            if type(lat) == np.ndarray:
                lat = lat[0]
            if not lon or not lat:
                raise self.NoLocationError
            else:
                return lon, lat


if __name__ == "__main__":
    betriebsstellen = BetriebsstellenBill()
    print('len:', len(betriebsstellen))
