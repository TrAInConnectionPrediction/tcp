import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np


class NoLocationError(Exception):
    pass


class BetriebsstellenBill:
    def __init__(self):
        cache_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/cache/'
        if not os.path.isdir(cache_dir):
            try:
                os.mkdir(cache_dir)
            except OSError:
                pass

        self.CACHE_PATH = cache_dir + 'betriebsstellen_cache'
        try:
            from database.engine import engine
            self.betriebsstellen = pd.read_sql('SELECT * FROM betriebstellen', con=engine)
            engine.dispose()
            self.betriebsstellen.to_pickle(self.CACHE_PATH)
        except Exception as e:
            print(e)
            try:
                self.betriebsstellen = pd.read_pickle(self.CACHE_PATH)
                print('Using offline station buffer')
            except FileNotFoundError:
                raise FileNotFoundError('There is no connection to the database and no local buffer')

        self.name_index_betriebsstellen = self.betriebsstellen.set_index('name')
        self.ds100_index_betriebsstellen = self.betriebsstellen.set_index('ds100')
        self.betriebsstellen_list = self.betriebsstellen.dropna(subset=['lat', 'lon'])['name'].tolist()
        self.NoLocationError = NoLocationError

    def __len__(self):
        return len(self.betriebsstellen)

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n < len(self.betriebsstellen_list):
            self.n += 1
            return self.betriebsstellen_list[self.n - 1]
        else:
            raise StopIteration

    def get_geopandas(self):
        """
        Convert stations to geopandas DataFrame.

        Returns
        -------
        geopandas.DateFrame
            Stations with coordinates as geometry for geopandas.DataFrame.
        """
        import geopandas as gpd
        return gpd.GeoDataFrame(self.name_index_betriebsstellen, geometry=gpd.points_from_xy(self.name_index_betriebsstellen.lon, self.name_index_betriebsstellen.lat))

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
    for be in betriebsstellen:
        print(be)
