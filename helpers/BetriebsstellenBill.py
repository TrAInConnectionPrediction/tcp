import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from database.cached_table_fetch import cached_table_fetch


class NoLocationError(Exception):
    pass


class BetriebsstellenBill:
    def __init__(self, **kwargs):
        self.betriebsstellen = cached_table_fetch('betriebstellen', **kwargs)

        self.name_index_betriebsstellen = self.betriebsstellen.set_index('name')
        self.ds100_index_betriebsstellen = self.betriebsstellen.set_index('ds100')
        self.betriebsstellen_list = self.betriebsstellen.dropna(subset=['lat', 'lon'])['name'].tolist()
        self.NoLocationError = NoLocationError

    def __len__(self):
        return len(self.betriebsstellen)

    def __iter__(self):
        """
        Iterate over Betriebsstellen names

        Yields
        -------
        str
            Name of Betriebsstelle
        """
        yield from self.betriebsstellen['name']

    def get_geopandas(self):
        """
        Convert stations to geopandas DataFrame.

        Returns
        -------
        geopandas.DateFrame
            Stations with coordinates as geometry for geopandas.DataFrame.
        """
        import geopandas as gpd
        # Not all of the betriebsstellen have geo information. A GeoDataFrame without geo
        # is kind of useless, so we drop these betriebsstellen
        betriebsstellen_with_location = self.name_index_betriebsstellen.dropna(subset=['lon', 'lat'])
        return gpd.GeoDataFrame(
            betriebsstellen_with_location, 
            geometry=gpd.points_from_xy(betriebsstellen_with_location.lon, betriebsstellen_with_location.lat)
        ).set_crs("EPSG:4326")

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
