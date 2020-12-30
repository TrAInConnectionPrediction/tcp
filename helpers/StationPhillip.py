import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import random
import logging

logger = logging.getLogger("webserver." + __name__)


class StationPhillip:
    def __init__(self):
        cache_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/cache/'
        if not os.path.isdir(cache_dir):
            logger.info("Creating cache dir")
            try:
                os.mkdir(cache_dir)
            except OSError:
                logger.error("Creation of the cache directory failed")

        logger.info("Getting stations...")

        self._BUFFER_PATH = cache_dir + 'station_cache'
        try:
            from database.engine import engine
            self.station_df = pd.read_sql('SELECT * FROM stations', con=engine)
            engine.dispose()
            self.station_df.to_pickle(self._BUFFER_PATH)
        except:
            try:
                self.station_df = pd.read_pickle(self._BUFFER_PATH)
                logger.warning('Using offline station buffer')
            except FileNotFoundError:
                raise FileNotFoundError('There is no connection to the database and no local buffer')
        
        logger.info("Done")

        self.station_df['eva'] = self.station_df['eva'].astype(int)
        self.name_index_stations = self.station_df.set_index('name')
        self.eva_index_stations = self.station_df.set_index('eva')
        self.ds100_index_stations = self.station_df.set_index('ds100')
        self.sta_list = self.station_df['name'].tolist()
        self.random_sta_list = self.station_df['name'].tolist()

    def __len__(self):
        return len(self.station_df)

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n < self.__len__():
            self.n += 1
            return self.sta_list[self.n - 1]
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
        return gpd.GeoDataFrame(self.station_df, geometry=gpd.points_from_xy(self.station_df.lon, self.station_df.lat))

    def get_eva(self, name=None, ds100=None):
        """
        Get the eva from name or ds100.

        Parameters
        ----------
        name : str, optional
            Official station name, by default None
        ds100 : str, optional
            ds100 of station, by default None

        Returns
        -------
        int
            Eva of station

        Notes
        -----
        ds100 is not unique and may raise an error
        """
        if name:
            return self.name_index_stations.at[name, 'eva']
        elif ds100:
            return self.ds100_index_stations.at[ds100, 'eva']
        else:
            return None

    def get_name(self, eva=None, ds100=None):
        """
        Get the name from eva or ds100.

        Parameters
        ----------
        eva : int, optional
            eva of station, by default None
        ds100 : str, optional
            ds100 of station, by default None

        Returns
        -------
        str
            official station name

        Notes
        -----
        ds100 is not unique and may raise an error
        """
        if eva:
            return self.eva_index_stations.at[eva, 'name']
        elif ds100:
            return self.ds100_index_stations.at[ds100, 'name']
        else:
            return None

    def get_ds100(self, name=None, eva=None):
        """
        Get the ds100 from eva or station name.

        Parameters
        ----------
        name : str, optional
            Official station name, by default None
        eva : int, optional
            eva of station, by default None

        Returns
        -------
        str
            ds100 of station
        """
        if name:
            return self.name_index_stations.at[name, 'ds100']
        elif eva:
            return self.eva_index_stations.at[eva, 'ds100']
        else:
            return None

    def get_location(self, name=None, eva=None, ds100=None):
        """
        Get the location of a station.

        Parameters
        ----------
        name : str, optional
            Official station name, by default None
        eva : int, optional
            eva of station, by default None
        ds100 : str, optional
            ds100 of station, by default None

        Returns
        -------
        tuple
            longitude and latitide
        """
        if name or ds100:
            return self.get_location(eva=self.get_eva(name=name, ds100=ds100))
        else:
            return (self.eva_index_stations.at[eva, 'lon'],
                    self.eva_index_stations.at[eva, 'lat'])

    def random_iter(self):
        """
        Random order iterator over station names.

        Yields
        -------
        str
            Station names in random order.
        """
        random.shuffle(self.random_sta_list)
        for sta in self.random_sta_list:
            yield sta


if __name__ == "__main__":
    stations = StationPhillip()
    print('len:', len(stations))
