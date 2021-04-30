import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import cached_table_fetch, DB_CONNECT_STRING
import pandas as pd
from typing import Tuple, Optional, Union, Any


class StationPhillip:
    def __init__(self, **kwargs):
        if 'generate' in kwargs:
            kwargs['generate'] = False
            print('StationPhillip does not support generate')
        
        self.name_index_stations = cached_table_fetch('stations', index_col='name', **kwargs)
        self.name_index_stations['eva'] = self.name_index_stations['eva'].astype(int)

        self.eva_index_stations = self.name_index_stations.reset_index().set_index('eva')
        self.ds100_index_stations = self.name_index_stations.reset_index().set_index('ds100')
        self.sta_list = self.name_index_stations.sort_values(by='number_of_events', ascending=False).index.to_list()

    def __len__(self):
        return len(self.name_index_stations)

    def __iter__(self):
        """
        Iterate over station names

        Yields
        -------
        str
            Name of station
        """
        yield from self.name_index_stations.index

    def to_gpd(self):
        """
        Convert stations to geopandas DataFrame.

        Returns
        -------
        geopandas.DateFrame
            Stations with coordinates as geometry for geopandas.DataFrame.
        """
        import geopandas as gpd
        return gpd.GeoDataFrame(
            self.name_index_stations,
            geometry=gpd.points_from_xy(self.name_index_stations.lon, self.name_index_stations.lat)
        ).set_crs("EPSG:4326")

    def get_eva(
        self,
        name: Optional[Union[str, Any]]=None,
        ds100: Optional[Union[str, Any]]=None
    ) -> Union[int, pd.Series]:
        """
        Get eva from name or ds100

        Parameters
        ----------
        name : str or ArrayLike[str], optional
            The name or names to get the eva or evas from, by default None
        ds100 : str or ArrayLike[str], optional
            The ds100 or ds100ths to get the eva or evas from, by default None

        Returns
        -------
        int | pd.Series
            int - the single eva matching name or ds100
            pd.Series: Series with the evas matching name or ds100. Contains NaNs
            if no eva was found for a given name or ds100.
        """        
        if name is not None and ds100 is not None:
            raise ValueError("Either name or ds100 must be defined not none")

        if name is not None:
            if isinstance(name, str):
                return self.name_index_stations.at[name, 'eva']
            else:
                evas = pd.Series(index=name, name='eva', dtype=int)
                evas.loc[:] = self.name_index_stations.loc[:, 'eva']
                return evas

        elif ds100 is not None:
            if isinstance(ds100, str):
                return self.ds100_index_stations.at[ds100, 'eva']
            else:
                evas = pd.Series(index=ds100, name='eva', dtype=int)
                evas.loc[:] = self.ds100_index_stations.loc[:, 'eva']
                return evas
        else:
            raise ValueError("Either name or ds100 must be defined not none")

    def get_name(
        self,
        eva: Optional[Union[int, Any]]=None,
        ds100: Optional[Union[str, Any]]=None
    ) -> Union[int, pd.Series]:
        """
        Get name from eva or ds100

        Parameters
        ----------
        eva : int or ArrayLike[int], optional
            The eva or evas to get the name or names from, by default None
        ds100 : str or ArrayLike[str], optional
            The ds100 or ds100ths to get the name or names from, by default None

        Returns
        -------
        str | pd.Series
            str - the single name matching eva or ds100
            pd.Series: Series with the names matching evas or ds100ths. Contains NaNs
            if no name was found for a given eva or ds100.
        """        
        if eva is not None and ds100 is not None:
            raise ValueError("Either eva or ds100 must be defined not none")

        if eva is not None:
            if isinstance(eva, int):
                return self.eva_index_stations.at[eva, 'name']
            else:
                names = pd.Series(index=eva, name='name', dtype=str)
                names.loc[:] = self.eva_index_stations.loc[:, 'name']
                return names

        elif ds100 is not None:
            if isinstance(ds100, str):
                return self.ds100_index_stations.at[ds100, 'name']
            else:
                names = pd.Series(index=ds100, name='name', dtype=str)
                names.loc[:] = self.ds100_index_stations.loc[:, 'name']
                return names
        else:
            raise ValueError("Either name or ds100 must be defined not none")

    def get_ds100(
        self,
        name: Optional[Union[str, Any]]=None,
        eva: Optional[Union[int, Any]]=None
    ) -> Union[str, pd.Series]:
        """
        Get ds100 from name or eva

        Parameters
        ----------
        name : str or ArrayLike[str], optional
            The name or names to get the ds100 or ds100ths from, by default None
        eva : int or ArrayLike[int], optional
            The eva or evas to get the ds100 or ds100ths from, by default None

        Returns
        -------
        str | pd.Series
            str - the single ds100 matching name or eva
            pd.Series: Series with the ds100ths matching name or eva. Contains NaNs
            if no ds100 was found for a given name or eva.
        """        
        if name is not None and eva is not None:
            raise ValueError("Either name or eva must be defined not none")

        if name is not None:
            if isinstance(name, str):
                return self.name_index_stations.at[name, 'ds100']
            else:
                ds100ths = pd.Series(index=name, name='ds100', dtype=str)
                ds100ths.loc[:] = self.name_index_stations.loc[:, 'ds100']
                return ds100ths

        elif eva is not None:
            if isinstance(eva, int):
                return self.eva_index_stations.at[eva, 'ds100']
            else:
                ds100ths = pd.Series(index=eva, name='ds100', dtype=str)
                ds100ths.loc[:] = self.eva_index_stations.loc[:, 'ds100']
                return ds100ths
        else:
            raise ValueError("Either name or eva must be defined not none")

    def get_location(
        self,
        name: Optional[Union[str, Any]]=None,
        eva: Optional[Union[int, Any]]=None,
        ds100: Optional[Union[str, Any]]=None
    ) -> Union[Tuple[float, float], pd.DataFrame]:
        """
        Get the location (lon, lat) of a station or multiple stations

        Parameters
        ----------
        name : str | ArrayLike[str], optional
            Name or names of the station or stations, by default None
        eva : int | ArrayLike[int], optional
            Eva or evas of the station or stations, by default None
        ds100 : str | ArrayLike[str], optional
            ds100 or ds100ths of the station or stations, by default None

        Returns
        -------
        Tuple[float, float] | pd.DataFrame
            Tuple[float, float]: longitude and latitude of the station
            pd.DataFrame: DataFrame with a lon and a lat column with the
            location of the stations
        """
        if name is None:
            return self.get_location(name=self.get_name(eva=eva, ds100=ds100))
        else:
            if isinstance(name, str):
                return (self.name_index_stations.at[name, 'lon'],
                        self.name_index_stations.at[name, 'lat'])
            else:
                locations = pd.DataFrame(index=name, columns=['lon', 'lat'])
                locations.loc[:, ['lon', 'lat']] = self.name_index_stations.loc[:, ['lon', 'lat']]
                return locations

    @staticmethod
    def search_station(search_term):
        import requests
        search_term = search_term.replace('/', ' ')
        matches = requests.get(f'https://marudor.de/api/hafas/v1/station/{search_term}').json()
        return matches

    @staticmethod
    def search_iris(search_term):
        import requests
        from rtd_crawler.xml_parser import xml_to_json
        import lxml.etree as etree

        search_term = search_term.replace('/', ' ')
        matches = requests.get(f'http://iris.noncd.db.de/iris-tts/timetable/station/{search_term}').text
        matches = etree.fromstring(matches.encode())
        matches = list(xml_to_json(match) for match in matches)
        return matches

    def read_stations_from_derf_Travel_Status_DE_IRIS(self):
        import requests

        derf_stations = requests.get(
            'https://raw.githubusercontent.com/derf/Travel-Status-DE-IRIS/master/share/stations.json'
        ).json()
        parsed_derf_stations = {
            'name': [],
            'eva': [],
            'ds100': [],
            'lat': [],
            'lon': [],
        }
        for station in derf_stations:
            parsed_derf_stations['name'].append(station['name'])
            parsed_derf_stations['eva'].append(station['eva'])
            parsed_derf_stations['ds100'].append(station['ds100'])
            parsed_derf_stations['lat'].append(station['latlong'][0])
            parsed_derf_stations['lon'].append(station['latlong'][1])
        return pd.DataFrame(parsed_derf_stations)

    def add_number_of_events(self):
        from data_analysis.per_station import PerStationAnalysis
        # Fail if cache does not exist
        per_station = PerStationAnalysis(None)

        self.name_index_stations['number_of_events'] = (
            per_station.data[('ar_delay', 'count')] + per_station.data[('dp_delay', 'count')]
        )

    def push_to_db(self):
        self.name_index_stations.to_sql('stations', DB_CONNECT_STRING, if_exists='replace', method='multi')


if __name__ == "__main__":
    import helpers.fancy_print_tcp
    stations = StationPhillip(prefer_cache=True)

    print('len:', len(stations))
