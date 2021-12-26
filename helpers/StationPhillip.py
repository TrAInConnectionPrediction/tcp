import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import cached_table_fetch, DB_CONNECT_STRING
from helpers import lru_cache_time
from config import CACHE_TIMEOUT_SECONDS
import pandas as pd
from typing import Tuple, Optional, Union, List, Iterator
import datetime


class StationPhillip:
    def __init__(self, **kwargs):
        if 'generate' in kwargs:
            kwargs['generate'] = False
            print('StationPhillip does not support generate')
        self.kwargs = kwargs

    @property
    @lru_cache_time(CACHE_TIMEOUT_SECONDS, 1)
    def stations(self) -> pd.DataFrame:
        stations = cached_table_fetch('stations', **self.kwargs)
        if 'valid_from' not in stations.columns:
            stations['valid_from'] = pd.NaT
        if 'valid_to' not in stations.columns:
            stations['valid_to'] = pd.NaT
        stations['valid_from'] = stations['valid_from'].fillna(pd.Timestamp.min)
        stations['valid_to'] = stations['valid_to'].fillna(pd.Timestamp.max)
        stations['eva'] = stations['eva'].astype(int)
        stations['name'] = stations['name'].astype(pd.StringDtype())
        stations['ds100'] = stations['ds100'].astype(pd.StringDtype())

        stations.set_index(['name', 'eva', 'ds100'], drop=False, inplace=True, )
        stations.index.set_names(['name', 'eva', 'ds100'], inplace=True)
        return stations

    @property
    @lru_cache_time(CACHE_TIMEOUT_SECONDS, 1)
    def name_index_stations(self) -> pd.DataFrame:
        name_index_stations = cached_table_fetch('stations', **self.kwargs).set_index('name')
        name_index_stations['eva'] = name_index_stations['eva'].astype(int)
        return name_index_stations

    @property
    @lru_cache_time(CACHE_TIMEOUT_SECONDS, 1)
    def sta_list(self) -> List[str]:
        return list(self.stations.sort_values(by='number_of_events', ascending=False)['name'].unique())

    def __len__(self):
        return len(self.stations)

    def __iter__(self):
        """
        Iterate over station names

        Yields
        -------
        str
            Name of station
        """
        yield from self.stations['name'].unique()

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
            self.stations,
            geometry=gpd.points_from_xy(self.stations.lon, self.stations.lat)
        ).set_crs("EPSG:4326")

    
    @staticmethod
    def _filter_stations_by_date(
        date: Union[datetime.datetime, Iterator[datetime.datetime]],
        stations_to_filter: pd.DataFrame,
    ):
        if not isinstance(date, datetime.datetime):
            date = pd.Series(
                index=stations_to_filter.index.unique(),
                data=list(date),
                name='date'
            )

        stations_to_filter = stations_to_filter.loc[
            (date >= stations_to_filter['valid_from'])
            & (date < stations_to_filter['valid_to'])
        ]

        stations_to_filter['date'] = date
        stations_to_filter.set_index(['date'], append=True, inplace=True)

        return stations_to_filter

    def _get_station(
            self,
            date: datetime.datetime,
            name: Union[str, Iterator[str]] = None,
            eva: Union[int, Iterator[int]] = None,
            ds100: Union[str, Iterator[str]] = None,
    ) -> pd.DataFrame:
        if name is not None:
            if isinstance(name, str):
                return self._filter_stations_by_date(date, self.stations.xs(name, level='name'))
            else:
                stations = self.stations.loc[(
                    name,
                    slice(None),
                    slice(None),
                ), :]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['eva', 'ds100'])

                return stations

        elif eva is not None:
            if isinstance(eva, int):
                return self._filter_stations_by_date(date, self.stations.xs(eva, level='eva'))
            else:
                stations = self.stations.loc[(
                    slice(None),
                    eva,
                    slice(None),
                ), :]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['name', 'ds100'])

                return stations

        elif ds100 is not None:
            if isinstance(ds100, str):
                return self._filter_stations_by_date(date, self.stations.xs(ds100, level='ds100'))
            else:
                stations = self.stations.loc[(
                    slice(None),
                    slice(None),
                    ds100,
                ), :]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['name', 'eva'])

                return stations

    def get_eva(
            self,
            date: datetime.datetime,
            name: Optional[Union[str, Iterator[str]]] = None,
            ds100: Optional[Union[str, Iterator[str]]] = None,
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
            raise ValueError("Either name or ds100 must be supplied not both")
        elif name is None and ds100 is None:
            raise ValueError("Either name or ds100 must be supplied not none")

        eva = self._get_station(date=date, name=name, ds100=ds100).loc[:, 'eva']
        if isinstance(date, datetime.datetime):
            eva = eva.item()

        return eva

    def get_name(
        self,
        date: datetime.datetime,
        eva: Optional[Union[int, Iterator[int]]] = None,
        ds100: Optional[Union[str, Iterator[str]]] = None,
    ) -> Union[str, pd.Series]:
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
            pd.Series: Series with the names matching eva or ds100. Contains NaNs
            if no name was found for a given eva or ds100.
        """
        if eva is not None and ds100 is not None:
            raise ValueError("Either eva or ds100 must be supplied not both")
        elif eva is None and ds100 is None:
            raise ValueError("Either eva or ds100 must be supplied not none")

        name = self._get_station(date=date, eva=eva, ds100=ds100).loc[:, 'name']
        if isinstance(date, datetime.datetime):
            name = name.item()

        return name

    def get_ds100(
        self,
        date: datetime.datetime,
        eva: Optional[Union[int, Iterator[int]]] = None,
        name: Optional[Union[str, Iterator[str]]] = None,
    ) -> Union[str, pd.Series]:
        """
        Get ds100 from eva or name

        Parameters
        ----------
        eva : int or ArrayLike[int], optional
            The eva or evas to get the ds100 or ds100ths from, by default None
        name : str or ArrayLike[str], optional
            The name or names to get the ds100 or ds100ths from, by default None

        Returns
        -------
        str | pd.Series
            str - the single ds100 matching eva or name
            pd.Series: Series with the ds100 matching eva or name. Contains NaNs
            if no ds100 was found for a given eva or name.
        """
        if eva is not None and name is not None:
            raise ValueError("Either eva or name must be supplied not both")
        elif eva is None and name is None:
            raise ValueError("Either eva or name must be supplied not none")

        ds100 = self._get_station(date=date, eva=eva, name=name).loc[:, 'ds100']
        if isinstance(date, datetime.datetime):
            ds100 = ds100.item()

        return ds100

    def get_location(
        self,
        date: datetime.datetime,
        eva: Optional[Union[int, Iterator[int]]] = None,
        name: Optional[Union[str, Iterator[str]]] = None,
        ds100: Optional[Union[str, Iterator[str]]] = None,
    ) -> Union[Tuple[int, int], pd.DataFrame]:
        """
        Get location from eva, name or ds100

        Parameters
        ----------
        eva : int or ArrayLike[int], optional
            The eva or evas to get the location or locations from, by default None
        name : str or ArrayLike[str], optional
            The name or names to get the location or locations from, by default None
        ds100 : str or ArrayLike[str], optional
            The ds100 or ds100ths to get the location or locations from, by default None

        Returns
        -------
        pd.DataFrame
            DataFrame with the locations matching eva, name or ds100. Contains NaNs
            if no location was found for a given eva, name or ds100.
        """
        if eva is not None and name is not None and ds100 is not None:
            raise ValueError("Either eva, name or ds100 must be supplied not all")
        elif eva is None and name is None and ds100 is None:
            raise ValueError("Either eva, name or ds100 must be supplied not none")

        location = self._get_station(date=date, eva=eva, name=name, ds100=ds100).loc[:, ['lon', 'lat']]
        if isinstance(date, datetime.datetime):
            location = (location['lon'].item(), location['lat'].item())

        return location

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

    @staticmethod
    def read_stations_from_derf_Travel_Status_DE_IRIS():
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
        # TODO: This function does probably not work anymore
        per_station = PerStationAnalysis(None)

        self.stations['number_of_events'] = (
                per_station.data[('ar_delay', 'count')] + per_station.data[('dp_delay', 'count')]
        )

    def push_to_db(self):
        self.stations.to_sql('stations', DB_CONNECT_STRING, if_exists='replace', method='multi')


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    stations = StationPhillip(prefer_cache=False)
    print(stations.sta_list[:10])
    print(stations._get_station(name=pd.Series(['Tübingen Hbf', 'Köln Hbf']), date=pd.Series([datetime.datetime.now(), datetime.datetime.today()])))
    # print(stations._get_station(eva=pd.Series([8000141, 8000141]), date=pd.Series([datetime.datetime.now(), datetime.datetime.today()])))

    print(stations.get_location(eva=8000141, date=datetime.datetime.now()))
    print(stations.get_location(ds100=pd.Series(['TT', 'KK']), date=pd.Series([datetime.datetime.now(), datetime.datetime.today()])))

    print('len:', len(stations))
