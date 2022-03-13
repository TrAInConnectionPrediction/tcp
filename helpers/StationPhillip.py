import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import cached_table_fetch, cached_table_push
from helpers import ttl_lru_cache
from config import CACHE_TIMEOUT_SECONDS
import pandas as pd
from typing import Tuple, Optional, Union, List, Literal, Set
import datetime
import time
import requests
from tqdm import tqdm

DateSelector = Union[
    datetime.datetime, list[datetime.datetime], Literal['latest'], Literal['all']
]


class StationPhillip:
    def __init__(self, **kwargs):
        if 'generate' in kwargs:
            kwargs['generate'] = False
            print('StationPhillip does not support generate')
        self.kwargs = kwargs

    @property
    @ttl_lru_cache(CACHE_TIMEOUT_SECONDS, 1)
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

        stations.set_index(
            ['name', 'eva', 'ds100'],
            drop=False,
            inplace=True,
        )
        stations.index.set_names(['name', 'eva', 'ds100'], inplace=True)
        stations = stations.sort_index()
        return stations

    @property
    @ttl_lru_cache(CACHE_TIMEOUT_SECONDS, 1)
    def name_index_stations(self) -> pd.DataFrame:
        name_index_stations = cached_table_fetch('stations', **self.kwargs).set_index(
            'name'
        )
        name_index_stations['eva'] = name_index_stations['eva'].astype(int)
        return name_index_stations

    @property
    @ttl_lru_cache(CACHE_TIMEOUT_SECONDS, 1)
    def sta_list(self) -> List[str]:
        return list(
            self._get_station(date=datetime.datetime.now())
            .sort_values(by='number_of_events', ascending=False)['name']
            .unique()
        )

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

    def to_gdf(self, date: DateSelector = None, index_cols: Tuple[str, ...] = None):
        """
        Convert stations to geopandas GeoDataFrame.

        Returns
        -------
        geopandas.GeoDataFrame
            Stations with coordinates as geometry for geopandas.GeoDataFrame.
        """
        import geopandas as gpd

        if date is not None:
            stations = self._get_station(date)
        else:
            stations = self.stations
        if index_cols is None:
            index_cols = ('name', 'eva', 'ds100', 'date')

        for level in stations.index.names:
            if level not in index_cols:
                stations = stations.droplevel(level=level)

        return gpd.GeoDataFrame(
            stations,
            geometry=gpd.points_from_xy(stations['lon'], stations['lat']),
        ).set_crs('EPSG:4326')

    @staticmethod
    def _filter_stations_by_date(
        date: DateSelector,
        stations_to_filter: pd.DataFrame,
    ):
        stations_to_filter = stations_to_filter.sort_index()
        if isinstance(date, str) and date == 'all':
            return stations_to_filter
        elif isinstance(date, str) and date == 'latest':
            # find max date for every station
            date = (
                stations_to_filter['valid_from']
                .groupby(level=stations_to_filter.index.names)
                .max()
            )
            date.name = 'date'
        elif not isinstance(date, datetime.datetime):
            # convert list / etc to dataframe
            date = pd.Series(
                index=stations_to_filter.index.unique(), data=list(date), name='date'
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
        date: DateSelector,
        name: Union[str, List[str]] = None,
        eva: Union[int, List[int]] = None,
        ds100: Union[str, List[str]] = None,
    ) -> pd.DataFrame:
        if name is not None:
            if isinstance(name, str):
                return self._filter_stations_by_date(
                    date, self.stations.xs(name, level='name')
                )
            else:
                stations = self.stations.loc[
                    (
                        name,
                        slice(None),
                        slice(None),
                    ),
                    :,
                ]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['eva', 'ds100'])

                return stations

        elif eva is not None:
            if isinstance(eva, int):
                return self._filter_stations_by_date(
                    date, self.stations.xs(eva, level='eva')
                )
            else:
                stations = self.stations.loc[
                    (
                        slice(None),
                        eva,
                        slice(None),
                    ),
                    :,
                ]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['name', 'ds100'])

                return stations

        elif ds100 is not None:
            if isinstance(ds100, str):
                return self._filter_stations_by_date(
                    date, self.stations.xs(ds100, level='ds100')
                )
            else:
                stations = self.stations.loc[
                    (
                        slice(None),
                        slice(None),
                        ds100,
                    ),
                    :,
                ]
                stations = self._filter_stations_by_date(date, stations)
                stations = stations.droplevel(level=['name', 'eva'])

                return stations

        else:
            stations = self._filter_stations_by_date(date, self.stations)
            stations = stations.droplevel(level=['name', 'eva', 'ds100'])
            return stations

    def get_eva(
        self,
        date: DateSelector,
        name: Optional[Union[str, List[str]]] = None,
        ds100: Optional[Union[str, List[str]]] = None,
    ) -> Union[int, pd.Series]:
        """
        Get eva from name or ds100

        Parameters
        ----------
        date : DateSelector
            The date of the stations to get the location for.
            - datetime.datetime : Stations that were active on the given date
            - List[datetime.datetime] : Stations that were active on a given date. Each element of the list is matched to the corresponding element of the eva, name or ds100 list.
            - 'latest' : The latest or current active station
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
            raise ValueError('Either name or ds100 must be supplied not both')

        eva = self._get_station(date=date, name=name, ds100=ds100).loc[:, 'eva']
        if isinstance(name, str) or isinstance(ds100, str):
            eva = eva.item()

        return eva

    def get_name(
        self,
        date: DateSelector,
        eva: Optional[Union[int, List[int]]] = None,
        ds100: Optional[Union[str, List[str]]] = None,
    ) -> Union[str, pd.Series]:
        """
        Get name from eva or ds100

        Parameters
        ----------
        date : DateSelector
            The date of the stations to get the location for.
            - datetime.datetime : Stations that were active on the given date
            - List[datetime.datetime] : Stations that were active on a given date. Each element of the list is matched to the corresponding element of the eva, name or ds100 list.
            - 'latest' : The latest or current active station
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
            raise ValueError('Either eva or ds100 must be supplied not both')

        name = self._get_station(date=date, eva=eva, ds100=ds100).loc[:, 'name']
        if isinstance(eva, int) or isinstance(ds100, str):
            name = name.item()

        return name

    def get_ds100(
        self,
        date: DateSelector,
        eva: Optional[Union[int, List[int]]] = None,
        name: Optional[Union[str, List[str]]] = None,
    ) -> Union[str, pd.Series]:
        """
        Get ds100 from eva or name

        Parameters
        ----------
        date : DateSelector
            The date of the stations to get the location for.
            - datetime.datetime : Stations that were active on the given date
            - List[datetime.datetime] : Stations that were active on a given date. Each element of the list is matched to the corresponding element of the eva, name or ds100 list.
            - 'latest' : The latest or current active station
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
            raise ValueError('Either eva or name must be supplied not both')

        ds100 = self._get_station(date=date, eva=eva, name=name).loc[:, 'ds100']
        if isinstance(eva, int) or isinstance(name, str):
            ds100 = ds100.item()

        return ds100

    def get_location(
        self,
        date: DateSelector,
        eva: Optional[Union[int, List[int]]] = None,
        name: Optional[Union[str, List[str]]] = None,
        ds100: Optional[Union[str, List[str]]] = None,
    ) -> Union[Tuple[int, int], pd.DataFrame]:
        """
        Get location from eva, name or ds100

        Parameters
        ----------
        date : DateSelector
            The date of the stations to get the location for.
            - datetime.datetime : Stations that were active on the given date
            - List[datetime.datetime] : Stations that were active on a given date. Each element of the list is matched to the corresponding element of the eva, name or ds100 list.
            - 'latest' : The latest or current active station
        eva : int or ArrayLike[int], optional
            The eva or evas to get the location or locations from, by default None
        name : str or ArrayLike[str], optional
            The name or names to get the location or locations from, by default None
        ds100 : str or ArrayLike[str], optional
            The ds100 or ds100ths to get the location or locations from, by default None

        Returns
        -------
        (int, int) | pd.DataFrame
            (int, int) - the single location matching (eva, name or ds100) and date
            pd.DataFrame - DataFrame with the locations matching (eva, name or ds100) and date
        """
        if eva is not None and name is not None and ds100 is not None:
            raise ValueError('Either eva, name or ds100 must be supplied not all')
        elif eva is None and name is None and ds100 is None:
            raise ValueError('Either eva, name or ds100 must be supplied not none')

        location = self._get_station(date=date, eva=eva, name=name, ds100=ds100).loc[
            :, ['lon', 'lat']
        ]
        if isinstance(eva, int) or isinstance(name, str) or isinstance(ds100, str):
            location = (location['lon'].item(), location['lat'].item())

        return location

    def add_number_of_events(self):
        from data_analysis.per_station import PerStationAnalysis

        # Fail if cache does not exist
        # TODO: This function does probably not work anymore
        per_station = PerStationAnalysis(None)

        self.stations['number_of_events'] = (
            per_station.data[('ar_delay', 'count')]
            + per_station.data[('dp_delay', 'count')]
        )

    def update_stations(self):
        modified_stations = self.stations.copy()
        modified_stations.reset_index(inplace=True, drop=True)

        iris_stations = search_iris_multiple(set(self.stations['eva']))

        # Extract meta stations and get the one we don't yet have
        metas = set()
        while True:
            for new_station in iris_stations.values():
                if new_station is not None and 'meta' in new_station:
                    metas.update(parse_meta(new_station['meta']))
            unprocessed_metas = metas - set(iris_stations.keys())
            if unprocessed_metas:
                iris_stations.update(search_iris_multiple(unprocessed_metas))
            else:
                break

        metas_per_eva = extract_metas_per_eva(iris_stations)

        if 'meta' not in modified_stations.columns:
            modified_stations['meta'] = None
        modified_stations = modified_stations.astype({'meta': 'object'})

        new_stations = []
        for eva in tqdm(iris_stations):
            try:
                old_station = self._get_station(eva=eva, date='latest').iloc[0]
                # If station attributes get changed in IRIS, the creation timestamp will not be changed.
                # Thus, the 'latest' station might not be created at the timestamp that is saved in IRIS.
                # Only if the station never got changed, 'valid_from' should be set to the creation
                # timestamp.
                update_valid_from = len(self._get_station(eva=eva, date='all')) == 1
            except KeyError:
                old_station = None

            if iris_stations[eva] is None or eva != iris_stations[eva]['eva']:
                # Station not found -> no loger valid and not in IRIS
                modified_stations.at[
                    old_station['index'], 'valid_to'
                ] = datetime.datetime.now()
                modified_stations.at[old_station['index'], 'iris'] = False
            elif old_station is None:
                # No old station -> new station -> add new station valid from creation timestamp
                # (the station might be old, but we didn't have it before)
                new_station = {
                    'name': iris_stations[eva]['name'],
                    'eva': iris_stations[eva]['eva'],
                    'ds100': iris_stations[eva]['ds100'],
                    'valid_from': datetime.datetime.strptime(
                        iris_stations[eva]['creationts'], '%d-%m-%y %H:%M:%S.%f'
                    ),
                    'valid_to': pd.Timestamp.max,
                    'meta': metas_per_eva.get(eva, None),
                    'platforms': iris_stations[eva].get('p', ''),
                    'db': iris_stations[eva].get('db', 'false') == 'true',
                    'iris': True,
                }
                new_stations.append(new_station)
            else:
                if update_valid_from:
                    creation = datetime.datetime.strptime(
                        iris_stations[eva]['creationts'], '%d-%m-%y %H:%M:%S.%f'
                    )
                    if creation > old_station['valid_from']:
                        modified_stations.at[
                            old_station['index'], 'valid_from'
                        ] = creation

                if stations_equal(iris_stations[eva], old_station):
                    # Station itself didn't change, but some data about the station might have changed
                    modified_stations.at[
                        old_station['index'], 'meta'
                    ] = metas_per_eva.get(eva, None)
                    modified_stations.at[
                        old_station['index'], 'platforms'
                    ] = iris_stations[eva].get('p', '')
                    modified_stations.at[old_station['index'], 'db'] = (
                        iris_stations[eva].get('db', 'false') == 'true'
                    )
                    modified_stations.at[old_station['index'], 'iris'] = True
                else:
                    # Station changed -> old station is valid till now and new modified station is valid from now
                    modification_ts = datetime.datetime.now()
                    modified_stations.at[
                        old_station['index'], 'valid_to'
                    ] = modification_ts

                    new_station = {
                        'name': iris_stations[eva]['name'],
                        'eva': iris_stations[eva]['eva'],
                        'ds100': iris_stations[eva]['ds100'],
                        'valid_from': modification_ts,
                        'valid_to': pd.Timestamp.max,
                        'meta': metas_per_eva.get(eva, None),
                        'platforms': iris_stations[eva].get('p', ''),
                        'db': iris_stations[eva].get('db', 'false') == 'true',
                        'iris': True,
                    }
                    new_stations.append(new_station)

        modified_stations = pd.concat(
            [modified_stations, pd.DataFrame(new_stations)], ignore_index=True
        )

        # IRIS does not give us any location information for the stations, so
        # new and modified stations will not have any location data. Also, the
        # location of some stations might have changed, so we update all the
        # location data.
        modified_stations = update_locations(modified_stations, metas_per_eva)

        modified_stations.to_pickle('modified_stations_with_location.pkl')

        print(
            f'{len(modified_stations) - len(self.stations)} stations were added or changed'
        )
        if input('Do you want to upload these to the database? [y/N] ') == 'y':
            modified_stations = modified_stations.drop(columns=['index'])
            cached_table_push(modified_stations, 'stations', fast=False)


def remove_item_from_list(lst: list, item: any) -> list:
    """
    Removes an item from a list and returns the resulting list.
    Args:
        lst: list to remove item from
        item: item to remove

    Returns: resulting list
    """
    if item in lst:
        lst.remove(item)
    return lst


def get_station_from_db_stations(eva: int) -> Union[dict, None]:
    r = requests.get(f'https://v5.db.transport.rest/stations/{eva}')
    station = r.json()
    if 'error' in station:
        return None
    elif eva == int(station['id']):
        return {
            'eva': int(station['id']),
            'name': station['name'],
            'ds100': station['ril100'],
            'lat': station['location']['latitude'],
            'lon': station['location']['longitude'],
        }
    elif str(eva) in station['additionalIds']:
        return {
            'eva': eva,
            'name': station['name'],
            'ds100': station['ril100'],
            'lat': station['location']['latitude'],
            'lon': station['location']['longitude'],
        }
    else:
        return None


def get_station_from_hafas_stop(eva: int) -> Union[dict, None]:
    r = requests.get(f'https://v5.db.transport.rest/stops/{eva}')
    stop = r.json()
    if 'error' in stop:
        return None
    elif eva == int(stop['id']):
        return {
            'eva': int(stop['id']),
            'name': stop['name'],
            # 'ds100': stop['ril100'],
            'lat': stop['location']['latitude'],
            'lon': stop['location']['longitude'],
        }
    elif 'additionalIds' in stop and str(eva) in stop['additionalIds']:
        return {
            'eva': eva,
            'name': stop['name'],
            # 'ds100': stop['ril100'],
            'lat': stop['location']['latitude'],
            'lon': stop['location']['longitude'],
        }
    else:
        return None


def stations_equal(iris_station: dict, station: pd.Series) -> bool:
    """
    Check weather two stations are equal (same name, same eva, same ds100)
    Args:
        iris_station: A station from the IRIS API
        station: A station from StationPhillip's database

    Returns: True if the stations are equal, False otherwise
    """
    return (
        iris_station['name'] == station['name']
        and iris_station['eva'] == station['eva']
        and iris_station['ds100'] == station['ds100']
    )


def parse_meta(meta: str) -> List[int]:
    meta = meta.split('|')
    return list(map(int, meta))


def get_meta_group(
    eva: int,
    metas: dict,
    checked: Set = None,
    group: Set = None
) -> Tuple[Set[int], Set[int]]:
    if checked is None:
        checked = set()
    if group is None:
        group = set()

    if eva in checked:
        return checked, group

    group.update(metas.get(eva, [eva]))
    checked.add(eva)

    while True:
        initial_group_size = len(group)
        for eva in group.copy():
            checked, group = get_meta_group(eva, metas, checked, group)
        if len(group) - initial_group_size == 0:
            break

    return checked, group


def get_pyhafas_location(client, eva: int) -> Optional[Tuple[float, float]]:
    import pyhafas.types.exceptions

    for _ in range(10):
        try:
            locations = client.locations(eva)
            for location in locations:
                if int(location.id) == eva:
                    return location.latitude, location.longitude
            else:
                return None

        except pyhafas.types.exceptions.GeneralHafasError:
            time.sleep(20)
    else:
        raise ValueError(f'Max retries exceeded for eva: {eva}')


def get_locations_midpoint(client, evas: List[int]) -> Tuple[float, float]:
    latitudes = []
    longitudes = []
    for eva in evas:
        location = get_pyhafas_location(client, eva)
        if location is not None:
            latitudes.append(location[0])
            longitudes.append(location[1])
    if len(latitudes) == 0:
        raise ValueError('No locations found for evas: {}'.format(evas))
    return sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes)


def search_station_marudor(search_term):
    search_term = search_term.replace('/', ' ')
    matches = requests.get(
        f'https://marudor.de/api/hafas/v1/station/{search_term}'
    ).json()
    return matches


def search_iris_single(search_term: Union[str, int], dev_portal_api=False):
    import urllib.parse
    from rtd_crawler.xml_parser import xml_to_json
    import lxml.etree as etree
    from config import bahn_api_headers

    if isinstance(search_term, int):
        search_term = str(search_term)
    search_term = urllib.parse.quote(search_term)

    if dev_portal_api:
        # We have a quota of 20 request per Minute
        for _ in range(5):
            matches = requests.get(
                f'https://api.deutschebahn.com/timetables/v1/station/{search_term}',
                headers=bahn_api_headers,
            )
            if matches.ok:
                break
            else:
                time.sleep(20)
    else:
        matches = requests.get(
            f'http://iris.noncd.db.de/iris-tts/timetable/station/{search_term}'
        )
    if matches.ok:
        matches = matches.text
    else:
        raise ValueError(
            'Did not get a valid response. Http Code: ' + str(matches.status_code)
        )

    matches = etree.fromstring(matches.encode())
    matches = list(xml_to_json(match) for match in matches)
    if len(matches) == 0:
        return None
    elif len(matches) != 1:
        raise ValueError(
            f'More than 1 match found for {search_term} which is unexpected'
        )
    match = matches[0]
    match['eva'] = int(match['eva'])
    return match


def search_iris_multiple(search_terms: Set[Union[str, int]], dev_portal_api=False):
    from tqdm import tqdm

    return {
        search_term: search_iris_single(search_term, dev_portal_api)
        for search_term in tqdm(search_terms, desc=f'Searching IRIS')
    }


def extract_metas_per_eva(stations: dict) -> dict:
    meta_dict = {}
    for new_station in stations.values():
        if new_station is not None and 'meta' in new_station:
            meta_dict[new_station['eva']] = parse_meta(new_station['meta'])

    meta_groups = []
    for eva in stations:
        _, group = get_meta_group(eva, meta_dict)
        if len(group) > 1:
            meta_groups.append(group)

    metas_per_eva = {}
    for group in meta_groups:
        for eva in group:
            metas_per_eva[eva] = sorted(remove_item_from_list(list(group.copy()), eva))
    return metas_per_eva


def update_locations(stations: pd.DataFrame, groups_per_eva: dict) -> pd.DataFrame:
    from pyhafas import HafasClient
    from pyhafas.profile import DBProfile

    client = HafasClient(DBProfile())

    for index, eva in tqdm(stations['eva'].iteritems(), total=len(stations)):
        location = get_pyhafas_location(client, eva)
        if location is None:
            if stations.at[index, 'valid_to'] < datetime.datetime.now():
                print(
                    f'Could not find location for EVA {eva} with pyhafas, but station is no longer in use anyway.'
                )
            else:
                print(f'Could not find location for EVA {eva} with pyhafas')
                location = get_station_from_db_stations(eva)
                location = (
                    get_station_from_hafas_stop(eva) if location is None else location
                )

                if location is not None:
                    print(f'Found location for EVA {eva} using db-stations.')
                    stations.at[index, 'lat'] = location['lat']
                    stations.at[index, 'lon'] = location['lon']
                else:
                    if not pd.isna(stations.at[index, 'lat']) and not pd.isna(
                        stations.at[index, 'lon']
                    ):
                        print(f'Location for EVA {eva} not found, but already set.')
                    elif eva in groups_per_eva:
                        group = groups_per_eva[eva]
                        lat, lon = get_locations_midpoint(client, group)
                        stations.at[index, 'lat'] = lat
                        stations.at[index, 'lon'] = lon
                        print(f'got position {lat}, {lon} from meta stations')
                    else:
                        raise ValueError(
                            f'Could not find group for EVA {eva}, so no position could be determined.'
                        )
        else:
            lat, lon = location
            stations.at[index, 'lat'] = lat
            stations.at[index, 'lon'] = lon

    return stations


def read_stations_from_derf_Travel_Status_DE_IRIS():
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


if __name__ == '__main__':
    import helpers.bahn_vorhersage

    stations = StationPhillip(prefer_cache=False)

    # stations.update_stations()

    print(stations.sta_list[:10])
    # print(
    #     stations._get_station(
    #         name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
    #         date='latest',
    #     )
    # )
    # # print(stations._get_station(eva=pd.Series([8000141, 8000141]), date=pd.Series([datetime.datetime.now(), datetime.datetime.today()])))

    # print(stations.get_location(eva=8000141, date=datetime.datetime.now()))
    # print(
    #     stations.get_location(
    #         ds100=pd.Series(['TT', 'KK']),
    #         date=pd.Series([datetime.datetime.now(), datetime.datetime.today()]),
    #     )
    # )

    # print('len:', len(stations))
