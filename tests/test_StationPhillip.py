import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers import StationPhillip
import datetime
import numpy as np
import pandas as pd


stations = StationPhillip(prefer_cache=False)


def test_get_eva():
    assert (
        stations.get_eva(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1))
        == 8000141
    )
    assert (
        stations.get_eva(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)) == 8000207
    )

    assert stations.get_eva(ds100='TT', date=datetime.datetime(2021, 1, 1)) == 8000141
    assert stations.get_eva(ds100='KK', date=datetime.datetime(2021, 1, 1)) == 8000207

    evas = stations.get_eva(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(evas, pd.Series)
    assert 'eva' == evas.name
    assert ['ds100', 'date'] == evas.index.names
    assert evas.dtype == int
    assert evas[('TT', datetime.datetime(2021, 1, 1))] == 8000141
    assert evas[('KK', datetime.datetime(2021, 1, 1))] == 8000207

    evas = stations.get_eva(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(evas, pd.Series)
    assert 'eva' == evas.name
    assert ['name', 'date'] == evas.index.names
    assert evas.dtype == int
    assert evas[('Tübingen Hbf', datetime.datetime(2021, 1, 1))] == 8000141
    assert evas[('Köln Hbf', datetime.datetime(2021, 1, 1))] == 8000207


def test_get_name():
    assert (
        stations.get_name(eva=8000141, date=datetime.datetime(2021, 1, 1))
        == 'Tübingen Hbf'
    )
    assert (
        stations.get_name(eva=8000207, date=datetime.datetime(2021, 1, 1)) == 'Köln Hbf'
    )

    assert (
        stations.get_name(ds100='TT', date=datetime.datetime(2021, 1, 1))
        == 'Tübingen Hbf'
    )
    assert (
        stations.get_name(ds100='KK', date=datetime.datetime(2021, 1, 1)) == 'Köln Hbf'
    )

    names = stations.get_name(
        eva=(8000141, 8000207),
        date=(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)),
    )

    assert isinstance(names, pd.Series)
    assert 'name' == names.name
    assert ['eva', 'date'] == names.index.names
    assert names.dtype == pd.StringDtype()
    assert names[(8000141, datetime.datetime(2021, 1, 1))] == 'Tübingen Hbf'
    assert names[(8000207, datetime.datetime(2021, 1, 1))] == 'Köln Hbf'

    names = stations.get_name(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(names, pd.Series)
    assert 'name' == names.name
    assert ['ds100', 'date'] == names.index.names
    assert names.dtype == pd.StringDtype()
    assert names[('TT', datetime.datetime(2021, 1, 1))] == 'Tübingen Hbf'
    assert names[('KK', datetime.datetime(2021, 1, 1))] == 'Köln Hbf'


def test_get_ds100():
    assert stations.get_ds100(eva=8000141, date=datetime.datetime(2021, 1, 1)) == 'TT'
    assert stations.get_ds100(eva=8000207, date=datetime.datetime(2021, 1, 1)) == 'KK'

    assert (def test_get_eva():
    assert (
        stations.get_eva(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1))
        == 8000141
    )
    assert (
        stations.get_eva(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)) == 8000207
    )

    assert stations.get_eva(ds100='TT', date=datetime.datetime(2021, 1, 1)) == 8000141
    assert stations.get_eva(ds100='KK', date=datetime.datetime(2021, 1, 1)) == 8000207

    evas = stations.get_eva(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(evas, pd.Series)
    assert 'eva' == evas.name
    assert ['ds100', 'date'] == evas.index.names
    assert evas.dtype == int
    assert evas[('TT', datetime.datetime(2021, 1, 1))] == 8000141
    assert evas[('KK', datetime.datetime(2021, 1, 1))] == 8000207

    evas = stations.get_eva(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(evas, pd.Series)
    assert 'eva' == evas.name
    assert ['name', 'date'] == evas.index.names
    assert evas.dtype == int
    assert evas[('Tübingen Hbf', datetime.datetime(2021, 1, 1))] == 8000141
    assert evas[('Köln Hbf', datetime.datetime(2021, 1, 1))] == 8000207


def test_get_name():
    assert (
        stations.get_name(eva=8000141, date=datetime.datetime(2021, 1, 1))
        == 'Tübingen Hbf'
    )
    assert (
        stations.get_name(eva=8000207, date=datetime.datetime(2021, 1, 1)) == 'Köln Hbf'
    )

    assert (
        stations.get_name(ds100='TT', date=datetime.datetime(2021, 1, 1))
        == 'Tübingen Hbf'
    )
    assert (
        stations.get_name(ds100='KK', date=datetime.datetime(2021, 1, 1)) == 'Köln Hbf'
    )

    names = stations.get_name(
        eva=(8000141, 8000207),
        date=(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)),
    )

    assert isinstance(names, pd.Series)
    assert 'name' == names.name
    assert ['eva', 'date'] == names.index.names
    assert names.dtype == pd.StringDtype()
    assert names[(8000141, datetime.datetime(2021, 1, 1))] == 'Tübingen Hbf'
    assert names[(8000207, datetime.datetime(2021, 1, 1))] == 'Köln Hbf'

    names = stations.get_name(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(names, pd.Series)
    assert 'name' == names.name
    assert ['ds100', 'date'] == names.index.names
    assert names.dtype == pd.StringDtype()
    assert names[('TT', datetime.datetime(2021, 1, 1))] == 'Tübingen Hbf'
    assert names[('KK', datetime.datetime(2021, 1, 1))] == 'Köln Hbf'


def test_get_ds100():
    assert stations.get_ds100(eva=8000141, date=datetime.datetime(2021, 1, 1)) == 'TT'
    assert stations.get_ds100(eva=8000207, date=datetime.datetime(2021, 1, 1)) == 'KK'

    assert (
        stations.get_ds100(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1))
        == 'TT'
    )
    assert (
        stations.get_ds100(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)) == 'KK'
    )

    ds100s = stations.get_ds100(
        eva=(8000141, 8000207),
        date=(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)),
    )

    assert isinstance(ds100s, pd.Series)
    assert 'ds100' == ds100s.name
    assert ['eva', 'date'] == ds100s.index.names
    assert ds100s.dtype == pd.StringDtype()
    assert ds100s[(8000141, datetime.datetime(2021, 1, 1))] == 'TT'
    assert ds100s[(8000207, datetime.datetime(2021, 1, 1))] == 'KK'

    ds100s = stations.get_ds100(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(ds100s, pd.Series)
    assert 'ds100' == ds100s.name
    assert ['name', 'date'] == ds100s.index.names
    assert ds100s.dtype == pd.StringDtype()
    assert ds100s[('Tübingen Hbf', datetime.datetime(2021, 1, 1))] == 'TT'
    assert ds100s[('Köln Hbf', datetime.datetime(2021, 1, 1))] == 'KK'


def test_get_location():
    np.testing.assert_allclose(
        stations.get_location(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    np.testing.assert_allclose(
        stations.get_location(eva=8000141, date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(eva=8000207, date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    np.testing.assert_allclose(
        stations.get_location(ds100='TT', date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(ds100='KK', date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    locations = stations.get_location(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['name', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[('Tübingen Hbf', datetime.datetime(2021, 1, 1))],
        (9.055407, 48.515811),
    )
    np.testing.assert_allclose(
        locations.loc[('Köln Hbf', datetime.datetime(2021, 1, 1))],
        (6.958729, 50.943030),
    )

    locations = stations.get_location(
        eva=pd.Series([8000141, 8000207]),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['eva', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[(8000141, datetime.datetime(2021, 1, 1))], (9.055407, 48.515811)
    )
    np.testing.assert_allclose(
        locations.loc[(8000207, datetime.datetime(2021, 1, 1))], (6.958729, 50.943030)
    )

    locations = stations.get_location(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['ds100', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[('TT', datetime.datetime(2021, 1, 1))], (9.055407, 48.515811)
    )
    np.testing.assert_allclose(
        locations.loc[('KK', datetime.datetime(2021, 1, 1))], (6.958729, 50.943030)
    )

        stations.get_ds100(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1))
        == 'TT'
    )
    assert (
        stations.get_ds100(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)) == 'KK'
    )

    ds100s = stations.get_ds100(
        eva=(8000141, 8000207),
        date=(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)),
    )

    assert isinstance(ds100s, pd.Series)
    assert 'ds100' == ds100s.name
    assert ['eva', 'date'] == ds100s.index.names
    assert ds100s.dtype == pd.StringDtype()
    assert ds100s[(8000141, datetime.datetime(2021, 1, 1))] == 'TT'
    assert ds100s[(8000207, datetime.datetime(2021, 1, 1))] == 'KK'

    ds100s = stations.get_ds100(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(ds100s, pd.Series)
    assert 'ds100' == ds100s.name
    assert ['name', 'date'] == ds100s.index.names
    assert ds100s.dtype == pd.StringDtype()
    assert ds100s[('Tübingen Hbf', datetime.datetime(2021, 1, 1))] == 'TT'
    assert ds100s[('Köln Hbf', datetime.datetime(2021, 1, 1))] == 'KK'


def test_get_location():
    np.testing.assert_allclose(
        stations.get_location(name='Tübingen Hbf', date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(name='Köln Hbf', date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    np.testing.assert_allclose(
        stations.get_location(eva=8000141, date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(eva=8000207, date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    np.testing.assert_allclose(
        stations.get_location(ds100='TT', date=datetime.datetime(2021, 1, 1)),
        (9.055407, 48.515811),
    )

    np.testing.assert_allclose(
        stations.get_location(ds100='KK', date=datetime.datetime(2021, 1, 1)),
        (6.958729, 50.943030),
    )

    locations = stations.get_location(
        name=pd.Series(['Tübingen Hbf', 'Köln Hbf']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['name', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[('Tübingen Hbf', datetime.datetime(2021, 1, 1))],
        (9.055407, 48.515811),
    )
    np.testing.assert_allclose(
        locations.loc[('Köln Hbf', datetime.datetime(2021, 1, 1))],
        (6.958729, 50.943030),
    )

    locations = stations.get_location(
        eva=pd.Series([8000141, 8000207]),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['eva', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[(8000141, datetime.datetime(2021, 1, 1))], (9.055407, 48.515811)
    )
    np.testing.assert_allclose(
        locations.loc[(8000207, datetime.datetime(2021, 1, 1))], (6.958729, 50.943030)
    )

    locations = stations.get_location(
        ds100=pd.Series(['TT', 'KK']),
        date=pd.Series([datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 1)]),
    )

    assert isinstance(locations, pd.DataFrame)
    assert 'lat' in locations.columns
    assert 'lon' in locations.columns
    assert ['ds100', 'date'] == locations.index.names
    assert locations['lat'].dtype == np.float64
    assert locations['lon'].dtype == np.float64
    np.testing.assert_allclose(
        locations.loc[('TT', datetime.datetime(2021, 1, 1))], (9.055407, 48.515811)
    )
    np.testing.assert_allclose(
        locations.loc[('KK', datetime.datetime(2021, 1, 1))], (6.958729, 50.943030)
    )
