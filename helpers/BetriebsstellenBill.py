import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
from typing import Optional, Union, Any, Tuple
from database.cached_table_fetch import cached_table_fetch


class NoLocationError(Exception):
    pass


class BetriebsstellenBill:
    def __init__(self, **kwargs):
        self.name_index_betriebsstellen = cached_table_fetch('betriebstellen', index_col='name', **kwargs)

        self.ds100_index_betriebsstellen = self.name_index_betriebsstellen.reset_index().set_index('ds100')
        self.betriebsstellen_list = self.name_index_betriebsstellen.dropna(subset=['lat', 'lon']).index.to_list()
        self.NoLocationError = NoLocationError

    def __len__(self):
        return len(self.name_index_betriebsstellen)

    def __iter__(self):
        """
        Iterate over Betriebsstellen names

        Yields
        -------
        str
            Name of Betriebsstelle
        """
        yield from self.name_index_betriebsstellen.index

    def to_gdf(self):
        """
        Convert stations to geopandas GeoDataFrame.

        Returns
        -------
        geopandas.DateFrame
            Betriebsstellen with coordinates as geometry for geopandas.GeoDataFrame.
        """
        import geopandas as gpd
        # Not all of the betriebsstellen have geo information. A GeoDataFrame without geo
        # is kind of useless, so we drop these betriebsstellen
        betriebsstellen_with_location = self.name_index_betriebsstellen.dropna(subset=['lon', 'lat'])
        return gpd.GeoDataFrame(
            betriebsstellen_with_location, 
            geometry=gpd.points_from_xy(betriebsstellen_with_location.lon, betriebsstellen_with_location.lat)
        ).set_crs("EPSG:4326")

    def get_name(self, ds100: Union[str, Any]) -> Union[str, pd.Series]:
        """Get the name of a Betriebsstelle or multiple Betriebsstellen

        Parameters
        ----------
        ds100 : str | ArrayLike[str]
            The ds100 or ds100ths to get the name or names from, by default None

        Returns
        -------
        str | pd.Series
            str - the single name matching ds100
            pd.Series: Series with the names matching ds100ths. Contains NaNs
            if no name was found for a given ds100.
        """
        if isinstance(ds100, str):
            return self.ds100_index_betriebsstellen.at[ds100, 'name']
        else:
            names = pd.Series(index=ds100, name='name', dtype=str)
            names.loc[:] = self.ds100_index_betriebsstellen.loc[:, 'name']
            return names

    def get_ds100(self, name: Union[str, Any]) -> Union[str, pd.Series]:
        if isinstance(name, str):
            return self.name_index_betriebsstellen.at[name, 'ds100']
        else:
            ds100ths = pd.Series(index=name, name='ds100', dtype=str)
            ds100ths.loc[:] = self.name_index_betriebsstellen.loc[:, 'ds100']
            return ds100ths

    def get_location(
        self,
        name: Optional[Union[str, Any]]=None,
        ds100: Optional[Union[str, Any]]=None
    ) -> Union[Tuple[float, float], pd.DataFrame]:
        """
        Get the location (lon, lat) of a Betriebsstelle or multiple Betriebsstellen

        Parameters
        ----------
        name : str | ArrayLike[str], optional
            Name or names of the Betriebsstelle or Betriebsstellen, by default None
        ds100 : str | ArrayLike[str], optional
            ds100 or ds100ths of the Betriebsstelle or Betriebsstellen, by default None

        Returns
        -------
        Tuple[float, float] | pd.DataFrame
            Tuple[float, float]: longitude and latitude of the Betriebsstelle
            pd.DataFrame: DataFrame with a lon and a lat column with the
            location of the Betriebsstellen
        """
        if name is None:
            return self.get_location(name=self.get_name(ds100=ds100))
        else:
            if isinstance(name, str):
                lon = self.name_index_betriebsstellen.at[name, 'lon']
                lat = self.name_index_betriebsstellen.at[name, 'lat']
                if type(lon) == np.ndarray:
                    lon = lon[0]
                if type(lat) == np.ndarray:
                    lat = lat[0]
                if not lon or not lat:
                    raise self.NoLocationError
                else:
                    return (lon, lat)
            else:
                locations = pd.DataFrame(index=name, columns=['lon', 'lat'])
                locations.loc[:, ['lon', 'lat']] = self.name_index_betriebsstellen.loc[:, ['lon', 'lat']]
                return locations


if __name__ == "__main__":
    betriebsstellen = BetriebsstellenBill()
    print('len:', len(betriebsstellen))
