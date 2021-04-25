import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from database.engine import DB_CONNECT_STRING
from config import CACHE_PATH

def cached_table_fetch(tablename: str, use_cache: bool=True, prefer_cache: bool=False, **kwargs) -> pd.DataFrame:  
    """
    Fetch table from database and create a local cache of it

    Parameters
    ----------
    tablename : str
        Name of the sql-table to fetch
    use_cache : bool, optional
        Whether to try to load the cache or not, by default True
    prefer_cache : bool, optional
        Whether to try to only load the cache and not ping the database. Usfull for big tables, by default False

    Returns
    -------
    pd.DataFrame
        DataFrame containing the feched table

    Raises
    ------
    FileNotFoundError
        The Database is not reachable and there was no local cache found
    """
    cache_path = CACHE_PATH + '/' + tablename + '.pkl'
    if prefer_cache:
        try:
            return pd.read_pickle(cache_path)
        except FileNotFoundError:
            pass

    try:
        table_df = pd.read_sql_table(tablename, DB_CONNECT_STRING, **kwargs)
        if use_cache:
            table_df.to_pickle(cache_path)
        return table_df
    except Exception as ex:
        try:
            if not use_cache:
                raise FileNotFoundError
            return pd.read_pickle(cache_path)
        except FileNotFoundError:
            print(ex)
            raise FileNotFoundError(f'There is no connection to the database and no cache of {tablename}')


def cached_table_push(df: pd.DataFrame, tablename: str, **kwargs):
    """
    Save df to local cache file and replace the table in the database.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to push
    tablename : str
        Name of the table in the database
    """
    cache_path = CACHE_PATH + '/' + tablename + '.pkl'
    df.to_pickle(cache_path)
    df.to_sql(tablename, DB_CONNECT_STRING, if_exists='replace', method='multi', **kwargs)