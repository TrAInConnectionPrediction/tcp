import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Optional, Callable
import pandas as pd
from database import DB_CONNECT_STRING
from config import CACHE_PATH

def cached_table_fetch(
    tablename: str,
    use_cache: Optional[bool]=True,
    prefer_cache: Optional[bool]=False,
    generate: Optional[bool]=False,
    table_generator: Optional[Callable[[], pd.DataFrame]]=None,
    push: Optional[bool]=False,
    **kwargs
) -> pd.DataFrame:
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
    generate : bool, optional
        Whether to use table_generator to generate the DataFrame and not look for cache or database, by default False
    table_generator : Callable[[], pd.DataFrame], optional
        Callable that generates the data of table tablename, by default None
    push : bool, optional
        Whether to push data to the db after calling table_generator, by default False

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
    if generate:
        if table_generator is None:
            raise ValueError('Cannot generate if no table_generator was supplied')
        df = table_generator()
        if push:
            cached_table_push(df, tablename)
        return df
        
    if prefer_cache:
        try:
            return pd.read_pickle(cache_path)
        except FileNotFoundError:
            pass

    try:
        df = pd.read_sql_table(tablename, DB_CONNECT_STRING, **kwargs)
        if use_cache:
            try:
                df.to_pickle(cache_path)
            except Exception as ex:
                print('could not write cache file\n', ex)
        return df
    except Exception as ex:
        try:
            if not use_cache:
                raise FileNotFoundError
            return pd.read_pickle(cache_path)
        except FileNotFoundError:
            if table_generator is not None:
                df = table_generator()
                if push:
                    cached_table_push(df, tablename)
                return df
                
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