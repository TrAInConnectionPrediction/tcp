import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Optional, Callable
import pandas as pd
from database import DB_CONNECT_STRING
from config import CACHE_PATH


def cached_table_fetch(
        tablename: str,
        use_cache: Optional[bool] = True,
        prefer_cache: Optional[bool] = False,
        generate: Optional[bool] = False,
        table_generator: Optional[Callable[[], pd.DataFrame]] = None,
        push: Optional[bool] = True,
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


def pd_to_psql(df, uri, table_name, schema_name=None, if_exists='fail', sep=','):
    """
    Load pandas dataframe into a sql table using native postgres COPY FROM.
    Args:
        df (dataframe): pandas dataframe
        uri (str): postgres psycopg2 sqlalchemy database uri
        table_name (str): table to store data in
        schema_name (str): name of schema in db to write to
        if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
        sep (str): separator for temp file, eg ',' or '\t'
    Returns:
        bool: True if loader finished
    """

    if 'psycopg2' not in uri:
        raise ValueError(
            'need to use psycopg2 uri eg postgresql+psycopg2://psqlusr:psqlpwdpsqlpwd@localhost/psqltest. install with `pip install psycopg2-binary`')
    table_name = table_name.lower()
    if schema_name:
        schema_name = schema_name.lower()

    import sqlalchemy
    import io

    if schema_name is not None:
        sql_engine = sqlalchemy.create_engine(uri, connect_args={'options': '-csearch_path={}'.format(schema_name)})
    else:
        sql_engine = sqlalchemy.create_engine(uri)
    sql_cnxn = sql_engine.raw_connection()
    cursor = sql_cnxn.cursor()

    df[:0].to_sql(table_name, sql_engine, schema=schema_name, if_exists=if_exists, index=False)

    fbuf = io.StringIO()
    df.to_csv(fbuf, index=False, header=False, sep=sep)
    fbuf.seek(0)
    cursor.copy_from(fbuf, table_name, sep=sep, null='')
    sql_cnxn.commit()
    cursor.close()

    return True


def cached_table_push(df: pd.DataFrame, tablename: str, fast: bool = True, **kwargs):
    """
    Save df to local cache file and replace the table in the database.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to push
    tablename : str
        Name of the table in the database
    fast : bool, optional
        Whether to use a faster push method or not, by default False
        True: use the fast method, which might not be as accurate
        False: use the slow method, which is more accurate
    """
    cache_path = CACHE_PATH + '/' + tablename + '.pkl'
    df.to_pickle(cache_path)
    # d6stack is way faster than pandas at inserting data to sql.
    # It exports the dataframe to a csv and then inserts it to the database.
    if fast:
        pd_to_psql(df, DB_CONNECT_STRING, tablename, if_exists='replace')
    else:
        df.to_sql(tablename, DB_CONNECT_STRING, if_exists='replace', method='multi', chunksize=10_000, **kwargs)
