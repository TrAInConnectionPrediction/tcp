import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import functools
import geopy.distance
from helpers import StationPhillip
from database import cached_table_fetch, cached_table_push, get_engine
import igraph
import pangres
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, BIGINT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
import random
import time


Base = declarative_base()

class EdgePathPersistantCache(Base):
    __tablename__ = 'edge_path_persistant_cache'
    index = Column(sqlalchemy.types.String, primary_key=True, autoincrement=False)
    path = Column(postgresql.ARRAY(sqlalchemy.types.INT))


class StreckennetzSteffi(StationPhillip):
    def __init__(self, **kwargs):
        if 'generate' in kwargs:
            kwargs['generate'] = False
            print('StreckennetzSteffi does not support generate')

        super().__init__(**kwargs)

        streckennetz_df = cached_table_fetch('minimal_streckennetz', **kwargs)

        try:
            engine = get_engine()
            Base.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f'database.{EdgePathPersistantCache.__tablename__} running offline!')

        self.persistent_path_cache = cached_table_fetch('edge_path_persistant_cache', index_col='index')['path'].to_dict()

        tuples = [tuple(x) for x in streckennetz_df[['u', 'v', 'length']].values]
        self.streckennetz_igraph = igraph.Graph.TupleList(tuples, directed=False, edge_attrs=['length'])

        self.get_length = lambda edge: self.streckennetz_igraph.es[edge]['length']

    def route_length(self, waypoints) -> float:
        """
        Calculate approximate length of a route, e.g. the sum of the distances between the waypoints.

        Parameters
        ----------
        waypoints: list
            List of station names that describe the route.

        Returns
        -------
        float:
            Length of route.

        """
        length = 0
        for i in range(len(waypoints) - 1):
            try:
                length += self.distance(waypoints[i], waypoints[i + 1])
            except KeyError:
                pass
        return length
    
    def eva_route_length(self, waypoints) -> float:
        """
        Calculate approximate length of a route, e.g. the sum of the distances between the waypoints.

        Parameters
        ----------
        waypoints: list
            List of station evas that describe the route.

        Returns
        -------
        float:
            Length of route.

        """
        length = 0
        for i in range(len(waypoints) - 1):
            try:
                length += self.distance(*sorted(
                    self.get_name(eva=waypoints[i]), self.get_name(eva=waypoints[i + 1])
                ))
            except KeyError:
                pass
        return length

    @functools.lru_cache(maxsize=None)
    def get_edge_path(self, source, target):
        try:
            return self.streckennetz_igraph.get_shortest_paths(
                source, target, weights='length', output='epath'
            )[0]
        except ValueError:
            return None

    def get_edge_path_persistant_cache(self, source, target):
        source, target = sorted((source, target))
        key = source + '__' + target
        result = self.persistent_path_cache.get(key, -1)
        if result == -1:
            result = self.get_edge_path(source, target)
            self.persistent_path_cache[key] = result
        return result

    def store_edge_path_persistant_cache(self, engine: sqlalchemy.engine):
        cache_df = pd.DataFrame.from_dict({key: [self.persistent_path_cache[key]] for key in self.persistent_path_cache}, orient='index', columns=['path'])
        cache_df.index.rename('index', inplace=True)
        while True:
            # This might be executed in parallel several times and that
            # might cause deadlock errors with the postgres database
            try:
                pangres.upsert(
                    engine,
                    cache_df,
                    if_row_exists='update',
                    table_name='edge_path_persistant_cache',
                    dtype={'path':postgresql.ARRAY(sqlalchemy.types.INT)},
                    create_schema=False,
                    add_new_columns=False,
                    adapt_dtype_of_empty_db_columns=False
                )
                break
            except sqlalchemy.exc.OperationalError:
                # Try again after random delay
                time.sleep(random.randint(0, 20))
        self.persistent_path_cache = cached_table_fetch('edge_path_persistant_cache', index_col='index')['path'].to_dict()
        # db_version = cached_table_fetch('edge_path_persistant_cache', index_col='index')
        # cache_df = pd.concat([cache_df, db_version], ignore_index=False)
        # cache_df = cache_df.loc[~cache_df.index.duplicated(keep='first'), :]
        # cached_table_push(cache_df, 'edge_path_persistant_cache', dtype={'path':postgresql.ARRAY(sqlalchemy.types.INT)})


    @functools.lru_cache(maxsize=None)
    def distance(self, u: str, v: str) -> float:
        """
        Calculate approx distance between two stations. Uses the Streckennetz if u and v are part of it,
        otherwise it usese geopy.distance.distance.

        Parameters
        ----------
        u: str
            Station name
        v: str
            Station name

        Returns
        -------
        float:
            Distance in meters between u and v.
        """
        # path = self.get_edge_path(u, v)
        path = self.get_edge_path_persistant_cache(u, v)
        if path is not None:
            return sum(map(self.get_length, path))
        else:
            try:
                u_coords = self.get_location(name=u)
                v_coords = self.get_location(name=v)
                return geopy.distance.distance(u_coords, v_coords).meters
            except KeyError:
                return 0


if __name__ == "__main__":
    import helpers.fancy_print_tcp
    streckennetz_steffi = StreckennetzSteffi(prefer_cache=False)

    print(streckennetz_steffi.route_length(['Tübingen Hbf', 'Altingen(Württ)', 'Stuttgart Hbf', 'Paris Est']))

    streckennetz_steffi.store_edge_path_persistant_cache()
