import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import functools
import geopy.distance
from helpers import StationPhillip
from database import cached_table_fetch, cached_table_push, get_engine
import igraph
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, String, INT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
import random
import time


Base = declarative_base()


class EdgePathPersistantCache(Base):
    __tablename__ = "edge_path_persistent_cache"
    index = Column(String, primary_key=True, autoincrement=False)
    path = Column(postgresql.ARRAY(INT))

    def __init__(self) -> None:
        try:
            engine = get_engine()
            Base.metadata.create_all(engine)
            engine.dispose()
        except sqlalchemy.exc.OperationalError:
            print(f"database.{EdgePathPersistantCache.__tablename__} running offline!")


class StreckennetzSteffi(StationPhillip):
    def __init__(self, **kwargs):
        if "generate" in kwargs:
            kwargs["generate"] = False
            print("StreckennetzSteffi does not support generate")

        self.kwargs = kwargs

        super().__init__(**kwargs)

        streckennetz_df = cached_table_fetch("minimal_streckennetz", **kwargs)

        # Lazy loading of persistent cache, as many programs will not use it
        self.persistent_path_cache = None
        self.original_persistent_path_cache_len = None

        nodes = list(set(streckennetz_df['u'].to_list() + streckennetz_df['v'].to_list()))
        node_ids = dict(zip(nodes, range(len(nodes))))
        edges = list(zip(streckennetz_df["u"].map(node_ids.get).to_list(), streckennetz_df["v"].map(node_ids.get).to_list()))
        self.streckennetz_igraph = igraph.Graph(
            n=len(nodes),
            edges=edges,
            directed=False,
            vertex_attrs={'name': nodes},
            edge_attrs={'length': streckennetz_df['length'].to_list()},
        )

        self.get_length = lambda edge: self.streckennetz_igraph.es[edge]["length"]

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
                length += self.distance(
                    *sorted(
                        self.get_name(eva=waypoints[i]),
                        self.get_name(eva=waypoints[i + 1]),
                    )
                )
            except KeyError:
                pass
        return length

    @functools.lru_cache(maxsize=None)
    def get_edge_path(self, source, target):
        try:
            return self.streckennetz_igraph.get_shortest_paths(
                source, target, weights="length", output="epath"
            )[0]
        except ValueError:
            return None

    def get_edge_path_persistent_cache(self, source, target):
        # Lazy loading of persistent cache, as many programs will not use it
        if self.persistent_path_cache is None:
            self.persistent_path_cache = cached_table_fetch(
                "edge_path_persistent_cache", index_col="index", **self.kwargs
            )["path"].to_dict()
            self.original_persistent_path_cache_len = len(self.persistent_path_cache)

        source, target = sorted((source, target))
        key = source + "__" + target

        result = self.persistent_path_cache.get(key, -1)
        if result == -1:
            result = self.get_edge_path(source, target)
            self.persistent_path_cache[key] = result
        return result

    def store_edge_path_persistent_cache(self, engine: sqlalchemy.engine):
        # Store persistent path cache if 1000 new paths were added and it's worth it
        n_new_paths = len(self.persistent_path_cache) - self.original_persistent_path_cache_len
        if n_new_paths > 1000:
            print("uploading", n_new_paths, "new rows")
            local_cache_df = pd.DataFrame.from_dict(
                {
                    key: [self.persistent_path_cache[key]]
                    for key in self.persistent_path_cache
                },
                orient="index",
                columns=["path"],
            )
            local_cache_df.index.rename("index", inplace=True)
            while True:
                try:
                    db_cache_df = cached_table_fetch("edge_path_persistent_cache", index_col="index")
                    break
                except Exception as e:
                    delay = random.randint(0, 300)
                    print("Failed to fetch db. Wating", delay, "seconds.")
                    print('Failed with exception:')
                    print(e)
                    time.sleep(delay)

            cache_df = pd.concat([local_cache_df, db_cache_df])
            cache_df = cache_df.loc[~cache_df.index.duplicated(), :]
            retry = True
            while retry:
                try:
                    cached_table_push(cache_df, tablename="edge_path_persistent_cache", fast=False, dtype={"path": postgresql.ARRAY(sqlalchemy.types.INT)})
                    retry = False
                except sqlalchemy.exc.ProgrammingError:
                    # Try again after random delay
                    delay = random.randint(0, 300)
                    print("Failed to add new paths to db. Wating", delay, "seconds.")
                    time.sleep(delay)

            self.persistent_path_cache = cache_df["path"].to_dict()
            self.original_persistent_path_cache_len = len(self.persistent_path_cache)

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
        path = self.get_edge_path_persistent_cache(u, v)
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

    streckennetz_steffi = StreckennetzSteffi(prefer_cache=True)

    print("Tübingen Hbf - Altingen(Württ):", streckennetz_steffi.route_length(["Tübingen Hbf", "Altingen(Württ)"]))
    print("Tübingen Hbf - Reutlingen Hbf:", streckennetz_steffi.route_length(["Tübingen Hbf", "Reutlingen Hbf"]))
    print("Tübingen Hbf - Stuttgart Hbf:", streckennetz_steffi.route_length(["Tübingen Hbf", "Stuttgart Hbf"]))
    print("Tübingen Hbf - Ulm Hbf:", streckennetz_steffi.route_length(["Tübingen Hbf", "Ulm Hbf"]))
