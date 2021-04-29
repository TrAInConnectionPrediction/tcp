import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import functools
import geopy.distance
from helpers import StationPhillip
from database import cached_table_fetch
import igraph


class StreckennetzSteffi(StationPhillip):
    def __init__(self, **kwargs):
        super().__init__()

        streckennetz_df = cached_table_fetch('minimal_streckennetz', **kwargs)

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
                length += self.distance(self.get_name(eva=waypoints[i]),
                                        self.get_name(eva=waypoints[i + 1]))
            except KeyError:
                pass
        return length

    @functools.lru_cache(maxsize=1000)
    def get_edge_path(self, source, target):
        try:
            return self.streckennetz_igraph.get_shortest_paths(
                source, target, weights='length', output='epath'
            )[0]
        except ValueError:
            return None

    @functools.lru_cache(maxsize=800)
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
        path = self.get_edge_path(u, v)
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
