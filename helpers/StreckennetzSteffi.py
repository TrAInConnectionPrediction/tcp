import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import networkx as nx
import functools
import geopy.distance
from helpers.StationPhillip import StationPhillip

logger = logging.getLogger("webserver." + __name__)

class StreckennetzSteffi(StationPhillip):
    def __init__(self, prefer_cache=False):
        super().__init__()
        self._CACHE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) \
                            + '/cache/streckennetz_cache'
        self.using_cache = False
        if prefer_cache:
            try:
                streckennetz_df = pd.read_pickle(self._CACHE_PATH)
                self.using_cache = True
                print('Using streckennetz cache')
            except FileNotFoundError:
                pass

        if not self.using_cache:
            try:
                from database.engine import engine
                streckennetz_df = pd.read_sql('SELECT u, v, length FROM minimal_streckennetz', con=engine)
                streckennetz_df.to_pickle(self._CACHE_PATH)
            except:
                try:
                    streckennetz_df = pd.read_pickle(self._CACHE_PATH)
                    print('Using streckennetz cache')
                except FileNotFoundError:
                    raise FileNotFoundError('There is no connection to the database and no cache of it')

        self.streckennetz = nx.from_pandas_edgelist(streckennetz_df, source='u', target='v', edge_attr=True)

        logger.info("Done")

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
        return sum(self.distance(waypoints[i], waypoints[i + 1]) for i in range(len(waypoints) - 1))

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

    @functools.lru_cache(maxsize=8000)
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
        if u in self.streckennetz and v in self.streckennetz:
            return nx.shortest_path_length(self.streckennetz, u, v, weight='length')
        else:
            u_coords = self.get_location(name=u)
            v_coords = self.get_location(name=v)
            return geopy.distance.distance(u_coords, v_coords).meters


if __name__ == "__main__":
    import fancy_print_tcp
    streckennetz_steffi = StreckennetzSteffi()
    print(streckennetz_steffi.route_length(['Tübingen Hbf', 'Stuttgart Hbf', 'Paris Est']))
