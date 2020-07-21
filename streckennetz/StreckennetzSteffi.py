import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd 
import networkx as nx 
from config import db_database, db_password, db_server, db_username
import sqlalchemy

class StreckennetzSteffi:
    class NoMappingFoundError(Exception):
        pass

    def __init__(self, notebook=False):
        try:
            self.engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require')
            streckennetz_df = pd.read_sql('SELECT source, target, distance FROM streckennetz', con=self.engine)
            self.mappings = pd.read_sql('SELECT * FROM streckennetz_mappings', con=self.engine).set_index('name')
            if notebook:
                    streckennetz_df.to_pickle('../data_buffer/streckennetz_offline_buffer')
                    self.mappings.to_pickle('../data_buffer/mappings_offline_buffer')
            else:
                streckennetz_df.to_pickle('data_buffer/streckennetz_offline_buffer')
                self.mappings.to_pickle('data_buffer/mappings_offline_buffer')
        except:
            try:
                if notebook:
                    streckennetz_df = pd.read_pickle('../data_buffer/streckennetz_offline_buffer')
                    self.mappings = pd.read_pickle('../data_buffer/mappings_offline_buffer')
                else:
                    streckennetz_df = pd.read_pickle('data_buffer/streckennetz_offline_buffer')
                    self.mappings = pd.read_pickle('data_buffer/mappings_offline_buffer')
                print('Using local streckennetz buffers')
            except FileNotFoundError:
                raise FileNotFoundError('There is no connection to the database and no local buffer')

        self.streckennetz = nx.from_pandas_edgelist(streckennetz_df, source='source', target='target', edge_attr=True)
        

    def load_atributes(self):
        streckennetz_df = pd.read_sql('SELECT * FROM streckennetz', con=self.engine)
        self.streckennetz = nx.from_pandas_edgelist(streckennetz_df, source='', target='', edge_attr=True)

    def route_distance(self, waypoints) -> float:
        """calculate approx length of a route, e.g. the sum of the distances between the waypoints

        Args:
            waypoints (list): list of station names that describe the route

        Returns:
            float: lenght of route
        """
        distance = 0
        for i in range(len(waypoints) - 1):
            distance += self.distance(waypoints[i], waypoints[i+1])
        return distance


    def distance(self, source, target) -> float:
        """calculate approx distance between two stations

        Args:
            source (string): station
            target (string): station

        Raises:
            self.NoMappingFoundError: there is no mapping for the source
            self.NoMappingFoundError: there is no mapping for the target

        Returns:
            float: distance between source and target
        """
        try:
            source = self.mappings.loc[source, 'match']
        except KeyError:
            raise self.NoMappingFoundError('No mapping for: ' + source)

        try:
            target = self.mappings.loc[target, 'match']
        except KeyError:
            raise self.NoMappingFoundError('No mapping for: ' + target)

            
        return self.distance_streckennetz_names(source, target)
    
    def distance_streckennetz_names(self, source, target) -> float:
        """calculate approx distance between to nodes of the streckennetz

        Args:
            source (string): source node
            target (string): target node

        Returns:
            float: distance between source and target node
        """
        return(nx.shortest_path_length(self.streckennetz, source, target, weight='distance'))

if __name__ == "__main__":
    streckennetz_steffi = StreckennetzSteffi()
    print(streckennetz_steffi.route_distance(['Tübingen Hbf', 'Herrenberg', 'Büsum (Hafen)']))