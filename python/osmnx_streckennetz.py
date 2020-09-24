import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from multiprocessing import Pool
import osmnx as ox
import networkx as nx
import shapely
from shapely.geometry import Point, LineString
import numpy as np
import geopy.distance
import tqdm
import itertools
from functools import partial
import pandas as pd
from helpers.StationPhillip import StationPhillip
import matplotlib.pyplot as plt


def get_streckennetz_from_osm():
    """
    Load Streckennetz from OpenStreetMap.

    Returns
    -------
    nx.Graph:
        Streckennetz without stations.
    """
    ox.config(log_console=True, use_cache=True)
    rail_filter = '["railway"~"rail|tram|narrow_gauge|light_rail"]'
    streckennetz = ox.graph_from_place('Germany',
                                       simplify=True,
                                       network_type='none',
                                       custom_filter=rail_filter
                                       )
    nx.write_gpickle(streckennetz, "original_osm_rail_graph.gpickle")
    return streckennetz


def reduce_decimal_precision(line):
    """Multiply each point in line by 1e6 and round it to 3 decimal places."""
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) * 1e6).round(decimals=3))


def to_geographic_coordinate_system(line):
    """Inverse of reduce_decimal_precision but without rounding."""
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) / 1e6))


def find_edges_to_split(bhf: str, streckennetz, nodes, edges):
    """
    Find edges that are part of a station by setting a cross at the coordinates of a station and finding intersections
    with Streckennetz edges.

    Parameters
    ----------
    bhf: str
        Station to find.
    streckennetz: nx.Graph
        Graph of Streckennetz to find the station in.
    nodes: gpd.GeoDataFrame
        Nodes of Streckennetz.
    edges: gpd.GeoDataFrame
        Edges of Streckennetz.

    Returns
    -------
    list:
        List of edges to split.

    Notes
    -----
    This function need a change in osmnx/distance.py:
def get_nearest_edge(G, point, return_geom=False, return_dist=False, gdf_edges=None):
    if gdf_edges is None:
        # get u, v, key, geom from all the graph edges
        gdf_edges = utils_graph.graph_to_gdfs(G, nodes=False, fill_edge_geometry=True)
    """
    stations = StationPhillip()
    original_coords = np.array(stations.get_location(name=bhf))
    coords = original_coords.copy()

    closest_edge = ox.get_nearest_edge(streckennetz, np.flip(original_coords), gdf_edges=edges, return_geom=True)
    closest_geom = closest_edge[3]
    coords = coords * 1e6
    try:
        points = shapely.ops.nearest_points(Point(coords), reduce_decimal_precision(closest_geom))
    except:
        return []

    bhf_vec = (np.array([points[0].x - points[1].x,
                         points[0].y - points[1].y]) * 1e6).round(decimals=3)
    bhf_vec = bhf_vec * 1e6 / np.sqrt(bhf_vec.dot(bhf_vec))
    line1 = LineString([coords - (0.001 * bhf_vec),
                        coords + (0.001 * bhf_vec)])

    bhf_orth_vec = np.array([bhf_vec[1], -bhf_vec[0]])
    line2 = LineString([coords - (0.001 * bhf_orth_vec),
                        coords + (0.001 * bhf_orth_vec)])

    close_edges = edges.cx[original_coords[0] - 0.01:original_coords[0] + 0.01,
                           original_coords[1] - 0.01:original_coords[1] + 0.01]

    edges_split = []
    for index, edge in close_edges.iterrows():
        geom = reduce_decimal_precision(edge['geometry'])
        intersection = geom.intersection(line1)
        if intersection:
            edges_split.append((bhf, edge, line1))

        else:
            intersection = geom.intersection(line2)
            if intersection:
                edges_split.append((bhf, edge, line2))

    return edges_split


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def length_of_line(line) -> int:
    """
    Calculate length of line in meters.

    Parameters
    ----------
    line: LineString
        Line (geo points) to calculate length of.

    Returns
    -------
    int:
        Length of line.
    """
    return sum((geopy.distance.distance(p1, p2).meters for p1, p2 in pairwise(tuple(zip(line.xy[0], line.xy[1])))))


def split_edges(to_split: list):
    """
    Use result of find_edges_to_split() to generate new edges
    and nodes and generate those that have to be removed.

    Parameters
    ----------
    to_split: list
        Result of find_edges_to_split().

    Returns
    -------
    (list, list, list)
        List of nodes to add.
        List of edges to remove.
        List of edges to add.

    """
    nodes_to_add = {}
    edges_to_remove = []
    edges_to_add = []
    for bhf, edge, splitter in to_split:
        nodes_to_add[bhf] = {**dict(zip(('x', 'y'), stations.get_location(name='Tübingen Hbf'))), 'bhf': True}
        edges_to_remove.append((edge['u'], edge['v']))
        geom = reduce_decimal_precision(edge['geometry'])
        geoms = shapely.ops.split(geom, splitter)
        geoms = [to_geographic_coordinate_system(geoms[0]),
                 to_geographic_coordinate_system(geoms[1])]
        edges_to_add.append((edge['u'], bhf, {
            'geometry': geoms[0],
            'length': length_of_line(geoms[0]),
            **{key: value for key, value in edge.items() if key not in ('u', 'v', 'length', 'geometry')}
        }))
        edges_to_add.append((bhf, edge['v'], {
            'geometry': geoms[1],
            'length': length_of_line(geoms[1]),
            **{key: value for key, value in edge.items() if key not in ('u', 'v', 'length', 'geometry')}
        }))
    return nodes_to_add.items(), edges_to_remove, edges_to_add


def upload_minimal(streckennetz):
    """
    Upload edges of Streckennetz without attributes except edge length to database.

    Parameters
    ----------
    streckennetz: nx.Graph
        Graph of the Streckennetz
    """
    from database.engine import engine
    streckennetz = ox.graph_to_gdfs(nodes=False)
    streckennetz = pd.DataFrame(streckennetz[['u', 'v', 'length']])
    streckennetz.to_sql('minimal_streckennetz', if_exists='replace', method='multi', con=engine)


if __name__ == '__main__':
    stations = StationPhillip()

    ox.config(log_console=False, use_cache=True)
    # streckennetz = get_streckennetz_from_osm()
    streckennetz = nx.read_gpickle("original_osm_rail_graph.gpickle")
    s_nodes, s_edges = ox.graph_to_gdfs(streckennetz, fill_edge_geometry=True)
    process_find_edges_to_split = partial(find_edges_to_split,
                                          streckennetz=streckennetz,
                                          nodes=s_nodes,
                                          edges=s_edges)
    with Pool() as p:
        edges_to_split = tuple(
            tqdm.tqdm(p.imap_unordered(process_find_edges_to_split, stations.sta_list),
                      total=len(stations.sta_list)))
    edges_to_split = tuple(itertools.chain(*edges_to_split))
    nodes_to_add, edges_to_remove, edges_to_add = split_edges(edges_to_split)
    streckennetz.add_nodes_from(nodes_to_add)
    streckennetz.remove_edges_from(edges_to_remove)
    streckennetz.add_edges_from(edges_to_add)

    nx.write_gpickle(streckennetz, "osm_rail_graph.gpickle")

    # print(nx.shortest_path_length(G, 'Tübingen Hbf', 'Köln Hbf', weight='length'))
    # ox.plot_graph(G)
