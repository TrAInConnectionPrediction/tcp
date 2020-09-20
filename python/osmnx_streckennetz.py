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
from helpers.StationPhillip import StationPhillip
import matplotlib.pyplot as plt


def get_streckennetz_from_osm():
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
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) * 1e6).round(decimals=3))


def to_geographic_coordinate_system(line):
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) / 1e6))


def find_edges_to_split(bhf: str, streckennetz, nodes, edges):
    stations = StationPhillip()
    original_coords = np.array(stations.get_location(name=bhf))
    coords = original_coords.copy()

    closest_edge_index = ox.get_nearest_edge(streckennetz, np.flip(original_coords))
    closest_edge = streckennetz[closest_edge_index[0]][closest_edge_index[1]][closest_edge_index[2]]
    coords = coords * 1e6
    points = shapely.ops.nearest_points(Point(coords), reduce_decimal_precision(closest_edge['geometry']))

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
    return sum((geopy.distance.distance(p1, p2).meters for p1, p2 in pairwise(tuple(zip(line.xy[0], line.xy[1])))))


def split_edges(to_split: list):
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


if __name__ == '__main__':
    stations = StationPhillip()

    ox.config(log_console=False, use_cache=True)
    streckennetz = get_streckennetz_from_osm()
    streckennetz = nx.read_gpickle("original_osm_rail_graph.gpickle")
    s_nodes, s_edges = ox.graph_to_gdfs(streckennetz)
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
