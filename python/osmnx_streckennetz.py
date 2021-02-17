import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import osmnx as ox
import networkx as nx
import shapely
from shapely.geometry import Point, LineString, Polygon
import numpy as np
import geopy.distance
import itertools
import progressbar
import pandas as pd
import geopandas as gpd
from helpers.StationPhillip import StationPhillip
import matplotlib.pyplot as plt
import pickle
from concurrent.futures import ProcessPoolExecutor
from config import streckennetz_threads


def get_streckennetz_from_osm():
    """
    Load Streckennetz from OpenStreetMap.

    Returns
    -------
    nx.Graph:
        Streckennetz without stations.
    """
    try:
        streckennetz = nx.read_gpickle("cache/original_osm_rail_graph.gpickle")
        print('Using chached streckennetz insted of downloading new one from osm')
    except FileNotFoundError:
        ox.config(log_console=True, use_cache=True)
        rail_filter = '["railway"~"rail|tram|narrow_gauge|light_rail"]'
        streckennetz = ox.graph_from_place('Germany',
                                        simplify=True,
                                        network_type='none',
                                        truncate_by_edge=True,
                                        custom_filter=rail_filter
                                        )
        nx.write_gpickle(streckennetz, "cache/original_osm_rail_graph.gpickle")
    return streckennetz


def reduce_decimal_precision(line):
    """Multiply each point in line by 1e6 and round it to 3 decimal places."""
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) * 1e6).round(decimals=3))


def to_geographic_coordinate_system(line):
    """Inverse of reduce_decimal_precision but without rounding."""
    return LineString((np.array(tuple(zip(line.xy[0], line.xy[1]))) / 1e6))


def find_edges_to_split(bhf: str, nearest_edge: list, streckennetz, edges, stations, plot=False):
    """
    Find edges that are part of a station by setting a cross at the coordinates of a station and finding intersections
    with Streckennetz edges.

    Parameters
    ----------
    bhf: str
        Station to find.
    streckennetz: nx.Graph
        Graph of Streckennetz to find the station in.
    edges: gpd.GeoDataFrame
        Edges of Streckennetz.
    stations: StationPhillip
        Instace of StationPhillip.

    Returns
    -------
    list:
        List of edges to split.
    """
    original_coords = np.array(stations.get_location(name=bhf))
    coords = original_coords.copy()

    closest_edge = streckennetz[nearest_edge[0]][nearest_edge[1]][nearest_edge[2]]
    closest_geom = closest_edge['geometry']
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

    # Get close edges using spatial indexing (this is way faster than using GeoDataFram.cx[])
    bbox = shapely.geometry.box(original_coords[0] - 0.001, original_coords[1] - 0.001,
                                original_coords[0] + 0.001, original_coords[1] + 0.001)
    join_gdf = gpd.GeoDataFrame({'geometry': [bbox]}, crs=edges.crs)
    close_edges = edges.loc[gpd.sjoin(join_gdf, edges, how='inner')['index_right0'].unique()]

    if plot:
        # Plot Station
        fig, ax = plt.subplots(figsize=(50,50))
        ax.set_aspect('equal', 'datalim')
        
        for index, edge in close_edges.iterrows():
            geom = reduce_decimal_precision(edge['geometry'])
            # xs, ys = geom.exterior.xy
            # ax.fill(xs, ys, alpha=1, fc='r', ec='none')
            ax.plot(*geom.xy, color='black')
        
        ax.plot(*line1.xy, color='red')
        ax.plot(*line2.xy, color='red')
        # ax.scatter(*coords)
        ax.scatter(*points[0].xy, color='blue')
        ax.scatter(*points[1].xy, color='blue')

        plt.show()

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
    if line is not None:
        return sum((geopy.distance.distance(p1, p2).meters for p1, p2 in pairwise(tuple(zip(line.xy[0], line.xy[1])))))
    else:
        return None


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
    nodes_to_add = []
    edges_to_remove = []
    edges_to_add = []
    bhfs_added = []
    for bhf, edge, splitter in to_split:
        if not bhf in bhfs_added:
            nodes_to_add.append([bhf, {**dict(zip(('x', 'y'), stations.get_location(name=bhf))), 'bhf': True}])
            bhfs_added.append(bhf)
        edges_to_remove.append((edge['u'], edge['v']))
        geom = reduce_decimal_precision(edge['geometry'])
        geoms = shapely.ops.split(geom, splitter)
        if len(geoms) < 2:
            pass
            # print('len(geoms) < 2:', bhf, edge['u'], edge['v'])
        elif len(geoms) > 2:
            pass
            # print('len(geoms) > 2:', bhf, edge['u'], edge['v'])
        else:
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
    return (nodes_to_add, edges_to_remove, edges_to_add)


def upload_minimal(streckennetz):
    """
    Upload edges of Streckennetz without attributes except edge length to database.

    Parameters
    ----------
    streckennetz: nx.Graph
        Graph of the Streckennetz
    """
    from database.engine import engine
    streckennetz = ox.graph_to_gdfs(streckennetz, nodes=False)
    u_v_key = pd.DataFrame(streckennetz.index.to_numpy().tolist(), columns=streckennetz.index.names)
    u_v_key = u_v_key.set_index(['u', 'v', 'key'], drop=False)
    streckennetz['u'] = u_v_key['u']
    streckennetz['v'] = u_v_key['v']
    streckennetz = pd.DataFrame(streckennetz[['u', 'v', 'length']])
    streckennetz = streckennetz.reset_index(drop=True)
    streckennetz.to_sql('minimal_streckennetz', if_exists='replace', method='multi', con=engine)


def upload_full(streckennetz):
    """
    Upload edges of Streckennetz

    Parameters
    ----------
    streckennetz: nx.Graph
        Graph of the Streckennetz
    """
    from database.engine import engine
    streckennetz_nodes, streckennetz_edges = ox.graph_to_gdfs(streckennetz, nodes=True)
    # Function to generate WKB hex
    def wkb_hexer(line):
        return line.wkb_hex

    # Convert `'geom'` column in GeoDataFrame `gdf` to hex
    # Note that following this step, the GeoDataFrame is just a regular DataFrame
    # because it does not have a geometry column anymore. Also note that
    # it is assumed the `'geom'` column is correctly datatyped.
    streckennetz_edges['geometry'] = streckennetz_edges['geometry'].apply(wkb_hexer)
    streckennetz_edges.to_sql('full_streckennetz', if_exists='replace', method='multi', con=engine)

    streckennetz_nodes['geometry'] = streckennetz_nodes['geometry'].apply(wkb_hexer)
    streckennetz_nodes.to_sql('full_streckennetz_nodes', if_exists='replace', method='multi', con=engine)


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    stations = StationPhillip()

    ox.config(log_console=True, use_cache=True)
    streckennetz = get_streckennetz_from_osm()
    s_nodes, s_edges = ox.graph_to_gdfs(streckennetz, fill_edge_geometry=True)
    u_v_key = pd.DataFrame(s_edges.index.to_numpy().tolist(), columns=s_edges.index.names)
    u_v_key = u_v_key.set_index(['u', 'v', 'key'], drop=False)
    s_edges['u'] = u_v_key['u']
    s_edges['v'] = u_v_key['v']
    streckennetz = ox.graph_from_gdfs(s_nodes, s_edges)
    s_edges.sindex

    # Calculate nearest edge for each station
    print('Calculating nearest edges')
    # Project the coordinates of the stations (using a collection
    # of points makes it faster than projecting it one by one)
    point_collection = LineString([[*stations.get_location(name=station)] for station in stations])
    point_collection = ox.projection.project_geometry(point_collection)[0].xy
    x = point_collection[0]
    y = point_collection[1]

    streckennetz_projected = ox.project_graph(streckennetz)
    nearest_edges = ox.get_nearest_edges(streckennetz_projected, x, y, method='kdtree', dist=50).tolist()
    pickle.dump(nearest_edges, open("cache/streckennetz.pkl", "wb"))
    # nearest_edges = pickle.load(open("cache/streckennetz.pkl", "rb"))

    # Find edges where a station node should be inserted
    edges_to_split = []
    print('Finding edges to split')
    with progressbar.ProgressBar(max_value=len(stations)) as bar:
        for i, station in enumerate(stations):
            # if station == 'Tübingen Hbf':
            edges_to_split.append(find_edges_to_split(station, nearest_edges[i], streckennetz, s_edges, stations, plot=False))
            bar.update(i)

    # Find point in edges where the station nodes are inserted
    edges_to_split = tuple(itertools.chain(*edges_to_split))
    pickle.dump(edges_to_split, open("cache/edges_to_split.pkl", "wb"))
    # edges_to_split = pickle.load(open("cache/edges_to_split.pkl", "rb"))

    print('splitting edges')
    import math
    chunksize = math.ceil(len(edges_to_split) / 8)
    edges_to_split_chunks = [edges_to_split[i:i + chunksize] for i in range(0, len(edges_to_split), chunksize)]
    with ProcessPoolExecutor(max_workers=streckennetz_threads) as executor:
        result = executor.map(split_edges, edges_to_split_chunks)
    result = list(result)
    nodes_to_add = [node for single_result in result for node in single_result[0]]
    edges_to_remove = [edge for single_result in result for edge in single_result[1]]
    edges_to_add = [edge for single_result in result for edge in single_result[2]]
    # result = [item for sublist in result for item in sublist]
    # nodes_to_add, edges_to_remove, edges_to_add = zip(*result)
    # nodes_to_add = [item for sublist in nodes_to_add for item in sublist]
    # edges_to_remove = [item for sublist in edges_to_remove for item in sublist]
    # edges_to_add = [item for sublist in edges_to_add for item in sublist]

    streckennetz.add_nodes_from(nodes_to_add)
    streckennetz.remove_edges_from(edges_to_remove)
    streckennetz.add_edges_from(edges_to_add)

    # Add length of geometry to edge (osmnx calculates distance from node to node)
    print('Adding more precisely lengths')
    with progressbar.ProgressBar(max_value=len(streckennetz.edges())) as bar:
        geometries = []
        for i, (u, v) in enumerate(streckennetz.edges()):
            try:
                geometries.append(streckennetz[u][v][0]['geometry'])
            except KeyError:
                geometries.append(None)
                pass
            bar.update(i)
        with ProcessPoolExecutor(max_workers=streckennetz_threads) as executor:
                lengths = executor.map(length_of_line, geometries)
        lengths = [length for length in lengths]
        for i, (u, v) in enumerate(streckennetz.edges()):
            if lengths[i] is not None:
                streckennetz[u][v][0]['length'] = lengths[i]

    # upload_minimal(streckennetz)
    upload_full(streckennetz)

    # Test functionality
    path = ox.shortest_path(streckennetz, 'Tübingen Hbf', 'Altingen(Württ)')
    print(nx.shortest_path_length(streckennetz, 'Tübingen Hbf', 'Altingen(Württ)', weight='length'))
    ox.plot_graph_route(streckennetz, path)
