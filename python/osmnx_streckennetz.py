import os
import sys

from shapely.geometry.point import Point
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import osmnx as ox
import networkx as nx
import shapely
from shapely.geometry import LineString, MultiLineString, MultiPoint, Polygon
import numpy as np
import geopy.distance
import itertools
import math
import pandas as pd
import geopandas as gpd
from helpers import StationPhillip, BetriebsstellenBill
import matplotlib.pyplot as plt
import pickle
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm


def get_streckennetz_from_osm(point_cloud=None, place=None, cache=True) -> nx.MultiDiGraph:
    """
    Load Streckennetz from OpenStreetMap.

    Returns
    -------
    nx.Graph:
        Streckennetz without stations.
    """
    if place is None and point_cloud is None:
        raise ValueError("Must either supply point_cloud or place, not none")
    if place is not None and point_cloud is not None:
        raise ValueError("Must either supply point_cloud or place, not both")
    try:
        if not cache:
            raise FileNotFoundError
        streckennetz = nx.read_gpickle("cache/original_osm_rail_graph.gpickle")
        print('Using chached streckennetz insted of downloading new one from osm')
    except FileNotFoundError:
        rail_filter = '["railway"~"rail|tram|narrow_gauge|light_rail"]'
        if place:
            streckennetz = ox.graph_from_place(
                place,
                simplify=True,
                network_type='none',
                truncate_by_edge=False,
                custom_filter=rail_filter
            )
        else:
            plt.plot(point_cloud[:,0], point_cloud[:,1], 'o')
            plt.plot(*MultiPoint(point_cloud).convex_hull.exterior.xy)
            plt.gca().set_aspect('equal', 'datalim')
            plt.show()
            streckennetz = ox.graph_from_polygon(
                MultiPoint(point_cloud).convex_hull,
                simplify=True,
                network_type='none',
                truncate_by_edge=False,
                custom_filter=rail_filter
            )
        print('projecting streckennetz')
        streckennetz = ox.project_graph(streckennetz, to_crs='EPSG:3857')
        # Convert to non directional graph which only has half the number of edges and is way faster to compute
        streckennetz = nx.MultiGraph(streckennetz)
        nx.write_gpickle(streckennetz, "cache/original_osm_rail_graph.gpickle")
    return streckennetz


def angle_three_points(a, b, c):
    """Calculate the smallest angle β from the points abc"""
    β = math.degrees(math.atan2(c[1]-b[1], c[0]-b[0]) - math.atan2(a[1]-b[1], a[0]-b[0]))
    return (β + 360 if β < 0 else β) % 180

flatten = itertools.chain.from_iterable


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def length_of_line(line) -> float:
    """
    Calculate length of line in meters.

    Parameters
    ----------
    line: LineString
        Line to calculate length of. Must be EPSG:4326 (lat/lon in degre)

    Returns
    -------
    float:
        Length of line.
    """
    if line is not None:
        return sum((geopy.distance.distance(p1, p2).meters for p1, p2 in pairwise(tuple(zip(line.xy[0], line.xy[1])))))
    else:
        return None


def upload_minimal(streckennetz):
    """
    Upload edges of Streckennetz without attributes except edge length to database.

    Parameters
    ----------
    streckennetz: nx.Graph
        Graph of the Streckennetz
    """
    from database import DB_CONNECT_STRING
    streckennetz = ox.graph_to_gdfs(streckennetz, nodes=False)
    u_v_key = pd.DataFrame(streckennetz.index.to_numpy().tolist(), columns=streckennetz.index.names)
    u_v_key = u_v_key.set_index(['u', 'v', 'key'], drop=False)
    streckennetz['u'] = u_v_key['u']
    streckennetz['v'] = u_v_key['v']
    streckennetz = pd.DataFrame(streckennetz[['u', 'v', 'length']])
    streckennetz = streckennetz.reset_index(drop=True)
    streckennetz.to_sql('minimal_streckennetz', if_exists='replace', method='multi', con=DB_CONNECT_STRING)


def upload_full(nodes, edges):
    """
    Upload Streckennetz, including edge attributes, to database

    Parameters
    ----------
    nodes : gpd.GeoDataFrame
        GeoDataFrame of nodes
    edges : gpd.GeoDataFrame
        GeoDataFrame of edges
    """    
    from database import DB_CONNECT_STRING
    # streckennetz_nodes, streckennetz_edges = ox.graph_to_gdfs(streckennetz, nodes=True)
    # Function to generate WKB hex
    def wkb_hexer(line):
        return line.wkb_hex

    # Convert `'geometry'` column in GeoDataFrame `gdf` to hex
    # Note that following this step, the GeoDataFrame is just a regular DataFrame
    # because it does not have a geometry column anymore. Also note that
    # it is assumed the `'geometry'` column is correctly datatyped.
    edges['geometry'] = edges['geometry'].apply(wkb_hexer)
    edges.to_sql('full_streckennetz', if_exists='replace', method='multi', con=DB_CONNECT_STRING)

    nodes['geometry'] = nodes['geometry'].apply(wkb_hexer)
    nodes.to_sql('full_streckennetz_nodes', if_exists='replace', method='multi', con=DB_CONNECT_STRING)


def plot_algorithm(
    name,
    close_edges,
    line,
    orth_line,
    points=None,
    cuts=None,
    rep_points=None,
    intersections=None,
    u=None,
    v=None,
    splitted=None,
    closest_edge=None,
):
    fig, ax = plt.subplots()
    ax.set_aspect('equal', 'datalim')
    ax.set_title(name)
    
    for index, edge in close_edges.iterrows():
        ax.plot(*edge['geometry'].xy, color='black')

    ax.plot(*line.xy, color='red')
    ax.plot(*orth_line.xy, color='red')

    if points:
        ax.scatter(*points[0].xy, color='blue')
        ax.scatter(*points[1].xy, color='blue')

    if cuts:
        for cut in cuts:
            ax.plot(*cut.xy, color='purple')

    if rep_points:
        for rep_point in rep_points:
            ax.scatter(*rep_point.xy, color='gold', zorder=3)

    if intersections:
        for intersextion in intersections:
            ax.scatter(*intersextion.xy, color='green', zorder=3)

    if u:
        ax.scatter(*u.xy, color='green', zorder=3)
    if v:
        ax.scatter(*v.xy, color='orange', zorder=3)

    if splitted:
        ax.plot(*splitted[0].xy, color='green')
        ax.plot(*splitted[1].xy, color='orange')

    if closest_edge:
        ax.plot(*closest_edge.xy, color='brown')

    plt.show()


def split_geom(geom, line, line_exterior):
    if geom.intersects(line):
        intersection = geom.intersection(line)
        try:
            cut = shapely.ops.split(geom, line_exterior)[1]
            angle = angle_three_points(
                cut.representative_point().coords[0],
                intersection.coords[0],
                line.representative_point().coords[0],
            )
        except IndexError:
            angle = 0
        except NotImplementedError:
            angle = 0
        # print(angle)
        if 70 < angle < 110:
            return {
                'intersection': intersection,
                'splitted': shapely.ops.split(geom, line)
            }
        else:
            return None


def split_edge(
    index: tuple,
    name: str,
    edge: gpd.GeoSeries,
    close_edges: gpd.GeoDataFrame,
    line: LineString,
    line_exterior: MultiLineString,
    orth_line: LineString,
    orth_line_exterior: MultiLineString,
    plot: bool=False,
):
    """Split the geometry of an edge.

    Parameters
    ----------
    index : tuple
        Index of the edge to split
    name : str
        Name of the station where the edge is split
    edge : gpd.GeoSeries
        GeoSeries of the edge to split
    close_edges : gpd.GeoDataFrame
        GeoDataFrame of this and other edges close to the station
    line : LineString
        Line, with the station as center and orthogonal to the nearest edge
    line_exterior : MultiLineString
        Two lines, parallel to line, but with +/- a small offset. Used
        to calculate the cut angle between edge and orth_line
    orth_line : LineString
        Line, with the station as center and orthogonal to line
    orth_line_exterior : MultiLineString
        Two lines, parallel to orth_line, bit with +/- a small offset. Used
        to calculate the cut angle between edge and orth_line
    plot : bool, optional
        Whether to plot the process or not, by default False

    Returns
    -------
    list or None
        [description]
    """
    # split by first line
    splitted = split_geom(edge['geometry'], line, line_exterior)
    if splitted is None:
        # split by orthogonal line if split by line did not result in a split
        splitted = split_geom(edge['geometry'], orth_line, orth_line_exterior)

    if splitted is not None:
        if plot:
            plot_algorithm(
                name,
                close_edges,
                line,
                orth_line,
                splitted=splitted['splitted'],
                u=nodes.loc[index[0], 'geometry'],
                v=nodes.loc[index[1], 'geometry'],
            )
        
        return [
            [index],
            [(index[0], name, 0), (name, index[1], 0)],
            [*splitted['splitted']],
            [splitted['intersection']],
        ]
    else:
        return None

def insert_station(name, station, edges, nodes, plot=False):
    # Get edges close to station using r-tree indexing (this is way faster than using GeoDataFrame.cx[])
    bbox = shapely.geometry.box(
        station['geometry'].x - 100,
        station['geometry'].y - 100,
        station['geometry'].x + 100,
        station['geometry'].y + 100
    )
    close_edges = edges.iloc[list(edges.sindex.intersection(bbox.bounds))]

    if not close_edges.empty:
        multipoint = close_edges['geometry'].unary_union
        # queried_geom, nearest_geom = nearest_points(point, multipoint)
        for i in range(2):
            if i == 1:
                # Move station close to closest edge in order to get a split
                station.at['geometry'] = Point(np.array(points[1].xy).flatten() - (10 * bhf_vec))
                # plot = True

            points = shapely.ops.nearest_points(
                station['geometry'],
                multipoint
            )
            # Calculate vector from station nearest point
            bhf_vec = np.array([points[0].x - points[1].x, points[0].y - points[1].y])
            # Shorten vector to have a length of 1
            bhf_vec = bhf_vec / np.sqrt(bhf_vec.dot(bhf_vec))
            # Create a vector orthogonal to bhf_vec
            bhf_orth_vec = np.array([bhf_vec[1], -bhf_vec[0]])

            # line and orth_line make a cross on the station
            line = LineString([
                np.array(station['geometry'].xy).flatten() - (100 * bhf_vec),
                np.array(station['geometry'].xy).flatten() + (100 * bhf_vec)
            ])
            orth_line = LineString([
                np.array(station['geometry'].xy).flatten() - (100 * bhf_orth_vec),
                np.array(station['geometry'].xy).flatten() + (100 * bhf_orth_vec)
            ])

            # Exterior lines are used to cut edges in order to calculate cut angle
            line_exterior = MultiLineString([
                (np.array(station['geometry'].xy).flatten() - (200 * bhf_vec) - (2 * bhf_orth_vec),
                np.array(station['geometry'].xy).flatten() + (200 * bhf_vec) - (2 * bhf_orth_vec)),
                (np.array(station['geometry'].xy).flatten() - (200 * bhf_vec) + (2 * bhf_orth_vec),
                np.array(station['geometry'].xy).flatten() + (200 * bhf_vec) + (2 * bhf_orth_vec))
            ])
            orth_line_exterior = MultiLineString([
                (np.array(station['geometry'].xy).flatten() - (200 * bhf_orth_vec) - (2 * bhf_vec),
                np.array(station['geometry'].xy).flatten() + (200 * bhf_orth_vec) - (2 * bhf_vec)),
                (np.array(station['geometry'].xy).flatten() - (200 * bhf_orth_vec) + (2 * bhf_vec),
                np.array(station['geometry'].xy).flatten() + (200 * bhf_orth_vec) + (2 * bhf_vec))
            ])

            # split edges either with line or orth_line
            new_edges = []
            for index, edge in close_edges.iterrows():
                new_edges.append(split_edge(
                    index,
                    name,
                    edge,
                    close_edges,
                    line,
                    line_exterior,
                    orth_line,
                    orth_line_exterior,
                    plot=plot,
                ))
            if plot:
                plot_algorithm(
                    name,
                    close_edges,
                    line,
                    orth_line,
                    points=points,
                    # closest_edge=station['nearest_edge'],
                )
            try:
                # Save and return the splits made
                drop, add, geom, intersections = list(zip(*[new_edge for new_edge in new_edges if new_edge is not None]))
                drop = list(flatten(drop))
                add = list(flatten(add))
                geom = list(flatten(geom))
                intersections = list(flatten(intersections))

                return drop, add, geom, intersections
            except ValueError:
                pass
        else:
            print('could not split', name)
    else:
        print('there are no edges close to', name)
    # If nothing was split at all, return Nones instead
    return [None] * 4


if __name__ == '__main__':
    import helpers.fancy_print_tcp

    stations = StationPhillip(prefer_cache=False)
    betriebsstellen = BetriebsstellenBill(prefer_cache=False)

    stations_gdf = stations.to_gpd()
    stations_gdf['type'] = 'station'
    betriebsstellen_gdf = betriebsstellen.to_gdf()
    betriebsstellen_gdf['type'] = 'betriebsstelle'

    # Merge stations and betriebsstellen
    stations_gdf = pd.concat([stations_gdf, betriebsstellen_gdf])
    stations_gdf = stations_gdf.loc[~stations_gdf.index.duplicated(keep='first')]

    ox.config(log_console=True, use_cache=True)
    streckennetz = get_streckennetz_from_osm(
        point_cloud=np.array(list(zip(stations_gdf['geometry'].x, stations_gdf['geometry'].y))),
        cache=False,
    )
    nodes, edges = ox.graph_to_gdfs(streckennetz, fill_edge_geometry=True)

    stations_gdf = stations_gdf.to_crs('EPSG:3857')

    pickle.dump(stations_gdf, open("cache/stations_gdf.pkl", "wb"))
    stations_gdf = pickle.load(open("cache/stations_gdf.pkl", "rb"))

    replace_edges = []
    add_edges = {'index': [], 'geometry': []}
    add_nodes = {}
    recompute = []
    stations_to_insert = stations_gdf.index.to_list()
    # stations_to_insert=['Sand (Niederbay) Hafen', 'Königstein (Sächs Schweiz) Hp', 'Limmritz (Sachs)']
    while stations_to_insert:
        print('inserting', len(stations_to_insert), 'stations')
        # rebuild r-tree index for fast spatial indexing
        edges.sindex
        for name in tqdm(stations_to_insert):
            drop, add, geom, intersections = insert_station(name, stations_gdf.loc[name], edges, nodes, plot=False)
            if drop is not None:
                # test whether one of the spitted egdes was already split for 
                # another station
                for i in range(len(drop)):
                    if drop[i] in replace_edges:
                        # save the station to be recomputed in the next run
                        recompute.append(name)
                        break
                else:
                    # save changes that should be made in order to insert this station
                    replace_edges.extend(drop)
                    for i in range(len(add)):
                        add_edges['index'].append(add[i])
                        add_edges['geometry'].append(geom[i])
                    add_nodes[name] = {
                        'geometry': MultiPoint(intersections),
                        'type': stations_gdf.loc[name, 'type']
                    }
            else:
                pass
                # print('there are no edges close to', name)
                # insert_station(name, stations_gdf.loc[name], edges, nodes, plot=True)
        # remove splitted edges, append new edges and nodes
        add_edges = pd.DataFrame(add_edges).set_index('index')
        add_nodes = pd.DataFrame().from_dict(add_nodes, orient='index')
        edges = edges.drop(replace_edges)
        edges = edges.append(add_edges)
        nodes = nodes.append(add_nodes)

        # reset variables to store splitting results
        replace_edges = []
        add_edges = {'index': [], 'geometry': []}
        add_nodes = {}
        stations_to_insert = recompute
        recompute = []


    # Add length of geometry to edge (osmnx calculates distance from node to node)
    edges = edges.set_crs("EPSG:3857").to_crs("EPSG:4326")
    nodes = nodes.set_crs("EPSG:3857").to_crs("EPSG:4326")
    print('Adding more precise lengths')
    with ProcessPoolExecutor() as executor:
        edges['length'] = list(tqdm(executor.map(length_of_line, edges['geometry']), total=len(edges['geometry'])))
    # edges['length'] = lengths

    streckennetz = ox.graph_from_gdfs(nodes, edges)

    print('Uploading minimal')
    upload_minimal(streckennetz)
    upload_full(nodes, edges)

