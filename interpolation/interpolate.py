import os.path
import statistics

import geopy
import geopy.distance
import pandas as pd
import numpy as np
from scipy.spatial import Delaunay


def find_k_nearest_neighbours(t, station_dfs, k):
    """
    
    :param t: Time point
    :type t: ``datetime.datetime``
    :param station_dfs: A list of temperature records sorted by distance.
    :param k: number of neighbours
    :return: 
    """
    neighbours = []
    found = 0
    for station_df in station_dfs:
        temperature_at_time_t = station_df.loc[t].temperature
        if np.isnan(temperature_at_time_t):  # station not available
            continue
        else:
            found += 1
            distance = station_df.distance  # a simple attribute attached while sorting
            neighbours.append([temperature_at_time_t, distance])
            if k != -1 and found == k:
                break

    return neighbours


def find_delaunay_neighbours(lat, lon, t, station_dfs):
    filtered_stations = []
    for station_df in station_dfs:
        temperature_at_time_t = station_df.loc[t].temperature
        if not np.isnan(temperature_at_time_t):
            filtered_stations.append(
                    np.array([
                        station_df.lat.values[0],
                        station_df.lon.values[0]
                    ]
                )
            )
    if len(filtered_stations) <= 4:  # QHULL: needs 4 to form initial simplex
        return []
    tri = Delaunay(filtered_stations)
    index = tri.find_simplex(np.array([(lat, lon)]))[0]
    if index != -1:
        coordinates_index = tri.simplices[index]
        coordinates = tri.points[coordinates_index]
        stations = list()
        for station_coordinates in coordinates:
            station_at_coordinates = (str(station_coordinates[0]) + "," +
                    str(station_coordinates[1]))
            stations.append(STATION_AT[station_at_coordinates])
        delaunay_neighbour_dfs = list()
        for station_df in stations:
            temperature = station_df.loc[t].temperature
            distance = -1
            delaunay_neighbour_dfs.append([temperature, distance])
        return delaunay_neighbour_dfs
    else:  # no delaunay triangle found, error
        return []

eddh_df = load_station("EDDH")


def score_algorithm_single(station_df, date, station_dfs, p):
    lat = station_df.lat.values[0]
    lon = station_df.lon.values[0]
    t_actual = station_df.loc[date].temperature
    t_eddh = eddh_df.loc[date].temperature

    result = {}
    square_error_simple_model = (t_eddh - t_actual) ** 2
    result["eddh"] = square_error_simple_model

    # Nearest Neighbour
    nbs = find_k_nearest_neighbours(lat, lon, date, station_dfs, 1)
    if len(nbs) == 1:
        t_nb = nbs[0][0]
        square_error_nn = (t_nb - t_actual) ** 2
        result["nn"] = square_error_nn

    # 3 Nearest Neighbours
    nbs = find_k_nearest_neighbours(lat, lon, date, station_dfs, 3)
    if len(nbs) != 0:
        # IDW 3 p2
        inverted_sum_distances = sum([x[1] ** -2 for x in nbs])
        weights = [(x[1] ** -2) / inverted_sum_distances for x in nbs]
        t_idw3 = sum([w * t_i for w, t_i in zip(weights, [x[0] for x in nbs])])
        square_error_idw3 = (t_idw3 - t_actual) ** 2
        result["idw3p2"] = square_error_idw3

        # IDW 3 p3
        inverted_sum_distances = sum([x[1] ** -3 for x in nbs])
        weights = [(x[1] ** -3) / inverted_sum_distances for x in nbs]
        t_idw3 = sum([w * t_i for w, t_i in zip(weights, [x[0] for x in nbs])])
        square_error_idw3 = (t_idw3 - t_actual) ** 2
        result["idw3p3"] = square_error_idw3

        nb3_temperatures = [x[1] for x in nbs]

        # MAX 3
        t_max3 = max(nb3_temperatures)
        square_error_max3 = (t_max3 - t_actual) ** 2
        result["max3"] = square_error_max3

        # MEDIAN 3
        t_median3 = statistics.median(nb3_temperatures)
        square_error_median3 = (t_median3 - t_actual) ** 2
        result["median3"] = square_error_median3

        # MIN 3
        t_min3 = min(nb3_temperatures)
        square_error_min3 = (t_min3 - t_actual) ** 2
        result["min3"] = square_error_min3

    nbs = find_delaunay_neighbours(lat, lon, date, station_dfs)
    if len(nbs) != 0:
        # IDW delaunay p2
        inverted_sum_distances = sum([x[1] ** -p for x in nbs])
        weights = [(x[1] ** -p) / inverted_sum_distances for x in nbs]
        t_idw3dt = sum([w * t_i for w, t_i in zip(weights,
                [x[0] for x in nbs])])
        square_error_idw3dt = (t_idw3dt - t_actual) ** 2
        result["idw3dt"] = square_error_idw3dt

        nb3dt_temperatures = [x[1] for x in nbs]

        # MAX 3 delaunay
        t_max3dt = max(nb3dt_temperatures)
        square_error_max3dt = (t_max3dt - t_actual) ** 2
        result["max3dt"] = square_error_max3dt

        # MEDIAN 3 delaunay
        t_median3dt = statistics.median(nb3dt_temperatures)
        square_error_median3dt = (t_median3dt - t_actual) ** 2
        result["median3dt"] = square_error_median3dt

        # MIN 3 delaunay
        t_min3dt = min(nb3dt_temperatures)
        square_error_min3dt = (t_min3dt - t_actual) ** 2
        result["min3dt"] = square_error_min3dt

    return result


def score_algorithm(station_dfs, p):
    husconet_stations = station_ids.husconet_stations
    for husconet_station in husconet_stations:
        print("work on " + husconet_station)
        station_df = load_station(husconet_station, is_clean=True)

        search_lat = station_df.lat.values[0]
        search_lon = station_df.lon.values[0]
        search = geopy.Point(search_lat, search_lon)
        for df in station_dfs:
            point_lat = df.lat.values[0]
            point_lon = df.lon.values[0]
            point = geopy.Point(point_lat, point_lon)
            distance = geopy.distance.distance(point, search).km
            df.distance = distance
        station_dfs.sort(key=lambda station_df: station_df.distance)

        #print("Have them sorted: ")
        #for df in station_dfs:
        #    print(df.station)

        sum_square_errors = dict()

        for date in station_df.index.values:
            result = score_algorithm_single(station_df, date, station_dfs, p)
            for method, square_error in result.items():
                if method not in sum_square_errors:
                    sum_square_errors[method] = {}
                    sum_square_errors[method]["total"] = 0
                    sum_square_errors[method]["n"] = 0
                if np.isnan(square_error):
                    continue
                sum_square_errors[method]["total"] += square_error
                sum_square_errors[method]["n"] += 1
        scoring = []
        for method, result in sum_square_errors.items():
            method_rmse = np.sqrt(sum_square_errors[method]["total"] /
                    sum_square_errors[method]["n"])
            sum_square_errors[method]["rmse"] = method_rmse
            scoring.append([method, method_rmse])
        scoring.sort(key=lambda x: x[1])
        for method, score in scoring:
            print(method, score)
        print()
    print()


def run(stations=None, p=2):
    if stations is None:
        #stations, _ = station_ids.station_c2_ids.split(15)
        stations = station_ids.station_c2_ids
        #stations = station_ids.station_ids
        #stations = station_ids.some_ids
    station_dfs = []
    for station in stations:
        station_df = load_station(station)
        if station_df is None:
            continue
        station_df.station = station
        station_dfs.append(station_df)
    score_algorithm(station_dfs, p)


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        run()
    elif len(sys.argv) == 2:
        p = int(sys.argv[1])
        run(p=p)
    else:
        print("too many arguments")
