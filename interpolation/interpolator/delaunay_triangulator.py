"""

"""
import logging

import numpy
from scipy.spatial import Delaunay
import geopy.distance

from .abstract_neighbour_finder import AbstractNeighbourFinder
from ..visualizer import draw_map


class DelaunayTriangulator(AbstractNeighbourFinder):

    def __init__(self, station_dicts, start_date, end_date, use_triangulation_cache=True):
        self.station_dicts = station_dicts

        self.use_triangulation_cache = use_triangulation_cache
        self.cached_triangulations = {}
        self.cached_distances = {}

        # logging.debug("start resampling for delaunay")
        self.station_dict_at_position = {}
        for station_dict in station_dicts:
            position = station_dict["meta_data"]["position"]
            self.station_dict_at_position[(position["lat"], position["lon"])] = station_dict
            self._sample_up(station_dict, start_date, end_date)

        # logging.debug("end resampling for delaunay")

    def find_delaunay_neighbours(self, target_station_dict, t):
        """
        
        :param target_station_dict: The station to find the neighbours for
        :param t: The time point to check, NaN neighbours don't count
        :return: 
        """

        # get delaunay triangulation
        triangulated = self._get_triangulation(t)
        if not triangulated:  # not enough data for time point
            return []

        # search for the index of the triangle the searched coordinates are in
        position = target_station_dict["meta_data"]["position"]
        lat, lon = position["lat"], position["lon"]
        index = triangulated.find_simplex(numpy.array([lat, lon]))

        # outside the triangulated area
        if index == -1:
            return []

        # find the three triangulation stations
        delaunay_neighbour_dicts = self._retrieve_station_dicts_from_simplex_index(triangulated, index)

        # prepare response
        neighbour_values = []
        for neighbour_dict in delaunay_neighbour_dicts:
            temperature = neighbour_dict["data_frame"].loc[t].temperature  # this is != NaN
            distance = self._get_distance(target_station_dict, neighbour_dict)
            neighbour_values.append((temperature, distance))
        return neighbour_values

    def _get_triangulation(self, t):
        """
        
        :param t: The data point
        :return: ``scipy.spatial.Delaunay``
        """
        filtered_stations = []
        for station_dict in self.station_dicts:
            temperature_at_time_t = station_dict["data_frame"].loc[t].temperature
            if not numpy.isnan(temperature_at_time_t):
                position = station_dict["meta_data"]["position"]
                filtered_stations.append((position["lat"], position["lon"]))
        if len(filtered_stations) <= 4:  # QHULL: needs 4 to form initial simplex
            return []
        filtered_stations = tuple(filtered_stations)
        if self.use_triangulation_cache and filtered_stations in self.cached_triangulations:
            triangulated = self.cached_triangulations[filtered_stations]
        else:
            filtered_stations_array = numpy.array(filtered_stations)
            triangulated = Delaunay(filtered_stations_array)
            if self.use_triangulation_cache:
                self.cached_triangulations[filtered_stations] = triangulated
        return triangulated

    def _retrieve_station_dicts_from_simplex_index(self, triangulated, index):
        """
        
        :param triangulated: A scipy triangulation
        :param index: The index for the triangulation
        :return: The three neighbours.
        """

        coordinates_index = triangulated.simplices[index]
        coordinates = triangulated.points[coordinates_index]
        station_dicts = []
        for station_coordinates in coordinates:
            station_dicts.append(self.station_dict_at_position[tuple(station_coordinates)])
        return station_dicts

    def _get_distance(self, station_dict_a, station_dict_b):
        """
        Retrieve distance in m
        
        :param station_dict_a: station_dict
        :type station_dict_a: dict
        :param station_dict_b: station_dict
        :type station_dict_b: dict
        :return: 
        """
        station_a = station_dict_a["name"]
        station_b = station_dict_b["name"]
        if (station_a, station_b) not in self.cached_distances:
            position_a = station_dict_a["meta_data"]["position"]
            position_b = station_dict_b["meta_data"]["position"]
            point_a = geopy.Point(position_a["lat"], position_a["lon"])
            point_b = geopy.Point(position_b["lat"], position_b["lon"])
            distance = geopy.distance.distance(point_a, point_b).m
            self.cached_distances[(station_b, station_a)] = self.cached_distances[(station_a, station_b)] = distance
        return self.cached_distances[(station_b, station_a)]


def demo():
    start_date = '2016-01-01T00:00'
    end_date = '2016-03-31T23:59'
    from filter_weather_data.filters import StationRepository
    station_repository = StationRepository()
    station_dicts = station_repository.load_all_stations(start_date, end_date, limit=20)
    meta_data_df = station_repository.get_all_stations()
    chosen_index = 9
    search_for = station_dicts[chosen_index]
    print("picked", station_dicts[chosen_index]["name"], "to look for")
    draw_map([(lat, lon, label)
              for label, (lat, lon) in meta_data_df.iterrows()])
    del station_dicts[chosen_index]  # otherwise triangles will contain the searched point as well.
    delaunay_triangulation = DelaunayTriangulator(station_dicts, start_date, end_date)
    neighbours = delaunay_triangulation.find_delaunay_neighbours(search_for, "2016-02-05T03:01")
    for neighbour in neighbours:
        temperature, distance = neighbour
        print("measured", temperature, "°C in", distance, "meters distance")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
