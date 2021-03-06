"""

"""
import logging

import numpy
import geopy
import geopy.distance

from ..visualise_points_on_map import draw_map
from .abstract_neighbour_finder import AbstractNeighbourFinder


class NearestKFinder(AbstractNeighbourFinder):

    def __init__(self, station_dicts, start_date, end_date):
        self._sorted_station_dicts = station_dicts[:]
        self.last_target = None
        self.cached_temporary = {}

        # logging.debug("start resampling for k nearest")
        self.station_dict_at_position = {}
        for station_dict in station_dicts:
            self.sample_up(station_dict, start_date, end_date)
        # logging.debug("end resampling for k nearest")

    def _sort_for_target(self, target_dict):
        position = target_dict["meta_data"]["position"]
        search_lat = position["lat"]
        search_lon = position["lon"]
        search = geopy.Point(search_lat, search_lon)
        key = "interpolation_distance_" + target_dict["name"]
        for station_dict in self._sorted_station_dicts:
            position = station_dict["meta_data"]["position"]
            point_lat = position["lat"]
            point_lon = position["lon"]
            point = geopy.Point(point_lat, point_lon)
            distance = geopy.distance.distance(point, search).m
            station_dict[key] = distance
        self._sorted_station_dicts.sort(key=lambda station_dict: station_dict[key])

    def find_k_nearest_neighbours(self, target_dict, date, k, cache=True):
        """
        
        :param target_dict: The station to look for
        :param date: The time point (pandas compatible)
        :param k: The number of neighbours, all neighbours for k=-1
        :return: List of closest temperatures and distances
        """
        if self.last_target != target_dict:
            self._sort_for_target(target_dict)

        neighbours = []
        found = 0
        key = "interpolation_distance_" + target_dict["name"]
        if cache:
            for target_dict in self._sorted_station_dicts:
                temperature_at_time_t = target_dict["data_frame"].loc[date].temperature
                if numpy.isnan(temperature_at_time_t):  # station not available
                    continue
                else:
                    found += 1
                    distance = target_dict[key]
                    neighbours.append((temperature_at_time_t, distance))
                    if k != -1 and found == k:
                        break
            return neighbours
        else:
            distance_key = "interpolation_distance_" + target_dict["name"]
            if distance_key not in self.cached_temporary:
                sorted_station_dicts = self._sorted_station_dicts.copy()
                position = target_dict["meta_data"]["position"]
                search_lat = position["lat"]
                search_lon = position["lon"]
                search = geopy.Point(search_lat, search_lon)
                for station_dict in sorted_station_dicts:
                    if distance_key not in station_dict:
                        position = station_dict["meta_data"]["position"]
                        point_lat = position["lat"]
                        point_lon = position["lon"]
                        point = geopy.Point(point_lat, point_lon)
                        distance = geopy.distance.distance(point, search).m
                        station_dict[distance_key] = distance
                sorted_station_dicts.sort(key=lambda station_dict: station_dict[distance_key])
                self.cached_temporary[distance_key] = sorted_station_dicts
            for station_dict in self.cached_temporary[distance_key]:
                temperature_at_time_t = station_dict["data_frame"].loc[date].temperature
                if numpy.isnan(temperature_at_time_t):  # station not available
                    continue
                else:
                    found += 1
                    distance = station_dict[distance_key]
                    neighbours.append((temperature_at_time_t, distance))
                    if k != -1 and found == k:
                        break
            return neighbours

    def find_k_nearest_neighbour_dicts(self, target_dict, date, k):
        """

        :param target_dict: The station to look for
        :param date: The time point (pandas compatible)
        :param k: The number of neighbours, all neighbours for k=-1
        :return: List of closest temperatures and distances
        """
        if self.last_target != target_dict:
            self._sort_for_target(target_dict)

        neighbours = []
        found = 0
        for station_dict in self._sorted_station_dicts:
            temperature_at_time_t = station_dict["data_frame"].loc[date].temperature
            if numpy.isnan(temperature_at_time_t):  # station not available
                continue
            else:
                found += 1
                neighbours.append(station_dict)
                if k != -1 and found == k:
                    break
        return neighbours


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
    k_nearest_finder = NearestKFinder(station_dicts, start_date, end_date)
    neighbours = k_nearest_finder.find_k_nearest_neighbours(search_for, "2016-02-05T03:01", 3)
    for neighbour in neighbours:
        temperature, distance = neighbour
        print("measured", temperature, "°C in", distance, "meters distance")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
