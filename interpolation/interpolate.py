"""

"""
import random
import logging

import numpy

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter

from .interpolator.delaunay_triangulator import DelaunayTriangulator
from .interpolator.nearest_k_finder import NearestKFinder
from . import load_airport
from .interpolator.statistical_interpolator import get_interpolation_result_for_all_neighbours
from .interpolator.statistical_interpolator import get_interpolation_result_for_some_neighbours


class Scorer:

    def __init__(self, target_station_dict, neighbour_station_dicts, start_date, end_date):
        self.target_station_dict = target_station_dict
        self.nearest_k_finder = NearestKFinder(neighbour_station_dicts, start_date, end_date)
        self.delaunay_triangulator = DelaunayTriangulator(neighbour_station_dicts, start_date, end_date)
        self.airport_df = load_airport("EDDH", start_date, end_date)

    def score_nearest_neighbour(self, date, t_actual):
        neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, 1)
        if len(neighbours) == 1:
            t_nb = neighbours[0][0]
            return (t_nb - t_actual) ** 2
        else:
            return numpy.nan

    def score_airport(self, date, t_actual):
        t_eddh = self.airport_df.loc[date].temperature
        return (t_eddh - t_actual) ** 2

    def score_3_nearest_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, 3)
        return get_interpolation_result_for_some_neighbours(relevant_neighbours, t_actual, "_cn3")

    def score_5_nearest_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, 5)
        return get_interpolation_result_for_some_neighbours(relevant_neighbours, t_actual, "_cn5")

    def score_delaunay_neighbours(self, date, t_actual):
        relevant_neighbours = self.delaunay_triangulator.find_delaunay_neighbours(self.target_station_dict, date)
        return get_interpolation_result_for_some_neighbours(relevant_neighbours, t_actual, "_dt")

    def score_all_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, -1)
        return get_interpolation_result_for_all_neighbours(relevant_neighbours, t_actual)


def score_interpolation_algorithm_at_date(scorer, date):
    t_actual = scorer.target_station_dict["data_frame"].loc[date].temperature
    results = {
        "nn": scorer.score_nearest_neighbour(date, t_actual),
        "eddh": scorer.score_airport(date, t_actual),
    }
    results.update(scorer.score_3_nearest_neighbours(date, t_actual))
    results.update(scorer.score_5_nearest_neighbours(date, t_actual))
    results.update(scorer.score_all_neighbours(date, t_actual))
    results.update(scorer.score_delaunay_neighbours(date, t_actual))
    return results


def score_algorithm(start_date, end_date, repository_parameters, limit=0):
    station_repository = StationRepository(*repository_parameters)
    station_dicts = station_repository.load_all_stations(start_date, end_date, limit=limit)

    # separate in two sets
    random.shuffle(station_dicts)
    separator = 1 - int(.7 * len(station_dicts))
    target_station_dicts, neighbour_station_dicts = station_dicts[:separator], station_dicts[separator:]
    print("targets: ", " ".join([station_dict["name"] for station_dict in target_station_dicts]))
    print("neighbours: ", " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))

    for target_station_dict in target_station_dicts:
        print("interpolate for", target_station_dict["name"])
        print("use", " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))
        scorer = Scorer(target_station_dict, neighbour_station_dicts, start_date, end_date)
        scorer.nearest_k_finder.sample_up(target_station_dict, start_date, end_date)
        sum_square_errors = {}
        for date in target_station_dict["data_frame"].index.values:
            result = score_interpolation_algorithm_at_date(scorer, date)
            for method, square_error in result.items():
                if method not in sum_square_errors:
                    sum_square_errors[method] = {}
                    sum_square_errors[method]["total"] = 0
                    sum_square_errors[method]["n"] = 0
                if not numpy.isnan(square_error):
                    sum_square_errors[method]["total"] += square_error
                    sum_square_errors[method]["n"] += 1
        scoring = []
        for method, result in sum_square_errors.items():
            if sum_square_errors[method]["n"] > 0:
                method_rmse = numpy.sqrt(sum_square_errors[method]["total"] / sum_square_errors[method]["n"])
            else:
                method_rmse = numpy.nan
            sum_square_errors[method]["rmse"] = method_rmse
            scoring.append([method, method_rmse])
        scoring.sort(key=lambda x: x[1])
        for method, score in scoring:
            score_str = "%.3f" % score
            print(method, " "*(12-len(method)), score_str, "n=" + str(sum_square_errors[method]["n"]))
        print()
    print()





def demo():
    start_date = "2016-01-31"
    end_date = "2016-02-01"
    repository_parameters = get_repository_parameters(RepositoryParameter.ONLY_OUTDOOR_AND_SHADED)
    score_algorithm(start_date, end_date, repository_parameters, limit=10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
