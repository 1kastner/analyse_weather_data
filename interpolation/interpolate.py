"""

Run demo with 
python3 -m interpolation.interpolate interpolate.py
"""
import datetime
import random
import logging
import itertools
import sys
import os

import numpy
import pandas

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter

from .interpolator.delaunay_triangulator import DelaunayTriangulator
from .interpolator.nearest_k_finder import NearestKFinder
from . import load_airport
from .interpolator.statistical_interpolator import get_interpolation_results


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
        return get_interpolation_results(relevant_neighbours, t_actual, "_cn3")

    def score_5_nearest_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, 5)
        return get_interpolation_results(relevant_neighbours, t_actual, "_cn5")

    def score_delaunay_neighbours(self, date, t_actual):
        relevant_neighbours = self.delaunay_triangulator.find_delaunay_neighbours(self.target_station_dict, date)
        return get_interpolation_results(relevant_neighbours, t_actual, "_dt")

    def score_all_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, -1)
        return get_interpolation_results(relevant_neighbours, t_actual, "_all")


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


def get_logger(interpolation_name):
    log = logging.getLogger('interpolate')

    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    file_name = "interpolation_{date}_{interpolation_name}.log".format(
        interpolation_name=interpolation_name,
        date=datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-")
    )
    path_to_file_to_log_to = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "log",
        file_name
    )
    file_handler = logging.FileHandler(path_to_file_to_log_to)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    log.propagate = False

    log.info("### Start new logging")
    return log


def do_interpolation_scoring(
        target_station_dict,
        j,
        target_station_dicts_len,
        neighbour_station_dicts,
        start_date,
        end_date,
        logger
):
    target_station_name = target_station_dict["name"]
    logger.info("interpolate for " + target_station_name)
    logger.info("currently at " + str(j + 1) + " out of " + target_station_dicts_len)
    logger.info("use " + " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))

    scorer = Scorer(target_station_dict, neighbour_station_dicts, start_date, end_date)
    scorer.nearest_k_finder.sample_up(target_station_dict, start_date, end_date)
    sum_square_errors = {}
    total_len = len(target_station_dict["data_frame"].index.values)
    each_minute = target_station_dict["data_frame"].index.values
    grouped_by_hour = numpy.array_split(each_minute, total_len / 60)
    each_hour = [numpy.random.choice(hour_group) for hour_group in grouped_by_hour]
    for current_i, date in enumerate(each_hour):
        result = score_interpolation_algorithm_at_date(scorer, date)
        for method, square_error in result.items():
            if method not in sum_square_errors:
                sum_square_errors[method] = {}
                sum_square_errors[method]["total"] = 0
                sum_square_errors[method]["n"] = 0
            if not numpy.isnan(square_error):
                sum_square_errors[method]["total"] += square_error
                sum_square_errors[method]["n"] += 1

    for method, result in sum_square_errors.items():
        if sum_square_errors[method]["n"] > 0:
            method_rmse = numpy.sqrt(sum_square_errors[method]["total"] / sum_square_errors[method]["n"])
        else:
            method_rmse = numpy.nan
        sum_square_errors[method]["rmse"] = method_rmse
        score_str = "%.3f" % method_rmse
        logger.info(method + " " * (12 - len(method)) + score_str + " n=" + str(sum_square_errors[method]["n"]))

    logger.info("end method list")

    data_dict = {}
    for method in sum_square_errors.keys():
        data_dict[method + "--rmse"] = [sum_square_errors[method]["rmse"]]
        data_dict[method + "--n"] = [sum_square_errors[method]["n"]]
        data_dict[method + "--total"] = [sum_square_errors[method]["total"]]
    return pandas.DataFrame(data=data_dict)


def score_algorithm(start_date, end_date, repository_parameters, limit=0, interpolation_name="NONE"):
    station_repository = StationRepository(*repository_parameters)
    station_dicts = station_repository.load_all_stations(start_date, end_date, limit=limit)

    # separate in two sets
    random.shuffle(station_dicts)
    separator = 1 - int(.7 * len(station_dicts))  # 70% vs 30%
    target_station_dicts, neighbour_station_dicts = station_dicts[:separator], station_dicts[separator:]

    logger = get_logger(interpolation_name)
    logger.info("General Overview")
    logger.info("targets: " + " ".join([station_dict["name"] for station_dict in target_station_dicts]))
    logger.info("neighbours: " + " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))
    logger.info("End overview")

    logger.info("Several Runs")
    target_station_dicts_len = str(len(target_station_dicts))

    overall_result = itertools.starmap(do_interpolation_scoring, [
        [
            target_station_dict,
            j,
            target_station_dicts_len,
            neighbour_station_dicts,
            start_date,
            end_date,
            logger
        ] for j, target_station_dict in enumerate(target_station_dicts)
    ])

    logger.info("end targets")

    logger.info("overall results")
    overall_result_df = pandas.concat(overall_result)
    column_names = overall_result_df.columns.values.tolist()
    methods = set()
    for column_name in column_names:
        method, value = column_name.split("--")
        methods.update([method])
    for method in methods:
        overall_total = numpy.nansum(overall_result_df[method + "--total"])
        overall_n = int(numpy.nansum(overall_result_df[method + "--n"]))
        overall_rmse = numpy.sqrt(overall_total / overall_n)
        score_str = "%.3f" % overall_rmse
        logger.info(method + " " * (12 - len(method)) + score_str + " n=" + str(overall_n))

    overall_result_df.to_csv("interpolation_result_{date}_{interpolation_name}.csv".format(
        date=datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-"),
        interpolation_name=interpolation_name
    ))


def demo():
    start_date = "2016-01-31"
    end_date = "2016-02-01"
    repository_parameters = get_repository_parameters(RepositoryParameter.ONLY_OUTDOOR_AND_SHADED)
    score_algorithm(start_date, end_date, repository_parameters, limit=10, interpolation_name="test")


if __name__ == "__main__":
    demo()
