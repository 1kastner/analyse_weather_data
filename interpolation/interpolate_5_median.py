"""

Run demo with 
python3 -m interpolation.interpolate_5_median interpolate_5_median.py
"""
import datetime
import random
import logging
import itertools
import statistics
import sys
import os

import numpy
import pandas

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter

from interpolation.interpolator.nearest_k_finder import NearestKFinder
from interpolation.interpolator.statistical_interpolator import get_interpolation_results


pandas.set_option("display.max_rows", 5)


class Scorer:
    def __init__(self, target_station_dict, neighbour_station_dicts, start_date, end_date, median_size=5):
        self.target_station_dict = target_station_dict
        self.nearest_k_finder = NearestKFinder(neighbour_station_dicts, start_date, end_date)
        self.key = "interpolation_distance_" + self.target_station_dict["name"]
        self.median_size = median_size

    def score_3_nearest_neighbours(self, date, t_actual):
        relevant_neighbours = self.find_k_median_neighbours(date, 3)
        return get_interpolation_results(relevant_neighbours, t_actual, "_cn3")

    def score_5_nearest_neighbours(self, date, t_actual):
        relevant_neighbours = self.find_k_median_neighbours(date, 5)
        return get_interpolation_results(relevant_neighbours, t_actual, "_cn5")

    def score_all_neighbours(self, date, t_actual):
        relevant_neighbours = self.find_k_median_neighbours(date, -1)
        return get_interpolation_results(relevant_neighbours, t_actual, "_all")

    def find_k_median_neighbours(self, date, k):
        neighbour_dicts = self.nearest_k_finder.find_k_nearest_neighbour_dicts(
            self.target_station_dict, date, k
        )
        for neighbour_dict in neighbour_dicts:
            temperatures_and_distances = self.nearest_k_finder.find_k_nearest_neighbours(
                neighbour_dict, date, self.median_size,  # <- hence the name of the script
                cache=False
            )
            temperatures, distances = zip(*temperatures_and_distances)
            neighbour_dict["_median"] = statistics.median(temperatures)
        relevant_neighbours = [(neighbour_dict["_median"], neighbour_dict[self.key])
                               for neighbour_dict in neighbour_dicts]
        return relevant_neighbours


def score_interpolation_algorithm_at_date(scorer, date):
    t_actual = scorer.target_station_dict["data_frame"].loc[date].temperature
    results = {}
    if numpy.isnan(t_actual):
        return results
    results.update(scorer.score_3_nearest_neighbours(date, t_actual))
    results.update(scorer.score_5_nearest_neighbours(date, t_actual))
    results.update(scorer.score_all_neighbours(date, t_actual))
    return results


def setup_logging(interpolation_name):
    log = logging.getLogger('')

    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    #console_handler = logging.StreamHandler(sys.stdout)
    #console_handler.setFormatter(formatter)
    #log.addHandler(console_handler)

    file_name = "interpolation_median_5_{date}_{interpolation_name}.log".format(
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
        logging
):
    target_station_name = target_station_dict["name"]
    logging.info("interpolate for " + target_station_name)
    logging.info("currently at " + str(j + 1) + " out of " + target_station_dicts_len)
    logging.info("use " + " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))

    scorer = Scorer(target_station_dict, neighbour_station_dicts, start_date, end_date)
    scorer.nearest_k_finder.sample_up(target_station_dict, start_date, end_date)
    sum_square_errors = {}
    total_len = len(target_station_dict["data_frame"].index.values)
    each_minute = target_station_dict["data_frame"].index.values
    grouped_by_hour = numpy.array_split(each_minute, total_len / 60)
    each_hour = [numpy.random.choice(hour_group) for hour_group in grouped_by_hour]

    hour_len = len(each_hour)
    for current_i, date in enumerate(each_hour):
        result = score_interpolation_algorithm_at_date(scorer, date)
        if current_i % 200 == 0:
            logging.debug("done: %.3f percent" % (100 * current_i / hour_len))
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
        logging.info(method + " " * (12 - len(method)) + score_str + " n=" + str(sum_square_errors[method]["n"]))

    logging.info("end method list")
    logging.info("overall result")
    data_dict = {}
    for method in sum_square_errors.keys():
        data_dict[method + "--rmse"] = [sum_square_errors[method]["rmse"]]
        data_dict[method + "--n"] = [sum_square_errors[method]["n"]]
        data_dict[method + "--total"] = [sum_square_errors[method]["total"]]
    logging.info("end overall result")
    return pandas.DataFrame(data=data_dict)


def score_algorithm(start_date, end_date, repository_parameters, limit=0, interpolation_name="NONE"):
    station_repository = StationRepository(*repository_parameters)
    station_dicts = station_repository.load_all_stations(start_date, end_date, limit=limit)

    # separate in two sets
    random.shuffle(station_dicts)
    separator = int(.3 * len(station_dicts))  # 70% vs 30%
    target_station_dicts, neighbour_station_dicts = station_dicts[:separator], station_dicts[separator:]

    setup_logging(interpolation_name)
    logging.info("General Overview")
    logging.info("targets: " + " ".join([station_dict["name"] for station_dict in target_station_dicts]))
    logging.info("neighbours: " + " ".join([station_dict["name"] for station_dict in neighbour_station_dicts]))
    logging.info("End overview")

    logging.info("Several Runs")
    target_station_dicts_len = str(len(target_station_dicts))

    overall_result = itertools.starmap(do_interpolation_scoring, [
        [
            target_station_dict,
            j,
            target_station_dicts_len,
            neighbour_station_dicts,
            start_date,
            end_date,
            logging
        ] for j, target_station_dict in enumerate(target_station_dicts)
    ])

    logging.info("end targets")

    logging.info("overall results")
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
        logging.info(method + " " * (12 - len(method)) + score_str + " n=" + str(overall_n))

    overall_result_df.to_csv("interpolation_result5_median_{date}_{interpolation_name}.csv".format(
        date=datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-"),
        interpolation_name=interpolation_name
    ))


def demo():
    start_date = "2016-01-31"
    end_date = "2016-02-15"
    repository_parameters = get_repository_parameters(RepositoryParameter.START)
    score_algorithm(start_date, end_date, repository_parameters, limit=30, interpolation_name="test")


if __name__ == "__main__":
    demo()
