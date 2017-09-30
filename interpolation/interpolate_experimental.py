"""

Run demo with 
python3 -m interpolation.interpolate interpolate.py
"""
import datetime
import random
import logging
import itertools
import os

import numpy
import pandas

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter

from interpolation.interpolator.nearest_k_finder import NearestKFinder
from interpolation.interpolator.statistical_interpolator_experimental import get_interpolation_results


class Scorer:
    def __init__(self, target_station_dict, neighbour_station_dicts, start_date, end_date):
        self.target_station_dict = target_station_dict
        self.nearest_k_finder = NearestKFinder(neighbour_station_dicts, start_date, end_date)

    def score_all_neighbours(self, date, t_actual):
        relevant_neighbours = self.nearest_k_finder.find_k_nearest_neighbours(self.target_station_dict, date, -1)
        return get_interpolation_results(relevant_neighbours, t_actual, "_all")


def score_interpolation_algorithm_at_date(scorer, date):
    t_actual = scorer.target_station_dict["data_frame"].loc[date].temperature
    results = {}
    results.update(scorer.score_all_neighbours(date, t_actual))
    return results


def setup_logging(interpolation_name):
    log = logging.getLogger('')

    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    #console_handler = logging.StreamHandler(sys.stdout)
    #console_handler.setFormatter(formatter)
    #log.addHandler(console_handler)

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
        end_date
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
    grouped_by_half_day = numpy.array_split(each_minute, total_len / 720)  # 12h
    each_half_day = [numpy.random.choice(hour_group) for hour_group in grouped_by_half_day]
    for current_i, date in enumerate(each_half_day):
        result = score_interpolation_algorithm_at_date(scorer, date)
        for method, square_error in result.items():
            if method not in sum_square_errors:
                sum_square_errors[method] = {}
                sum_square_errors[method]["total"] = 0
                sum_square_errors[method]["n"] = 0
            if not numpy.isnan(square_error):
                sum_square_errors[method]["total"] += square_error
                sum_square_errors[method]["n"] += 1

    method_and_result = list(sum_square_errors.items())
    method_and_result.sort(key=lambda x: x[0])
    for method, result in method_and_result:
        if sum_square_errors[method]["n"] > 0:
            method_rmse = numpy.sqrt(sum_square_errors[method]["total"] / sum_square_errors[method]["n"])
        else:
            method_rmse = numpy.nan
        sum_square_errors[method]["rmse"] = method_rmse
        score_str = "%.3f" % method_rmse
        logging.info(method + " " * (12 - len(method)) + score_str + " n=" + str(sum_square_errors[method]["n"]))

    logging.info("end method list")

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
            end_date
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
        score_str = "%.5f" % overall_rmse
        logging.info(method + " " * (12 - len(method)) + score_str + " n=" + str(overall_n))

    overall_result_df.to_csv("interpolation_result_{date}_{interpolation_name}.csv".format(
        date=datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-"),
        interpolation_name=interpolation_name
    ))


def demo():
    start_date = "2016-01-31"
    end_date = "2016-02-01"
    repository_parameters = get_repository_parameters(RepositoryParameter.ONLY_OUTDOOR_AND_SHADED)
    score_algorithm(start_date, end_date, repository_parameters, limit=60, interpolation_name="test")


if __name__ == "__main__":
    demo()
