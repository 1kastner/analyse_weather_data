#! /usr/bin/python3
"""

Runt it with
-m descriptive_statistics.calculate_available_data
for the main script.
"""

import datetime
import sys
import os.path
import logging

import dateutil.parser
import pandas

from filter_weather_data import RepositoryParameter
from filter_weather_data import get_repository_parameters
from filter_weather_data.filters import StationRepository


def setup_logger():
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    path_to_file_to_log_to = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "log",
        "calculate_available_data.log"
    )
    file_handler = logging.FileHandler(path_to_file_to_log_to)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    logging.info("### Start new logging")
    return log


def get_available_data(station_dict):
    """

    :param station_dict: The station dict
    :type station_dict dict
    :return: Fraction of available data
    :rtype: int
    """
    fraction_missing = (
        station_dict["data_frame"].temperature.isnull().sum()
        /
        len(station_dict["data_frame"].temperature)
    )
    return 1 - fraction_missing


def sample_up(df, start_date, end_date):
    if isinstance(end_date, str):
        end_date = dateutil.parser.parse(end_date)
    real_end_date = end_date + datetime.timedelta(days=1) - datetime.timedelta(minutes=1)
    df_year = pandas.DataFrame(index=pandas.date_range(start_date, real_end_date, freq='T', name="datetime"))
    df = df.join(df_year, how="outer")
    df.temperature.fillna(method="ffill", limit=30, inplace=True)
    return df


def gather_statistics(repository_parameter, start_date, end_date):
    logging.info("repository: %s" % repository_parameter.value)
    station_repository = StationRepository(*get_repository_parameters(repository_parameter))
    availabilities = []
    station_dicts = station_repository.load_all_stations(start_date=start_date, end_date=end_date)
    logging.info("total: %i" % len(station_dicts))
    while True:
        if len(station_dicts) == 0:
            break
        station_dict = station_dicts.pop()
        position = station_dict["meta_data"]["position"]
        station_dict["data_frame"] = sample_up(station_dict["data_frame"], start_date, end_date)
        row_result = {
                "station_name": station_dict["name"],
                "lat": position["lat"],
                "lon": position["lon"],
                "available_data": get_available_data(station_dict)
            }
        availabilities.append(row_result)
        logging.debug("{station_name}: {lat} {lon} -- {available_data}".format(**row_result))
    df = pandas.DataFrame(availabilities)
    result_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "log",
        "calculate_available_data_%s.csv" % repository_parameter.value
    )
    df.to_csv(result_file)


def run():

    start_date = "2016-01-01"
    end_date = "2016-12-31"

    logging.info("start")
    gather_statistics(RepositoryParameter.START, start_date, end_date)

    logging.info("frequent_reports")
    gather_statistics(RepositoryParameter.FREQUENT_REPORTS, start_date, end_date)

    logging.info("only_outdoor")
    gather_statistics(RepositoryParameter.ONLY_OUTDOOR, start_date, end_date)

    logging.info("outdoor_and_shaded")
    gather_statistics(RepositoryParameter.ONLY_OUTDOOR_AND_SHADED, start_date, end_date)


if __name__ == "__main__":
    setup_logger()
    run()
