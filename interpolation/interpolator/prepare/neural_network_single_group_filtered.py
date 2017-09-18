"""
prepare prediction:
filtered pws -> filtered pws

Uses:
PROCESSED_DATA_DIR/neural_networks/training_data_filtered.csv
"""

import os
import random
import logging
import platform

import pandas

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter
from filter_weather_data import PROCESSED_DATA_DIR

from interpolation.interpolator.prepare.neural_network_single_group import load_eddh
from interpolation.interpolator.prepare.neural_network_single_group import fill_missing_eddh_values


if platform.uname()[1].startswith("ccblade"):  # the output files can turn several gigabyte so better not store them
                                               # on a network drive
    PROCESSED_DATA_DIR = "/export/scratch/1kastner"


def join_to_big_vector(output_csv_file, station_dicts, eddh_df):
    """

    :param station_dicts: The stations to use
    :param output_csv_file: Where to save the joined data to
    :return:
    """

    joined_stations = []
    while len(station_dicts):
        station_dict = station_dicts.pop()
        logging.debug("work on %s" % station_dict["name"])
        station_df = station_dict["data_frame"]
        for attribute in station_df.columns:
            if attribute not in ["temperature", "humidity", "dewpoint"]:
                station_df.drop(attribute, axis=1, inplace=True)
        position = station_dict["meta_data"]["position"]
        station_df['lat'] = position["lat"]
        station_df['lon'] = position["lon"]
        joined_stations.append(station_df.join(eddh_df, how="left"))
    common_df = pandas.concat(joined_stations)
    common_df.sort_index(inplace=True)
    common_df = fill_missing_eddh_values(common_df)
    common_df.to_csv(output_csv_file)


def run():
    start_date = "2016-01-01T00:00"
    end_date = "2016-12-31T23:59"

    eddh_df = load_eddh(start_date, end_date)
    station_repository = StationRepository(*get_repository_parameters(
        #RepositoryParameter.ONLY_OUTDOOR_AND_SHADED_FULL_SENSOR
        RepositoryParameter.ONLY_OUTDOOR_AND_SHADED
    ))
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date,
        # limit=5,  # for testing purposes
        limit_to_temperature=False
    )

    random.shuffle(station_dicts)
    split_point = int(len(station_dicts) * .7)
    training_dicts, evaluation_dicts = station_dicts[:split_point], station_dicts[split_point:]

    logging.info("training stations: %s" % [station["name"] for station in training_dicts])
    logging.info("evaluation stations: %s" % [station["name"] for station in evaluation_dicts])

    training_csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "neural_networks",
        "training_data_filtered.csv"
    )
    join_to_big_vector(training_csv_file, training_dicts, eddh_df)

    evaluation_csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "neural_networks",
        "evaluation_data_filtered.csv"
    )
    join_to_big_vector(evaluation_csv_file, evaluation_dicts, eddh_df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
