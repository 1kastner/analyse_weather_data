"""
prediction:
airport -> husconet

Uses:
/export/scratch/1kastner/neural_networks/evaluation_data_husconet_only.csv
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
from gather_weather_data.husconet import StationRepository as HusconetStationRepository
from interpolation.interpolator.prepare.neural_network_single_group import fill_missing_eddh_values
from interpolation.interpolator.prepare.neural_network_single_group import load_eddh


if platform.uname()[1].startswith("ccblade"):  # the output files can turn several gigabyte so better not store them
                                               # on a network drive
    PROCESSED_DATA_DIR = "/export/scratch/1kastner"


def join_to_big_vector(husconet_dicts, eddh_df):
    """

    :param husconet_dicts: The stations to compare to
    :param output_csv_file: Where to save the joined data to
    :return:
    """

    husconet_dfs = []
    while len(husconet_dicts):
        husconet_dict = husconet_dicts.pop()
        station_df = husconet_dict["data_frame"]
        for attribute in station_df.columns:
            if attribute not in ["temperature", "humidity"]:
                station_df.drop(attribute, 1, inplace=True)
        position = husconet_dict["meta_data"]["position"]
        station_df['lat'] = position["lat"]
        station_df['lon'] = position["lon"]
        husconet_dfs.append(station_df)

    big_husconet_df = pandas.concat(husconet_dfs)
    big_husconet_df.columns = big_husconet_df.columns.map(lambda x: str(x) + "_husconet")
    logging.debug("provided by HUSCONET: %s" % str(big_husconet_df.head(1)))
    big_husconet_df.sort_index(inplace=True)

    common_df = big_husconet_df.join(eddh_df, how="left")
    logging.debug("common_df with airport and husconet: %s" % str(common_df.head(1)))

    common_df.sort_index(inplace=True)

    common_df = fill_missing_eddh_values(common_df)

    logging.debug("isnull: %s" % str(common_df[common_df.isnull().any(axis=1)]))

    return common_df


def run():
    start_date = "2016-01-01T00:00"
    end_date = "2016-12-31T23:59"
    eddh_df = load_eddh(start_date, end_date)

    husconet_dicts = HusconetStationRepository().load_all_stations(
        start_date,
        end_date,
        #limit=3  # for testing purposes
    )
    random.shuffle(husconet_dicts)
    split_point = int(len(husconet_dicts) * .7)
    training_dicts, evaluation_dicts = husconet_dicts[:split_point],husconet_dicts[split_point:]

    logging.info("training stations: %s" % [station["name"] for station in training_dicts])
    logging.info("evaluation stations: %s" % [station["name"] for station in evaluation_dicts])

    evaluation_csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "neural_networks",
        "evaluation_data_husconet_only.csv"
    )
    common_df = join_to_big_vector(evaluation_dicts, eddh_df)
    common_df.to_csv(evaluation_csv_file)

    training_csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "neural_networks",
        "training_data_husconet_only.csv"
    )
    common_df = join_to_big_vector(training_dicts, eddh_df)
    common_df.to_csv(training_csv_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
