"""

"""

import os
import random
import logging

import pandas

from filter_weather_data.filters import StationRepository
from filter_weather_data import get_repository_parameters
from filter_weather_data import RepositoryParameter
from filter_weather_data import PROCESSED_DATA_DIR
from gather_weather_data.husconet import StationRepository as HusconetStationRepository
from interpolation.interpolator.prepare.neural_network_single_group import fill_missing_eddh_values
from interpolation.interpolator.prepare.neural_network_single_group import load_eddh


def join_to_big_vector(output_csv_file, station_dicts, husconet_dicts, eddh_df):
    """

    :param husconet_dicts: The stations to compare to
    :param station_dicts: The stations to use
    :param output_csv_file: Where to save the joined data to
    :return:
    """

    station_dfs = []
    while len(station_dicts):
        station_dict = station_dicts.pop()
        station_df = station_dict["data_frame"]
        position = station_dict["meta_data"]["position"]
        station_df['lat'] = position["lat"]
        station_df['lon'] = position["lon"]
        station_dfs.append(station_df)

    big_station_df = pandas.concat(station_dfs)
    big_station_df.columns = big_station_df.columns.map(lambda x: str(x) + "_pws")
    logging.debug("provided by PWS: %s" % str(big_station_df.head(1)))
    big_station_df.sort_index(inplace=True)

    common_df = eddh_df.join(big_station_df, how="outer")
    logging.debug("common_df with airport and pws: %s" % str(common_df.head(1)))

    husconet_dfs = []
    while len(husconet_dicts):
        husconet_dict = husconet_dicts.pop()
        station_df = husconet_dict["data_frame"]
        for attribute in station_df.columns:
            if attribute != "temperature":
                station_df.drop(attribute, 1, inplace=True)
        position = husconet_dict["meta_data"]["position"]
        station_df['lat'] = position["lat"]
        station_df['lon'] = position["lon"]
        husconet_dfs.append(station_df)

    big_husconet_df = pandas.concat(husconet_dfs)
    big_husconet_df.columns = big_husconet_df.columns.map(lambda x: str(x) + "_husconet")
    logging.debug("provided by HUSCONET: %s" % str(big_husconet_df.head(1)))
    big_husconet_df.sort_index(inplace=True)

    common_df = common_df.join(big_husconet_df, how="outer")
    logging.debug("common_df with airport, pws and husconet: %s" % str(common_df.head(1)))

    common_df.sort_index(inplace=True)

    common_df = fill_missing_eddh_values(common_df)

    common_df.to_csv(output_csv_file)


def run():
    start_date = "2016-01-01"
    end_date = "2016-12-31"
    eddh_df = load_eddh(start_date, end_date)
    station_repository = StationRepository(*get_repository_parameters(RepositoryParameter.START_FULL_SENSOR))
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date,
        # limit=5  # for testing purposes
    )

    husconet_dicts = HusconetStationRepository().load_all_stations(
        start_date,
        end_date,
        # limit=3  # for testing purposes
    )
    random.shuffle(husconet_dicts)
    split_point = int(len(husconet_dicts) * .7)
    training_dicts, evaluation_dicts = husconet_dicts[:split_point],husconet_dicts[split_point:]
    logging.info("training stations: %s" % [station["name"] for station in training_dicts])
    logging.info("evaluation stations: %s" % [station["name"] for station in evaluation_dicts])

    evaluation_csv_file = os.path.join(
        #PROCESSED_DATA_DIR,
        "/export/scratch/1kastner", #only for ccblade
        "neural_networks",
        "evaluation_data_husconet.csv"
    )
    join_to_big_vector(evaluation_csv_file, station_dicts[:], evaluation_dicts, eddh_df)

    training_csv_file = os.path.join(
        #PROCESSED_DATA_DIR,
        "/export/scratch/1kastner", #only for ccblade
        "neural_networks",
        "training_data_husconet.csv"
    )
    join_to_big_vector(training_csv_file, station_dicts, training_dicts, eddh_df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run()
