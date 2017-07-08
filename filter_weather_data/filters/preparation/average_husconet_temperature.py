"""
Average over all stations.
"""

import os
import logging

import pandas

from gather_weather_data.husconet import HUSCONET_STATIONS
from gather_weather_data.husconet import PROCESSED_DATA_DIR


def average_temperature_across_husconet_stations(force_overwrite=False):
    """
    Average (statistic mean) all stations of husconet
    
    :param force_overwrite: Force to create new file
    """

    working_dir = os.path.join(PROCESSED_DATA_DIR, "husconet")
    if not os.path.isdir(working_dir):
        os.mkdir(working_dir)
    csv_output = os.path.join(working_dir, "husconet_average_temperature.csv")
    if os.path.isfile(csv_output) and not force_overwrite:
        logging.debug("husconet average temperature file already exists, skipping")
        return

    df = pandas.DataFrame()
    for station in HUSCONET_STATIONS:
        logging.debug(station)
        csv_file = os.path.join(working_dir, station + ".csv")
        station_df = pandas.read_csv(csv_file, usecols=["temperature", "datetime"], index_col="datetime",
                                     parse_dates=["datetime"])
        df = df.join(station_df, how="outer", rsuffix="_" + station)
    average_temperature = df.mean(axis=1).rename("temperature")
    standard_deviation_temperature = df.std(axis=1).rename("temperature_std")
    df_2 = pandas.concat([average_temperature, standard_deviation_temperature], axis=1)
    df_2.to_csv(csv_output, header=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    average_temperature_across_husconet_stations(True)
