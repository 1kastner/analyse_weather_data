"""
Average over all stations.
"""

import os
import logging

import pandas

from gather_weather_data.husconet import HUSCONET_STATIONS
from gather_weather_data.husconet import PROCESSED_DATA_DIR


def average_solar_radiation_across_husconet_stations(force_overwrite=False):
    """
    Average (statistic mean) all stations of husconet

    :param force_overwrite: Force to create new file
    """

    working_dir = os.path.join(PROCESSED_DATA_DIR, "husconet")
    if not os.path.isdir(working_dir):
        os.mkdir(working_dir)
    csv_output = os.path.join(working_dir, "husconet_average_radiation.csv")
    if os.path.isfile(csv_output) and not force_overwrite:
        return

    df = pandas.DataFrame()
    for station in HUSCONET_STATIONS:
        logging.debug(station)
        csv_file = os.path.join(working_dir, station + ".csv")
        station_df = pandas.read_csv(csv_file, usecols=["radiation", "datetime"], index_col="datetime",
                                     parse_dates=["datetime"])
        df = df.join(station_df, how="outer", rsuffix="_" + station)
    df = df.mean(axis=1).rename("radiation")
    df.to_csv(csv_output, header=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    average_solar_radiation_across_husconet_stations()
