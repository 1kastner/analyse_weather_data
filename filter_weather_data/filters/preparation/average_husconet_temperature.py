"""
Average over all stations.
"""

import os
import logging

import pandas

from gather_weather_data.husconet import HUSCONET_STATIONS
from gather_weather_data.husconet import PROCESSED_DATA_DIR


def average_temperature_across_husconet_stations():
    working_dir = os.path.join(PROCESSED_DATA_DIR, "husconet")
    df = pandas.DataFrame()
    for station in HUSCONET_STATIONS:
        logging.debug(station)
        csv_file = os.path.join(working_dir, station + ".csv")
        station_df = pandas.read_csv(csv_file, usecols=["temperature", "datetime"], index_col="datetime",
                                     parse_dates=["datetime"])
        df = df.join(station_df, how="outer", rsuffix="_" + station)
    df = df.mean(axis=1).rename("temperature")
    csv_output = os.path.join(working_dir, "husconet_average.csv")
    df.to_csv(csv_output, header=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    average_temperature_across_husconet_stations()
