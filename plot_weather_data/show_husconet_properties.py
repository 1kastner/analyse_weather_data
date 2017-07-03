"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""

import os

import pandas
from matplotlib import pyplot

from gather_weather_data.husconet import HUSCONET_STATIONS
from filter_weather_data.filters import PROCESSED_DATA_DIR


def plot_station():
    """
    Plots all HUSCONET weather stations in the background.
    """
    color = dict(boxes='DarkGreen', whiskers='DarkOrange', medians='DarkBlue', caps='Gray')
    csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average.csv")
    husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    husconet_station_df.plot(kind='box', color=color, sym='r+')

    for husconet_station in HUSCONET_STATIONS:
        csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", husconet_station + ".csv")
        husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
        husconet_station_df.plot(kind='box', color=color, sym='r+')

    pyplot.show()


if __name__ == "__main__":
    plot_station()
