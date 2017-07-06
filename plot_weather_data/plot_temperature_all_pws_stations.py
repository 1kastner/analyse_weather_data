"""
You can limit this by 
- passing the limit=n parameter to station_repository.load_all_stations()
- restricting the date range, e.g. plot_station("2016-01-01", "2016-01-31")

Depends on filter_weather_data.filters.preparation.average_husconet_temperature

-m plot_weather_data.plot_temperature_all_pws_stations
"""

import os
import logging

import pandas
import numpy
from matplotlib import pyplot
import matplotlib.dates as mdates

from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import PROCESSED_DATA_DIR
from . import insert_nans


def plot_station(start_date, end_date, time_zone):
    """
    Plots measured values in the foreground and the average of all HUSCONET weather stations in the background.

    :param start_date: The start date of the plot
    :param end_date: The end date of the plot
    """
    weather_stations = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations",
        "station_dicts_frequent.csv"
        # "station_dicts_outdoor.csv"
        # "station_dicts_shaded.csv"
        # "station_dicts_with_valid_position.csv"

    )
    summary_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_station_summaries_frequent"
        # "filtered_station_summaries_no_extreme_values"
        # "filtered_station_summaries_of_shaded_stations"
        # "station_summaries"
    )
    station_repository = StationRepository(weather_stations, summary_dir)
    for station_dict in station_repository.load_all_stations(start_date, end_date, time_zone, 80):
        station_df = station_dict['data_frame']
        station_df = insert_nans(station_df)  # discontinue line if gap is too big
        station_df.temperature.plot(kind='line', color='gray', alpha=.2)

    logging.debug("load husconet")
    csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average.csv")
    husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    husconet_station_df = husconet_station_df[start_date:end_date]
    logging.debug("start plotting")
    ax = husconet_station_df.temperature.plot(color="blue", alpha=0.7)
    ax.set_ylabel('Temperature in °C')
    ax.set_xlabel('Zeit')
    pyplot.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m.%Y'))
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_station("2016-01-01", "2016-12-31", "CET")
