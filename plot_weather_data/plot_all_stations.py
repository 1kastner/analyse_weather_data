"""
You can limit this by 
- passing the limit=n parameter to station_repository.load_all_stations()
- restricting the date range, e.g. plot_station("2016-01-01", "2016-01-31")

Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""

import os
import logging

import pandas
from matplotlib import pyplot
import matplotlib.dates as mdates

from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import PROCESSED_DATA_DIR


def plot_station(start_date, end_date, time_zone):
    """
    Plots measured values in the foreground and the average of all HUSCONET weather stations in the background.

    :param start_date: The start date of the plot
    :param end_date: The end date of the plot
    """
    not_empty_weather_stations = os.path.join(PROCESSED_DATA_DIR, "filtered_stations",
                                              "station_dicts_with_valid_position")
    summary_dir = os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_not_extreme")
    station_repository = StationRepository(not_empty_weather_stations, summary_dir)
    for station_dict in station_repository.load_all_stations(start_date, end_date, time_zone=time_zone, minutely=True):
        station_df = station_dict['data_frame']
        station_df.temperature.plot(kind='line', color='gray', alpha=.2)

    csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average.csv")
    husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    husconet_station_df = husconet_station_df[start_date:end_date]
    ax = husconet_station_df.temperature.plot(color="blue", alpha=0.7)
    ax.set_ylabel('Temperature in °C')
    ax.set_xlabel('Zeit')
    pyplot.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m.%Y'))
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_station("2016-01-01", "2016-12-31", "CET")
