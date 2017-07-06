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
import matplotlib.lines as mlines
import matplotlib.dates as mdates

from filter_weather_data.filters import GermanWinterTime
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
        # "station_dicts_frequent.csv"  # (2)
        # "station_dicts_outdoor.csv"  # (3)
        "station_dicts_shaded.csv"  # (4)
        # "station_dicts_with_valid_position.csv"  # (1)
    )
    summary_dir = os.path.join(
        PROCESSED_DATA_DIR,
        # "filtered_station_summaries_frequent"  # (2)
        # "filtered_station_summaries_no_extreme_values"  # (1)
        "filtered_station_summaries_of_shaded_stations"  # (3)
        # "station_summaries"  # (0)
    )
    fig = pyplot.figure()
    fig.canvas.set_window_title(os.path.basename(weather_stations[:-4]) + " " + os.path.basename(summary_dir))

    station_repository = StationRepository(weather_stations, summary_dir)
    station_dicts = station_repository.load_all_stations(start_date, end_date)
    for station_dict in station_dicts:
        logging.debug("prepare plotting " + station_dict["name"])
        station_df = station_dict['data_frame']
        station_df = insert_nans(station_df)  # discontinue line if gap is too big
        station_df.temperature.plot(kind='line', color='gray', alpha=.8)

    logging.debug("load husconet")
    csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average_temperature.csv")
    husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    husconet_station_df = husconet_station_df.tz_localize("UTC").tz_convert(time_zone).tz_localize(None)
    husconet_station_df = husconet_station_df[start_date:end_date]
    logging.debug("start plotting")
    husconet_station_df.temperature.plot(color="blue", alpha=0.2, label="Durchschnitt HUSCONET")
    upper_line = (husconet_station_df.temperature + husconet_station_df.temperature_std * 3)
    ax = upper_line.plot(color="green", alpha=0.4, label="Obergrenze HUSCONET")
    ax.set_ylabel('Temperature in °C')
    ax.set_xlabel('Zeit')
    pyplot.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m.%Y'))

    logging.debug("show plot")
    lines, labels = ax.get_legend_handles_labels()
    gray_line = mlines.Line2D([], [], color='gray', label="Netatmo")
    ax.legend(lines[len(station_dicts):] + [gray_line],
              labels[len(station_dicts):] + [gray_line.get_label()], loc='best')
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_station("2016-01-01", "2016-12-31", GermanWinterTime())
