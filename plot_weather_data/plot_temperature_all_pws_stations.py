"""
You can limit this by 
- passing the limit=n parameter to station_repository.load_all_stations()
- restricting the date range, e.g. plot_station("2016-01-01", "2016-01-31")

Depends on filter_weather_data.filters.preparation.average_husconet_temperature

-m plot_weather_data.plot_temperature_all_pws_stations
"""

import os
import logging
import datetime

from matplotlib import pyplot
import matplotlib.lines as mlines
import matplotlib.dates as mdates

from gather_weather_data.husconet import GermanWinterTime
from gather_weather_data.husconet import load_husconet_temperature_average
from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import PROCESSED_DATA_DIR
from . import insert_nans


def plot_station(title, weather_stations, summary_dir, start_date, end_date, time_zone):
    """
    Plots measured values in the foreground and the average of all HUSCONET weather stations in the background.

    :param title: The window title
    :type title: str
    :param weather_stations: path to file with list of weather stations
    :type weather_stations: str
    :param summary_dir: directory with all necessary summaries, possibly pre-filtered
    :type summary_dir: str
    :param start_date: The start date of the plot
    :type start_date: str | datetime.datetime
    :param end_date: The end date of the plot
    :type end_date: str | datetime.datetime
    :param time_zone: The time zone to use for husconet stations
    :type time_zone: datetime.tzinfo
    """

    fig = pyplot.figure()
    fig.canvas.set_window_title(title)

    station_repository = StationRepository(weather_stations, summary_dir)
    station_dicts = station_repository.load_all_stations(start_date, end_date)
    for station_dict in station_dicts:
        logging.debug("prepare plotting " + station_dict["name"])
        station_df = station_dict['data_frame']
        station_df = insert_nans(station_df)  # discontinue line if gap is too big
        station_df.temperature.plot(kind='line', color='gray', alpha=.8)

    logging.debug("load husconet")
    husconet_station_df = load_husconet_temperature_average(start_date, end_date)

    logging.debug("start plotting")
    ax = husconet_station_df.temperature.plot(color="blue", alpha=0.4, label="avg(Referenznetzwerk)")
    # upper_line = (husconet_station_df.temperature + husconet_station_df.temperature_std * 3)
    # ax = upper_line.plot(color="green", alpha=0.4, label="avg(HUSCONET) + 3 $\sigma$(HUSCONET)")
    ax.set_ylabel('Temperature in °C')
    pyplot.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m.%Y'))

    logging.debug("show plot")
    lines, labels = ax.get_legend_handles_labels()
    gray_line = mlines.Line2D([], [], color='gray', label="Crowdsourced")
    ax.legend(lines[len(station_dicts):] + [gray_line],
              labels[len(station_dicts):] + [gray_line.get_label()], loc='best')
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    filtered_stations_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )

    # The different filter steps depend on the filtering_pipe script
    # Be aware which filters only remove stations and which modify the data!
    start = (
        "valid position not extreme values",
        os.path.join(filtered_stations_dir, "station_dicts_with_valid_position.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_no_extreme_values")
    )
    frequent_reports = (
        "frequent reports",
        os.path.join(filtered_stations_dir, "station_dicts_frequent.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
    )
    only_outdoor = (
        "only outdoor",
        os.path.join(filtered_stations_dir, "station_dicts_outdoor.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
    )
    only_outdoor_and_shaded = (
        "only outdoor and shaded",
        os.path.join(filtered_stations_dir, "station_dicts_shaded.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_of_shaded_stations")
    )

    plot_station(*start, "2016-01-01", "2016-12-31", GermanWinterTime())
    plot_station(*frequent_reports, "2016-01-01", "2016-12-31", GermanWinterTime())
    plot_station(*only_outdoor, "2016-01-01", "2016-12-31", GermanWinterTime())
    plot_station(*only_outdoor_and_shaded, "2016-01-01", "2016-12-31", GermanWinterTime())
