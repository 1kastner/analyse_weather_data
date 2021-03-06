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

from gather_weather_data.husconet import load_husconet_temperature_average
from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import PROCESSED_DATA_DIR
from plot_weather_data import insert_nans
from plot_weather_data import style_year_2016_plot


def plot_station(title, weather_stations, summary_dir, start_date, end_date):
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
    """

    fig = pyplot.figure()
    fig.canvas.set_window_title(title)
    pyplot.rcParams['savefig.dpi'] = 300

    station_repository = StationRepository(weather_stations, summary_dir)
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date,
        # limit=10  # for testing purposes
    )
    for station_dict in station_dicts:
        logging.debug("prepare plotting " + station_dict["name"])
        station_df = station_dict['data_frame']
        station_df = insert_nans(station_df)  # discontinue line if gap is too big
        pyplot.plot(station_df.index, station_df.temperature, linewidth=.4, color='gray', alpha=.8)

    logging.debug("load husconet")
    husconet_station_df = load_husconet_temperature_average(start_date, end_date)

    logging.debug("start plotting")
    pyplot.plot(husconet_station_df.index, husconet_station_df.temperature, color="blue", linewidth=.4,
                label="Referenznetzwerk")
    # upper_line = (husconet_station_df.temperature + husconet_station_df.temperature_std * 3)
    # ax = upper_line.plot(color="green", alpha=0.4, label="avg(HUSCONET) + 3 $\sigma$(HUSCONET)")

    ax = pyplot.gca()
    style_year_2016_plot(ax)

    logging.debug("show plot")
    gray_line = mlines.Line2D([], [], color='gray', label="private Wetterstationen")  # only one entry for many
    blue_line = mlines.Line2D([], [], color="blue", label="Referenznetzwerk")  # proper line width to see color
    ax.legend(
        [blue_line, gray_line],
        [blue_line.get_label(), gray_line.get_label()],
        loc='best'
    )
    pyplot.show()


def plot_whole_filtering_pipe():
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
    start_date = "2016-01-01T00:00"
    end_date = "2016-12-31T23:59"
    plot_station(*start, start_date, end_date)
    plot_station(*frequent_reports, start_date, end_date)
    plot_station(*only_outdoor, start_date, end_date)
    plot_station(*only_outdoor_and_shaded, start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_whole_filtering_pipe()
