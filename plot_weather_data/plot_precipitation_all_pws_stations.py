"""
You can limit this by 
- passing the limit=n parameter to station_repository.load_all_stations()
- restricting the date range, e.g. plot_station("2016-01-01", "2016-01-31")

Depends on filter_weather_data.filters.preparation.average_husconet_temperature

-m plot_weather_data.plot_temperature_all_pws_stations
"""

import logging
import datetime

import pandas
import numpy
import pylab as pyplot
from matplotlib import dates as mdates
import matplotlib.patches as mpatches
import matplotlib.lines as mlines

from filter_weather_data import get_repository_parameters, RepositoryParameter
from gather_weather_data.dwd.get_pandas_compatible_csv_from_dwd_precipitation_file import load_dwd_precipitation
from filter_weather_data.filters import StationRepository
from . import insert_nans


def clean_data(station_df, monthly_dwd_df):
    """
    This replaces unneeded information and impossible data with nans.

    :param station_df:
    :return:
    """
    station_df.loc[station_df.precipitation <= 0] = numpy.nan

    for year in station_df.index.year.unique():
        year_key = str(year)
        year_df = station_df.loc[year_key:year_key]  # avoids getting a series if a single entry exists
        if year_df.empty or year_df.precipitation.count() == 0:
            continue
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = station_df.loc[month_key]
            max_precipitation_this_month = monthly_dwd_df.loc[month_key].precipitation[0]
            month_df.loc[month_df.precipitation >= max_precipitation_this_month] = numpy.nan
    return station_df


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

    station_repository = StationRepository(weather_stations, summary_dir)
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date,
        limit=10,  # for testing the design
        limit_to_temperature=False
    )

    dwd_station_df = load_dwd_precipitation(start_date, end_date)
    monthly_dwd_df = dwd_station_df.groupby(pandas.TimeGrouper("M")).sum()

    figure = pyplot.figure()
    figure.canvas.set_window_title(title)

    axis_2 = figure.add_subplot(111)
    axis_2.yaxis.tick_right()
    axis_2.yaxis.set_label_position("right")
    axis_2.set_ylabel("Niederschlag (mm/Tag)")
    axis_2.plot(dwd_station_df.index, dwd_station_df.precipitation, label="DWD Tageswerte", alpha=.8)
    axis_2.fill_between(dwd_station_df.index, dwd_station_df.precipitation, facecolors="b", interpolate=True, alpha=.8)
    _, max_precipitation = axis_2.get_ylim()
    axis_2.set_ylim((0, max_precipitation))

    axis_1 = figure.add_subplot(111, sharex=axis_2, frameon=False)
    for station_dict in station_dicts:
        logging.debug("prepare plotting " + station_dict["name"])
        station_df = station_dict['data_frame']
        station_df = insert_nans(station_df)  # discontinue line if gap is too big
        station_df = clean_data(station_df, monthly_dwd_df)
        axis_1.plot(station_df.index, station_df.precipitation, ".", alpha=.6)

    axis_1.set_xlabel('2016')
    axis_1.xaxis.set_major_locator(mdates.MonthLocator())
    axis_1.xaxis.set_major_formatter(mdates.DateFormatter('%m'))
    axis_1.set_ylabel('Niederschlag (mm/Stunde)')
    _, max_precipitation = axis_1.get_ylim()
    axis_1.set_ylim((0, max_precipitation))



    axis_1.margins(x=0)  # remove margins for both axes

    blue_patch = mpatches.Patch(color='blue', label='DWD Tageswerte', alpha=.8)
    grey_dot = mlines.Line2D([], [], color='grey', marker='.', linestyle=" ",
                             label='private Wetterstation Stundenwerte')
    pyplot.legend(handles=[blue_patch, grey_dot])

    pyplot.show()


def plot():
    start = get_repository_parameters(RepositoryParameter.START_FULL_SENSOR)
    start_date = "2016-01-01"
    end_date = "2016-12-31"
    plot_station("Niederschlag 2016", *start, start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot()
