"""

Checks if enough data is provided during the given period.
"""

import logging
import calendar

import numpy

from . import StationRepository


def check_station(station_df):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785

    :param station_df: The station data frame (pandas)
    :return: Does station provide enough reports for analysis
    """

    for year in station_df.index.year.unique():
        year_key = str(year)
        year_df = station_df.loc[year_key]
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key]
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = month_df.loc[day_key]
                if len(day_df.index.hour.unique()) < 19:  # less than 19h observations
                    station_df.loc[day_key].temperature = numpy.nan
            station_df.dropna(subset=["temperature"], inplace=True)
            eighty_percent_of_month = round(calendar.monthrange(2016, month)[1] * .8)
            if len(month_df.index.day.unique()) <= eighty_percent_of_month:
                return False
    return True


def filter_stations(station_dicts, start_date, end_date):
    """

    :param station_dicts: The station dicts
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)

    """
    filtered_stations = []
    for station_dict in station_dicts:
        logging.debug(station_dict["name"])
        if check_station(station_dict["data_frame"]):
            filtered_stations.append(station_dict)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    time_zone = "CET"
    stations = ['IHAMBURG69', 'IBNNINGS2', 'IHAMBURG1795']
    station_repository = StationRepository()
    station_dicts = filter(lambda x: x is not None, [station_repository.load_station(station, start_date, end_date,
                                                                                     time_zone, minutely=True)
                                                     for station in stations])
    stations_with_data = filter_stations(station_dicts, start_date, end_date)
    print([station_dict["name"] for station_dict in stations_with_data])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
