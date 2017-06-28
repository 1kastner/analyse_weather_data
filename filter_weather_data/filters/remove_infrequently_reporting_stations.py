"""
Checks if enough data is provided during the given period.
"""

import logging
import calendar

import dateutil.parser
import numpy

from . import load_station


def check_station(station_df, start_date, end_date, time_zone):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785

    :param station_df: The station data frame (pandas)
    :param start_date: The date to start (included), naive date interpreted as UTC  
    :param end_date: The date to stop (included), naive date interpreted as UTC
    :param time_zone: The target time zone, e.g. 'CET' is the standard time (no daylight saving switch)
    :type time_zone: str
    :return: Does station provide enough reports for analysis
    """
    if type(start_date) is str:
        start_date = dateutil.parser.parse(start_date)
    if type(end_date) is str:
        end_date = dateutil.parser.parse(end_date)
    station_df = station_df[start_date:end_date]
    station_df = station_df.tz_localize("UTC").tz_convert(time_zone)
    for year in station_df.index.year.unique():
        for month in station_df.index.month.unique():
            logging.debug(month)
            for day in station_df.index.day:
                key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = station_df.loc[key]
                if len(day_df.index.hour.unique()) < 19:  # less than 19h observations
                    station_df.loc[key].temperature = numpy.nan
            station_df.dropna(subset=["temperature"], inplace=True)
            eighty_percent_of_month = round(calendar.monthrange(2016, month)[1] * .8)
            key = "{year}-{month}".format(year=year, month=month)
            month_df = station_df.loc[key]
            if len(month_df.index.day.unique()) <= eighty_percent_of_month:
                return False
    return True


def filter_stations(stations, start_date, end_date, time_zone):
    """

    :param stations: The name of the stations, e.g. ['IHAMBURG69']
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    :param time_zone: The time zone to consider for grouping the data, e.g. 'CET' is the standard time (no daylight 
        saving switch)
    """
    filtered_stations = []
    for station in stations:
        logging.debug(station)
        if check_station(station, start_date, end_date, time_zone):
            filtered_stations.append(station)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    time_zone = "CET"
    station_dfs = [load_station(station, start_date, end_date) for station in ['IHAMBURG69', 'IBNNINGS2']]
    stations_with_data = filter_stations(station_dfs, start_date, end_date, time_zone)
    print(stations_with_data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
