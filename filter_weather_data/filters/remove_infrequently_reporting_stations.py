"""

Checks if enough data is provided during the given period.
The check removes entries when the reports were too infrequent.
"""

import logging
import calendar

import pandas
import numpy

from . import StationRepository


def check_station(station_dict):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785
    
    This function modifies the handed in station_df

    :param station_dict: The station dict
    :return: Does station provide enough reports for analysis
    """
    station_name = station_dict["name"]
    station_df = station_dict["data_frame"]
    for year in station_df.index.year.unique():
        year_key = str(year)
        year_df = station_df.loc[year_key:year_key]  # avoids getting a series if a single entry exists
        if isinstance(year_df, pandas.DataFrame) and year_df.temperature.count() == 0:
            return False
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key:month_key]  # avoids getting a series if a single entry exists
            if isinstance(month_df, pandas.DataFrame) and month_df.temperature.count() < 22:  # obs for less than 22d
                return False
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = month_df.loc[day_key:day_key]  # avoids getting a series if a single entry exists
                if isinstance(day_df, pandas.DataFrame) and day_df.temperature.count() < 19:  # obs for less than 19h
                    continue
                if len(day_df.index.hour.unique()) < 19:  # less than 19h observations
                    station_df.loc[day_key].temperature = numpy.nan
            station_df.dropna(subset=["temperature"], inplace=True)
            eighty_percent_of_month = round(calendar.monthrange(year, month)[1] * .8)
            days_with_enough_reports = len(month_df.index.day.unique())
            if days_with_enough_reports <= eighty_percent_of_month:
                logging.debug(month_key + " " + station_name + " only got " + str(days_with_enough_reports)
                              + " but needed " + str(eighty_percent_of_month))
                return False
    return True


def filter_stations(station_dicts):
    """

    :param station_dicts: The station dicts
    """
    filtered_stations = []
    for station_dict in station_dicts:
        logging.debug("infrequent " + station_dict["name"])
        if check_station(station_dict):
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
    stations_with_frequent_reports = filter_stations(station_dicts)
    print([station_dict["name"] for station_dict in stations_with_frequent_reports])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
