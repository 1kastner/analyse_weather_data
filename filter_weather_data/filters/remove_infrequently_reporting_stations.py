"""

Checks if enough data is provided during the given period.
The check removes entries when the reports were too infrequent.
"""

import logging
import calendar

import numpy

from gather_weather_data.husconet import GermanWinterTime
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
    station_df.info()
    for year in station_df.index.year.unique():
        year_key = str(year)
        year_df = station_df.loc[year_key:year_key]  # avoids getting a series if a single entry exists
        if year_df.empty or year_df.temperature.count() == 0:
            logging.debug(station_name + " is an empty data frame")
            return False
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key:month_key]  # avoids getting a series if a single entry exists
            if month_df.empty or month_df.temperature.count() < 22:  # obs for less than 22d
                logging.debug(month_key + " " + station_name + " got less than 22 entries - must be less than 80%")
                return False
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = month_df.loc[day_key:day_key]  # avoids getting a series if a single entry exists
                if day_df.temperature.count() < 19 or len(day_df.index.hour.unique()) < 19:
                    # logging.debug("remove {day_key}".format(day_key=day_key))
                    station_df.loc[day_key:day_key, "temperature"] = numpy.nan
            station_df.dropna(axis='index', how='any', subset=["temperature"], inplace=True)
            eighty_percent_of_month = int(round(calendar.monthrange(year, month)[1] * .8))
            days_with_enough_reports = len(month_df.index.day.unique())
            if days_with_enough_reports < eighty_percent_of_month:
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
        # logging.debug("infrequent " + station_dict["name"])
        if check_station(station_dict):
            filtered_stations.append(station_dict)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00"
    end_date = "2016-12-31T00:00:00"
    time_zone = GermanWinterTime()
    stations = ['IHAMBURG42']  # , 'IBNNINGS2', 'IHAMBURG1795']
    station_repository = StationRepository()
    station_dicts = [station_repository.load_station(station, start_date, end_date, time_zone) for station in stations]
    station_dicts = [station_dict for station_dict in station_dicts if station_dict is not None]
    print()
    print("Before filtering: ")
    for station_dict in station_dicts:
        print(station_dict["name"])
        station_dict["data_frame"].info()
        print(station_dict["data_frame"].describe())
    stations_with_frequent_reports = filter_stations(station_dicts)
    print()
    print("After filtering: ")
    for station_dict in stations_with_frequent_reports:
        print(station_dict["name"])
        station_dict["data_frame"].info()
        print(station_dict["data_frame"].describe())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
