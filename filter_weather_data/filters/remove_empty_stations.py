"""
Checks if any data is provided during the given period.
"""

import logging

import dateutil.parser

from . import load_station


def check_station(station_df, start_date, end_date):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785
    
    :param station_df: The station data frame (pandas)
    :param start_date: The date to start (included), naive date interpreted as UTC  
    :param end_date: The date to stop (included), naive date interpreted as UTC
    """
    return station_df.empty or station_df.temperature.count() == 0


def filter_stations(stations, start_date, end_date):
    """

    :param stations: The name of the stations, e.g. ['IHAMBURG69']
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    """
    filtered_stations = []
    for station in stations:
        if check_station(station, start_date, end_date):
            filtered_stations.append(station)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    station_dfs = [load_station(station, start_date, end_date) for station in ['IHAMBURG69', 'IBNNINGS2']]
    stations_with_data = filter_stations(station_dfs, start_date, end_date)
    print(stations_with_data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
