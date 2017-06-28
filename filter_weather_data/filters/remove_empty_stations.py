"""
Checks if any data is provided during the given period.

Run as script with '-m filter_weather_data.filters.remove_empty_stations'
"""

import logging

from . import StationRepository


def check_station(station_df, start_date, end_date):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785
    
    :param station_df: The station data frame (pandas)
    :param start_date: The date to start (included), naive date interpreted as UTC  
    :param end_date: The date to stop (included), naive date interpreted as UTC
    """
    return station_df.empty or station_df.temperature.count() == 0


def filter_stations(station_dicts, start_date, end_date):
    """

    :param station_dicts: The station dicts
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    """
    filtered_stations = []
    for station_dict in station_dicts:
        if check_station(station_dict["data_frame"], start_date, end_date):
            filtered_stations.append(station_dict["name"])
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    station_repository = StationRepository()
    stations = ['IHAMBURG69', 'IBNNINGS2']
    station_dicts = [station_repository.load_station(station, start_date, end_date) for station in stations]
    stations_with_data = filter_stations(station_dicts, start_date, end_date)
    print(stations_with_data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
