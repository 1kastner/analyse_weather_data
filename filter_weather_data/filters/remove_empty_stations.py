"""
Checks if any data is provided during the given period.

Run as script with '-m filter_weather_data.filters.remove_empty_stations'
"""

import logging

from . import StationRepository


def check_station(station_df):
    """
    Be aware that start and end date are not time zone sensitive when provided as a string, see
    https://github.com/pandas-dev/pandas/issues/16785
    
    :param station_df: The station data frame (pandas)
    """
    empty_station = station_df.empty or station_df.temperature.count() == 0
    return not empty_station


def filter_stations(station_dicts):
    """

    :param station_dicts: The station dicts
    """
    filtered_stations = []
    for station_dict in station_dicts:
        logging.debug("empty " + station_dict["name"])
        if check_station(station_dict["data_frame"]):
            filtered_stations.append(station_dict)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    station_repository = StationRepository()
    stations = ['IHAMBURG69', 'IBNNINGS2']
    station_dicts = [station_repository.load_station(station, start_date, end_date) for station in stations]
    stations_with_data = filter_stations(station_dicts)
    print([station_dict["name"] for station_dict in stations_with_data])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
