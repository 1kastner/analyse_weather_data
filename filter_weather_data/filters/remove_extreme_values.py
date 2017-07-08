"""

Checks if enough data is provided during the given period.
The check removes entries when the reports were too infrequent.
"""

import logging

import numpy

from . import StationRepository
from gather_weather_data.husconet import GermanWinterTime


def check_station(station_dict, minimum_temperature):
    """
    This function modifies the handed in station_df

    :param minimum_temperature: the lowest acceptable temperature
    :param station_dict: The station dict
    :return: Only values inside the realistic range
    """
    station_df = station_dict["data_frame"]

    smaller_than_minimum = station_df.loc[station_df.temperature < minimum_temperature]
    if not smaller_than_minimum.empty:
        logging.debug(smaller_than_minimum.describe())

    station_df.loc[station_df.temperature < minimum_temperature] = numpy.nan

    if station_df.empty:
        return False
    elif station_df.temperature.count() == 0:
        return False
    else:
        return True


def filter_stations(station_dicts, minimum_temperature):
    """

    :param station_dicts: The station dicts
    """
    filtered_station_dicts = []
    for station_dict in station_dicts:
        # logging.debug("extreme " + station_dict["name"])
        if check_station(station_dict, minimum_temperature):
            filtered_station_dicts.append(station_dict)
    return filtered_station_dicts


def demo():
    start_date = "2016-01-01T00:00:00"
    end_date = "2016-12-31T00:00:00"
    time_zone = GermanWinterTime()
    stations = ['IHAMBURG69', 'IBNNINGS2', 'IHAMBURG1795']
    station_repository = StationRepository()
    station_dicts = filter(lambda x: x is not None, [station_repository.load_station(station, start_date, end_date,
                                                                                     time_zone)
                                                     for station in stations])
    stations_with_frequent_reports = filter_stations(station_dicts, -32.1)
    print([station_dict["name"] for station_dict in stations_with_frequent_reports])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
