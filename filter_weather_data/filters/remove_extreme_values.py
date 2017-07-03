"""

Checks if enough data is provided during the given period.
The check removes entries when the reports were too infrequent.
"""

import logging

import numpy

from . import StationRepository


def check_station(station_dict, minimum, maximum):
    """
    This function modifies the handed in station_df

    :param station_dict: The station dict
    :param minimum: The minimum value which is acceptable
    :param maximum: The maximum value which is acceptable
    :return: Only values inside the realistic range
    """
    station_df = station_dict["data_frame"]
    below_minimum = station_df.loc[station_df.temperature < minimum]
    if below_minimum.temperature.count():
        logging.debug("below minimum")
        below_minimum.info()
        logging.debug(below_minimum.head(5))
    station_df.loc[station_df.temperature < minimum] = numpy.nan

    above_maximum = station_df.loc[station_df.temperature > maximum]
    if above_maximum.temperature.count():
        logging.debug("above maximum")
        above_maximum.info()
        logging.debug(above_maximum.head(5))
    station_df.loc[station_df.temperature > maximum] = numpy.nan


def filter_stations(station_dicts, minimum, maximum):
    """

    :param minimum: The minimum value which is acceptable
    :param maximum: The maximum value which is acceptable
    :param station_dicts: The station dicts
    """
    for station_dict in station_dicts:
        logging.debug("extreme " + station_dict["name"])
        check_station(station_dict, minimum, maximum)
    return station_dicts


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    time_zone = "CET"
    stations = ['IHAMBURG69', 'IBNNINGS2', 'IHAMBURG1795']
    station_repository = StationRepository()
    station_dicts = filter(lambda x: x is not None, [station_repository.load_station(station, start_date, end_date,
                                                                                     time_zone, minutely=True)
                                                     for station in stations])
    stations_with_frequent_reports = filter_stations(station_dicts, -29.1 - 20, 37.3 + 20)
    print([station_dict["name"] for station_dict in stations_with_frequent_reports])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
