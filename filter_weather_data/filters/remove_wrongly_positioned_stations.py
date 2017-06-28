"""

"""
import logging

from . import get_all_stations


def check_station(station, meta_info_df):
    """
    
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param meta_info_df: The location of the station
    :return: Is it at a location which corresponds to NaN
    """
    lat, lon = meta_info_df.loc[station]
    if lat == 0 and lon == 0:
        logging.debug(station, "is located at 0, 0")
        return False
    else:
        return True


def filter_stations(stations):
    """

    :param stations: The name of the stations, e.g. ['IHAMBURG69']
    """
    meta_info_df = get_all_stations()

    # entries which are at the same coordinates
    duplicated_indices = meta_info_df.duplicated(["lat", "lon"], keep=False)
    duplicated_entries = meta_info_df[duplicated_indices]
    logging.debug(duplicated_entries)

    meta_info_df = meta_info_df[~duplicated_indices]

    filtered_stations = []
    for station in stations:
        print(station)
        print(station in meta_info_df.index)
        if station in meta_info_df.index and check_station(station, meta_info_df):
            print(check_station(station, meta_info_df))
            filtered_stations.append(station)
    return filtered_stations


def demo():
    stations = ['IHAMBURG69', 'IHAMBURG44']
    stations_with_position = filter_stations(stations)
    print(stations_with_position)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
