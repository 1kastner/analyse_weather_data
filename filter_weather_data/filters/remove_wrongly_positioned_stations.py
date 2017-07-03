"""

Run with 
-m filter_weather_data.filters.remove_wrongly_positioned_stations
if you want to see the demo.
"""

import logging

from . import StationRepository


def check_station(station_dict, meta_info_df):
    """
    
    :param station_dict: The station dict
    :param meta_info_df: The location of the station
    :return: Is it at a location which corresponds to NaN
    """
    station_name = station_dict["name"]
    lat, lon = meta_info_df.loc[station_name]
    if lat == 0 and lon == 0:
        logging.debug(station_name, "is located at 0, 0")
        return False
    else:
        return True


def filter_stations(station_dicts, meta_info_df):
    """

    :param station_dicts: The station dict
    :param meta_info_df: The meta information
    """

    # entries which are at the same coordinates
    duplicated_indices = meta_info_df.duplicated(["lat", "lon"], keep=False)
    duplicated_entries = meta_info_df[duplicated_indices]
    logging.debug(duplicated_entries)

    meta_info_df = meta_info_df[~duplicated_indices]

    filtered_stations = []
    for station_dict in station_dicts:
        logging.debug("position " + station_dict["name"])
        if station_dict["name"] in meta_info_df.index and check_station(station_dict, meta_info_df):
            filtered_stations.append(station_dict)
    return filtered_stations


def demo():
    station_repository = StationRepository()
    meta_info_df = station_repository.get_all_stations()
    stations = ['IHAMBURG69', 'IBNNINGS2']
    station_dicts = [{"name": station} for station in stations]
    stations_at_good_position = filter_stations(station_dicts, meta_info_df)
    print([station_dict["name"] for station_dict in stations_at_good_position])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
