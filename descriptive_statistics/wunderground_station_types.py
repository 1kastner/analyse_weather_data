#! /usr/bin/python3
"""

Runt it with
-m descriptive_statistics.wunderground_station_types
for the main script.
"""

import os.path
import logging
import json
import collections

from filter_weather_data.filters import StationRepository
from . import WUNDERGROUND_RAW_DATA_DIR
from . import FILTERED_STATIONS_DIR


def get_software_type(station):
    """
    
    :param station: The station name
    :type station: str
    :return: The station type
    :rtype: str
    """
    station_dir = os.path.join(
        WUNDERGROUND_RAW_DATA_DIR,
        station
    )
    if not os.path.isdir(station_dir):
        logging.debug("path '{station_dir}' does not exist".format(station_dir=station_dir))
        return
    days = [file_name for file_name in os.listdir(station_dir) if file_name.endswith(".json")]
    if not days:
        logging.debug("No sample day could be found.")
        return

    for day in days:
        day_path = os.path.join(
            station_dir,
            day
        )
        with open(day_path) as f:
            res = json.load(f)
        observations = res["history"]["observations"]
        if observations:
            sample_observation = observations[0]
            if "softwaretype" in sample_observation:
                return sample_observation["softwaretype"]  # e.g. Netatmo
    return "unknown"


def gather_statistics(private_weather_stations_file_name):
    station_repository = StationRepository(private_weather_stations_file_name)
    software_types = []
    stations_df = station_repository.get_all_stations()
    for station in stations_df.index:
        software_types.append(get_software_type(station))
    for software_type, count in collections.Counter(software_types).items():
        print(software_type, ":", count)


def run():
    start = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_with_valid_position.csv")
    frequent_reports = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_frequent.csv")
    only_outdoor = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_outdoor.csv")
    outdoor_and_shaded = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_shaded.csv")

    print("start")
    gather_statistics(start)
    print()

    print("frequent_reports")
    gather_statistics(frequent_reports)
    print()

    print("only_outdoor")
    gather_statistics(only_outdoor)
    print()

    print("outdoor_and_shaded")
    gather_statistics(outdoor_and_shaded)
    print()

    
if __name__ == "__main__":
    run()
