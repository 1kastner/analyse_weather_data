#! /usr/bin/python3
"""

Runt it with
-m descriptive_statistics.wunderground_station_types
for the main script.
"""

import sys
import os.path
import logging
import json
import collections

from filter_weather_data.filters import StationRepository
from . import WUNDERGROUND_RAW_DATA_DIR
from . import FILTERED_STATIONS_DIR


def setup_logger():
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    path_to_file_to_log_to = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "log",
        "wunderground_station_types.log"
    )
    file_handler = logging.FileHandler(path_to_file_to_log_to)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    logging.info("### Start new logging")
    return log


def get_software_type(station):
    """
    Check day by day and return the first result.

    Assumptions:
    - The software does not change
    - We can derive the hardware from the software
    - So we can count netatmo stations and compare that number to the amount of other station types
    
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
    logging.info("total: %i" % len(stations_df))
    for station in stations_df.index:
        software_types.append(get_software_type(station))
    for software_type, count in collections.Counter(software_types).items():
        logging.info("  %s : %i" % (software_type, count))


def run():
    start = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_with_valid_position.csv")
    frequent_reports = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_frequent.csv")
    only_outdoor = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_outdoor.csv")
    outdoor_and_shaded = os.path.join(FILTERED_STATIONS_DIR, "station_dicts_shaded.csv")

    logging.info("start")
    gather_statistics(start)

    logging.info("frequent_reports")
    gather_statistics(frequent_reports)

    logging.info("only_outdoor")
    gather_statistics(only_outdoor)

    logging.info("outdoor_and_shaded")
    gather_statistics(outdoor_and_shaded)


if __name__ == "__main__":
    setup_logger()
    run()
