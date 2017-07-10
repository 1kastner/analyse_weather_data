"""
"""

import os

PROJECT_ROOT_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir
)

WUNDERGROUND_RAW_DATA_DIR = os.path.join(
    PROJECT_ROOT_DIR,
    "wunderground_raw_data"
)

PROCESSED_DATA_DIR = os.path.join(
    PROJECT_ROOT_DIR,
    "processed_data"
)

FILTERED_STATIONS_DIR = os.path.join(
    PROCESSED_DATA_DIR,
    "filtered_stations"
)
