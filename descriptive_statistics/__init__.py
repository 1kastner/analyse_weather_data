"""
"""

import os

WUNDERGROUND_RAW_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "wunderground_raw_data"
)

PROCESSED_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir,
    "processed_data"
)

FILTERED_STATIONS_DIR = os.path.join(
    PROCESSED_DATA_DIR,
    "filtered_stations"
)
