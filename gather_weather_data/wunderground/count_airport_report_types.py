"""
Extract reports and count the report types.

Uses UTC time zone.

Use
-m gather_weather_data.wunderground.count_airport_report_types
to run the demo
"""

import os
import json
import datetime
import logging
import collections


from . import WUNDERGROUND_RAW_AIRPORT_DATA_DIR
from .summarize_raw_data import _get_file_name


def _get_reports_for_single_day(station, day):
    """
    At the current time the day provided is interpreted as local time at wunderground.

    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to pick the json from
    :return: A valid csv file content with header
    :rtype: str
    """
    json_file_name = _get_file_name(station, day, 'json')
    json_file_path = os.path.join(WUNDERGROUND_RAW_AIRPORT_DATA_DIR, station, json_file_name)
    if not os.path.isfile(json_file_path):
        # search for files of other project
        json_file_name = station + "_" + day.strftime("%Y%m%d") + ".json"
        json_file_path = os.path.join(WUNDERGROUND_RAW_AIRPORT_DATA_DIR, station, json_file_name)
    if not os.path.isfile(json_file_path):
        # search for files created by yet another project
        json_file_name = day.strftime("%Y-%m-%d") + ".json"
        json_file_path = os.path.join(WUNDERGROUND_RAW_AIRPORT_DATA_DIR, station, json_file_name)
    if not os.path.isfile(json_file_path):
        logging.warning("missing input file: " + json_file_path)
        return
    if os.path.getsize(json_file_path) == 0:
        logging.warning("encountered an empty file: ", json_file_path)
        os.remove(json_file_path)
        return
    with open(json_file_path) as f:
        raw_json_weather_data = json.load(f)

    # These are the relevant observations we want to keep
    report_types = collections.Counter()

    for raw_observation in raw_json_weather_data["history"]["observations"]:
        weather_report = raw_observation["metar"]
        report_type = weather_report.split(" ")[0]
        report_types[report_type] += 1
    return report_types


def count_report_types(station, start_date, end_date):
    """

    :param station:
    :param start_date:
    :param end_date:
    :param force_overwrite:
    :return:
    """
    date_to_check = start_date
    report_types_global = _get_reports_for_single_day(station, date_to_check)
    start_date += datetime.timedelta(days=1)
    while date_to_check <= end_date:
        report_types_for_date_to_check = _get_reports_for_single_day(station, date_to_check)
        report_types_global += report_types_for_date_to_check
        date_to_check = date_to_check + datetime.timedelta(days=1)
    print(report_types_global)


def demo():
    stations = [
        "EDDH"
    ]
    for station in stations:
        logging.info(station)
        start_date = datetime.datetime(2016, 1, 1)
        end_date = datetime.datetime(2016, 12, 31)
        count_report_types(station, start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo()
