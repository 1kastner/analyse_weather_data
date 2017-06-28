"""
Downloads all available weather data for a given time span.

'-m gather_weather_data.wunderground.download_private_weather_data'
"""

import os
import datetime
import time
import logging
import json

import requests

from . import WUNDERGROUND_RAW_DATA_DIR
from . import WundergroundProperties
from . import get_all_stations


def get_data_for_day(station, day, force_overwrite):
    """
    
    :param force_overwrite: If not activated, existing data is kept instead of overwriting
    :param station: The station id
    :param day: The day
    :return: Was the download successful
    """
    yyyymmdd = day.strftime("%Y%m%d")
    yyyy_mm_dd = day.strftime("%Y-%m-%d")
    logging.info(day.strftime("%Y-%m-%d"))
    station_directory = os.path.join(WUNDERGROUND_RAW_DATA_DIR, station)
    if not os.path.isdir(station_directory):
        os.mkdir(station_directory)

    # Traditional format
    json_file_name = "{station_id}_{YYYYMMDD}.json".format(station_id=station, YYYYMMDD=yyyymmdd)
    json_file_path = os.path.join(station_directory, json_file_name)
    if os.path.isfile(json_file_path) and os.path.getsize(json_file_path) and not force_overwrite:
        logging.info("skip download for {station} at {yyyy_mm_dd}".format(station=station, yyyy_mm_dd=yyyy_mm_dd))
        return True

    # Format of foreign project
    json_file_name = "{station_id}_{YYYY_MM_DD}.json".format(station_id=station, YYYY_MM_DD=yyyy_mm_dd)
    json_file_path = os.path.join(station_directory, json_file_name)
    if os.path.isfile(json_file_path) and os.path.getsize(json_file_path) and not force_overwrite:
        logging.info("skip download for {station} at {yyyy_mm_dd}".format(station=station, yyyy_mm_dd=yyyy_mm_dd))
        return True

    history_query = "history_{YYYYMMDD}/q/pws:{station_id}.json".format(YYYYMMDD=yyyymmdd, station_id=station)
    query_url = WundergroundProperties.get_api_url() + history_query
    response = requests.get(query_url)
    if response.status_code != 200:
        time.sleep(0.5)
        response = requests.get(query_url)
        if response.status_code != 200:
            logging.warning("error for {station} at {YYYYMMDD}".format(station=station, YYYYMMDD=yyyymmdd))
    if response.text:
        try:
            json_payload = response.json()
            if "response" in json_payload and "error" in json_payload["response"]:
                error_description = json_payload["response"]["error"]["description"]
                logging.warning("error for {station} at {YYYYMMDD}: {error_description}".format(station=station,
                                YYYYMMDD=yyyymmdd, error_description=error_description))
            else:
                with open(json_file_path, "w") as f:
                    f.write(response.text)
                    return True
        except ValueError:
            logging.warning("error for {station} at {day}: invalid json".format(station=station, day=yyyymmdd))
    return False


def get_station_data_for_time_span(station, start_date, end_date, force_overwrite):
    """
    
    :param force_overwrite: If not activated, existing data is kept instead of overwriting
    :type force_overwrite: bool
    :param station: The station to check
    :type station: str
    :param start_date: The start date (included)
    :type start_date: datetime.date
    :param end_date: The end date (included)
    :type end_date: datetime.date
    """
    date_to_check = start_date
    success = False
    while date_to_check <= end_date:
        success = get_data_for_day(station, date_to_check, force_overwrite)
        if not success:
            logging.info("An error occurred - please re-run once the servers are up again.")
            break
        date_to_check = date_to_check + datetime.timedelta(days=1)
    return success


def demo():
    """
    Downloads all data available for each station during the year 2016.
    """
    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2016, 1, 31)
    stations = get_all_stations()
    for station in stations:
        logging.info("Station: {station}".format(station=station))
        success = get_station_data_for_time_span(station, start_date, end_date, force_overwrite=False)
        if not success:
            break


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo()
