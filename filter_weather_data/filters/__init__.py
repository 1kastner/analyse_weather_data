"""
Group some common functionality like loading data frames.
"""

import os
import datetime
import logging

import pandas
import dateutil.parser


PROCESSED_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        os.pardir,
        "processed_data"
)


def get_all_stations():
    """

    :return: All stations which have been detected before
    """
    csv_file = os.path.join(PROCESSED_DATA_DIR, "private_weather_stations.csv")
    if not os.path.isfile(csv_file):
        raise RuntimeError("No private weather station list found. Run 'list_private_weather_stations.py' before")
    stations_df = pandas.read_csv(csv_file, index_col="station")
    return stations_df


def load_station(station, start_date, end_date):
    """
    This only looks at the dates and returns the corresponding summary (assuming naive dates, overlapping at midnight
    is ignored).
    
    :param station: The station to load
    :param start_date: The earliest day which must be included (potentially earlier)
    :type start_date: str | datetime.datetime
    :param end_date: The latest day which must be included (potentially later)
    :type end_date: str | datetime.datetime
    :return: The searched data frame
    :rtype: ``pandas.DataFrame``
    """
    if type(start_date) is str:
        start_date = dateutil.parser.parse(start_date)
    if type(end_date) is str:
        end_date = dateutil.parser.parse(end_date)
    input_dir = os.path.join(PROCESSED_DATA_DIR, "station_summaries")
    searched_station_summary_file_name = None
    for station_summary_file_name in os.listdir(input_dir):
        if not station_summary_file_name.endswith(".csv"):
            continue
        if not station_summary_file_name.startswith(station):
            continue
        station_summary_file_name = station_summary_file_name[:-4]  # cut of '.csv'
        station, start_date_span_text, end_date_span_text = station_summary_file_name.split("_")
        start_date_span = datetime.datetime.strptime(start_date_span_text, "%Y%m%d").replace(tzinfo=start_date.tzinfo)
        end_date_span = datetime.datetime.strptime(end_date_span_text, "%Y%m%d").replace(tzinfo=end_date.tzinfo)
        if start_date_span <= start_date and end_date_span >= end_date:
            searched_station_summary_file_name = station_summary_file_name + ".csv"
    if searched_station_summary_file_name is None:
        logging.error("No file found: '{station}' ranging between {start_date} and {end_date}".format(
            station=station, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d")
        ))
        return None
    else:
        csv_file = os.path.join(input_dir, searched_station_summary_file_name)
        return pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
