"""

"""
import os
import datetime
import logging

import dateutil.parser
import pandas

from gather_weather_data.husconet import GermanWinterTime


PROJECT_ROOT_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir
)

PROCESSED_DATA_DIR = os.path.join(
    PROJECT_ROOT_DIR,
    "processed_data"
)


def _search_summary_file(station, start_date, end_date):
    searched_station_summary_file_name = None
    for station_summary_file_name in os.listdir(os.path.join(PROCESSED_DATA_DIR, "station_summaries")):
        if not station_summary_file_name.endswith(".csv"):
            continue
        if not station_summary_file_name.startswith(station):
            continue
        station_summary_file_name = station_summary_file_name[:-4]  # cut of '.csv'
        file_name_parts = station_summary_file_name.split("_")
        if len(file_name_parts) == 1:
            searched_station_summary_file_name = file_name_parts[0] + ".csv"
            break
        elif len(file_name_parts) == 3:
            station_part, start_date_span_text, end_date_span_text = file_name_parts
            if station_part != station:
                continue
            if start_date is None and end_date is None:
                searched_station_summary_file_name = station_summary_file_name + ".csv"
                break
            start_date_span = datetime.datetime.strptime(start_date_span_text, "%Y%m%d")
            start_date_span = start_date_span.replace(tzinfo=start_date.tzinfo)
            start_date_span = start_date_span.replace(hour=0, minute=0)
            end_date_span = datetime.datetime.strptime(end_date_span_text, "%Y%m%d")
            end_date_span = end_date_span.replace(tzinfo=end_date.tzinfo)
            end_date_span = end_date_span.replace(hour=23, minute=59)
            if start_date_span <= start_date and end_date_span >= end_date:
                searched_station_summary_file_name = station_summary_file_name + ".csv"
                break
    return searched_station_summary_file_name


def load_airport(airport_name, start_date, end_date):
    if isinstance(start_date, str):
        start_date = dateutil.parser.parse(start_date)
    if isinstance(end_date, str):
        end_date = dateutil.parser.parse(end_date)
    searched_summary_file_name = _search_summary_file(airport_name, start_date, end_date)
    if searched_summary_file_name is None:
        logging.debug("airport file not found")
        return
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "station_summaries",
        searched_summary_file_name
    )
    station_df = pandas.read_csv(
        csv_file,
        index_col="datetime",
        parse_dates=["datetime"]
    )
    station_df = station_df.tz_localize("UTC").tz_convert(GermanWinterTime()).tz_localize(None)

    df_year = pandas.DataFrame(index=pandas.date_range(start_date, end_date + datetime.timedelta(days=1), freq='T', name="datetime"))
    station_df = station_df.join([df_year], how="outer")  # insert nans everywhere, also in the beginning.
    station_df = station_df.groupby(station_df.index).first()  # remove duplicates
    station_df.temperature = station_df.temperature.ffill(limit=30)  # 30min decay
    return station_df
