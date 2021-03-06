"""
Summarize all downloaded private weather station data files.

Uses UTC time zone.

Use
-m gather_weather_data.wunderground.summarize_raw_data
to run the demo
"""

import os
import json
import datetime
import pytz
import logging
import re
import pandas

from . import WUNDERGROUND_RAW_DATA_DIR
from . import PROCESSED_DATA_DIR
from . import get_all_stations


def _parse_utc_date(utc_date_json):
    """
    
    :param utc_date_json: A json representation of a utc date according to the weather underground documentation
    :return: The corresponding datetime object
    :rtype: ``datetime.datetime``
    """
    year = int(utc_date_json["year"])
    month = int(utc_date_json["mon"])
    day = int(utc_date_json["mday"])
    hour = int(utc_date_json["hour"])
    minute = int(utc_date_json["min"])
    return datetime.datetime(year, month, day, hour, minute, tzinfo=pytz.utc)


def _cast_number(val, imperial=""):
    """
    
    :param val: A metric value as it is supplied by wunderground
    :param imperial: The imperial pendent to the value, used for determining NaNs
    :return: An integer or float
    """
    if val == "" or val == "N/A":
        return ""
    if imperial:
        if (imperial == "-99.99" or imperial == "-9999" or imperial == "-999.0"
                or imperial == "-999.9" or imperial == "-9999.0"):
            return ""
    if re.match(r"^-?\d+$", val):  # an integer
        return float(val)
    if re.match(r"^-?\d*\.\d+$", val):  # a float
        return float(val)
    else:
        raise RuntimeError("val not a number: " + repr(val))


def _get_file_name(station, day, file_ending):
    """

    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to pick the json from
    :param file_ending: preferably 'json' or 'csv'
    :return: The json file name
    :rtype: str
    """
    return station + "_" + day.strftime("%Y-%m-%d") + "." + file_ending


HEADER_FORMAT = ("{datetime},{temperature},{dewpoint},{windspeed},{windgust},{winddirection},{pressure},{humidity},"
                 "{precipitation}")


def _get_header():
    """
    
    :return: Formatted header complying csv standards
    """
    return HEADER_FORMAT.replace("{", "").replace("}", "")


def _get_data_for_single_day(station, day):
    """
    At the current time the day provided is interpreted as local time at wunderground.
    
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to pick the json from
    :return: A valid csv file content with header
    :rtype: str
    """
    json_file_name = _get_file_name(station, day, 'json')
    json_file_path = os.path.join(WUNDERGROUND_RAW_DATA_DIR, station, json_file_name)
    if not os.path.isfile(json_file_path):
        # search for files of other project
        json_file_name = station + "_" + day.strftime("%Y%m%d") + ".json"
        json_file_path = os.path.join(WUNDERGROUND_RAW_DATA_DIR, station, json_file_name)
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
    observations = []

    header = _get_header()
    observations.append(header)

    for raw_observation in raw_json_weather_data["history"]["observations"]:
        observation = {}
        utc_date = _parse_utc_date(raw_observation["utcdate"])
        observation["datetime"] = utc_date.isoformat()
        observation["temperature"] = _cast_number(raw_observation["tempm"])
        observation["dewpoint"] = _cast_number(raw_observation["dewptm"])
        observation["windspeed"] = _cast_number(raw_observation["wspdm"], raw_observation["wspdi"])
        observation["windgust"] = _cast_number(raw_observation["wgustm"], raw_observation["wgusti"])
        observation["winddirection"] = _cast_number(raw_observation["wdird"], raw_observation["wdird"])
        observation["pressure"] = _cast_number(raw_observation["pressurem"])
        observation["humidity"] = _cast_number(raw_observation["hum"])
        if "precip_ratem" in raw_observation:
            observation["precipitation"] = _cast_number(raw_observation["precip_ratem"],
                                                        raw_observation["precip_ratei"])
        else:
            observation["precipitation"] = ""
        observations.append(HEADER_FORMAT.format(**observation))
    return "\n".join(observations)


def _create_csv_from_json(station, day, force_overwrite):
    """
    
    :param force_overwrite: Whether to overwrite old daily summary files.
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to pick the json from
    """
    processed_station_dir = os.path.join(WUNDERGROUND_RAW_DATA_DIR, station)
    if not os.path.isdir(processed_station_dir):
        os.mkdir(processed_station_dir)
    csv_path = os.path.join(processed_station_dir, _get_file_name(station, day, 'csv'))
    if os.path.isfile(csv_path) and os.path.getsize(csv_path) and not force_overwrite:
        logging.info("skip " + csv_path)
        return
    with open(csv_path, "w") as f:
        csv_file_content = _get_data_for_single_day(station, day)
        if csv_file_content is not None:
            f.write(csv_file_content)
        else:
            f.write(_get_header())


def create_daily_summaries_for_time_span(station, start_date, end_date, force_overwrite):
    """
    
    :param force_overwrite: Whether to overwrite old daily summary files.
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    :return: 
    """
    date_to_check = start_date
    while date_to_check <= end_date:
        _create_csv_from_json(station, date_to_check, force_overwrite)
        date_to_check = date_to_check + datetime.timedelta(days=1)


def _open_daily_summary(station, day):
    """
    
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to get the summary for (can be naive)
    :return: The corresponding data frame
    """
    csv_file = os.path.join(WUNDERGROUND_RAW_DATA_DIR, station, _get_file_name(station, day, "csv"))
    data_frame = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    return data_frame


def join_daily_summaries(station, start_date, end_date, force_overwrite):
    """
    
    :param station: 
    :param start_date: 
    :param end_date: 
    :param force_overwrite: 
    :return: 
    """
    date_to_check = start_date
    span_summary_file_name = station + "_" + start_date.strftime("%Y%m%d") + "_" + end_date.strftime("%Y%m%d") + ".csv"
    output_dir = os.path.join(PROCESSED_DATA_DIR, "station_summaries")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    span_summary_path = os.path.join(output_dir, span_summary_file_name)
    if os.path.isdir(span_summary_path) and not force_overwrite:
        logging.info("skip " + span_summary_path)
        return
    data_frame = _open_daily_summary(station, start_date)
    start_date += datetime.timedelta(days=1)
    while date_to_check <= end_date:
        data_frame_next = _open_daily_summary(station, date_to_check)
        data_frame = data_frame.append(data_frame_next)
        date_to_check = date_to_check + datetime.timedelta(days=1)

    # remove duplicates (happens if same entry exists for two days)
    data_frame.groupby(data_frame.index).first()
    data_frame.sort_index(inplace=True)

    data_frame.to_csv(span_summary_path)


def demo():
    stations = get_all_stations()
    for station in stations:
        logging.info(station)
        start_date = datetime.datetime(2016, 1, 1)
        end_date = datetime.datetime(2016, 12, 31)
        logging.info("create daily summaries")
        create_daily_summaries_for_time_span(station, start_date, end_date, False)
        logging.info("create time span summary")
        join_daily_summaries(station, start_date, end_date, True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo()
