"""
Summarize all downloaded airport weather station data files.

Uses UTC time zone.

Use
-m gather_weather_data.wunderground.summarize_raw_airport_data
to run the demo
"""

import os
import json
import datetime
import logging

import numpy
import pandas

import metar.Metar  # needs https://github.com/tomp/python-metar/pull/25 to work stable

from . import WUNDERGROUND_RAW_AIRPORT_DATA_DIR
from . import PROCESSED_DATA_DIR
from .summarize_raw_data import _parse_utc_date
from .summarize_raw_data import _cast_number
from .summarize_raw_data import _get_file_name


HEADER_FORMAT = ("{datetime},{temperature},{dewpoint},{windspeed},{windgust},{winddirection},{pressure},{humidity},"
                 "{precipitation},{cloudcover}")


def _get_header():
    """
    
    :return: Formatted header complying csv standards
    """
    return HEADER_FORMAT.replace("{", "").replace("}", "")


def max_of_total_order(collection_of_interest, given_total_order):
    """
    
    :param collection_of_interest: Find the maximum in this collection
    :param given_total_order: Describe the total order to use on the collection
    :return: max element
    """
    l = [given_total_order.index(e) for e in collection_of_interest]
    return given_total_order[max(l)]


def get_cloud_cover(metar_string, date_of_observation):
    """
    This needs a small modification as described in https://github.com/tomp/python-metar/pull/25
    
    :param metar_string: A classical meteorological METAR
    :param date_of_observation: Used to parse the metar at hand
    :return: The cloud cover name
    """
    d = date_of_observation
    m = metar.Metar.Metar(
        metar_string,
        d.month,
        d.year,
        drop_unsupported_observations=True
    )
    cloud_cover = "CAVOC"  # 0 octas
    if not m.sky:
        return cloud_cover
    else:
        sorted_possible_cloud_covers = [
            "SKC", "CLR", "NSC",  # 0 octas
            "FEW",  # 1-2 octas
            "SCT",  # 3-4 octas
            "BKN",  # 5-7 octas
            "OVC",  # 8 octas
            "VV",  # clouds can not be seen because of fog or rain
        ]
        sky_covers = [cover for (cover, height, cloud) in m.sky]
        return max_of_total_order(sky_covers, sorted_possible_cloud_covers)


def _get_data_for_single_day(station, day):
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
        if raw_observation["metar"].startswith("METAR"):  # some other record
            observation["cloudcover"] = get_cloud_cover(raw_observation["metar"], utc_date)
        else:
            observation["cloudcover"] = numpy.nan
        observations.append(HEADER_FORMAT.format(**observation))
    return "\n".join(observations)


def _open_daily_summary(station, day):
    """

    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to get the summary for (can be naive)
    :return: The corresponding data frame
    """
    csv_file = os.path.join(WUNDERGROUND_RAW_AIRPORT_DATA_DIR, station, _get_file_name(station, day, "csv"))
    data_frame = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    return data_frame


def _create_csv_from_json(station, day, force_overwrite):
    """
    
    :param force_overwrite: Whether to overwrite old daily summary files.
    :param station: The name of the station, e.g. 'IHAMBURG69'
    :param day: The day to pick the json from
    """
    processed_station_dir = os.path.join(WUNDERGROUND_RAW_AIRPORT_DATA_DIR, station)
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


def demo():
    stations = ["EDDH"]
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
