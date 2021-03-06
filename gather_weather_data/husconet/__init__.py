"""

"""

import os
import datetime
import logging

import dateutil.parser
import pandas


HUSCONET_RAW_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir,
    os.pardir,
    "husconet_raw_data"
)

PROCESSED_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir,
    os.pardir,
    "processed_data"
)

HUSCONET_STATIONS = [
    "HCM",
    "IHM",
    "LGM",
    "LWM",
    "RIM",
    "SGM",
    "SWM",
    "WGM",
    "WWM",
    "ZOM",
]

OFFICIAL_HUSCONET_NAME = {
    "HCM": "HC",
    "IHM": "Nst",
    "LGM": "LaG",
    "LWM": "LaW",
    "RIM": "Ri",
    "SGM": "StG",
    "SWM": "StW",
    "WGM": "WiG",
    "WWM": "WiW",
    "ZOM": "Bi",
}


class GermanWinterTime(datetime.tzinfo):
    """
    The German winter time (no daylight saving time).
    
    Used in the HUSCONET project.
    """

    def utcoffset(self, dt):
        return datetime.timedelta(hours=1)

    def tzname(self, dt):
        return "GermanWinterTime"

    def dst(self, dt):
        return datetime.timedelta(0)


def load_husconet_temperature_average(start_date, end_date, excluded_husconet_stations=None):
    if excluded_husconet_stations is not None:
        excluded_husconet_stations = list(excluded_husconet_stations)
        excluded_husconet_stations.sort()
        file_name = "husconet_average_temperature_exclude_{excluded}.csv".format(
            excluded="_".join(excluded_husconet_stations))
    else:
        file_name = "husconet_average_temperature.csv"
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "husconet",
        file_name
    )
    logging.debug("loading for temperature average: '{file}'".format(file=csv_file))
    return load_husconet_file(csv_file, start_date, end_date)


def load_husconet_radiation_average(start_date, end_date, excluded_husconet_stations=None):
    if excluded_husconet_stations is not None:
        excluded_husconet_stations = list(excluded_husconet_stations)
        excluded_husconet_stations.sort()
        file_name = "husconet_average_radiation_exclude_{excluded}.csv".format(
            excluded="_".join(excluded_husconet_stations))
    else:
        file_name = "husconet_average_radiation.csv"
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "husconet",
        file_name
    )
    logging.debug("loading for radiation average: '{file}'".format(file=csv_file))
    return load_husconet_file(csv_file, start_date, end_date)


class StationRepository:
    """
    Works like `filter_weather_data.filters.StationRepository` but for husconet stations
    """
    def __init__(self):
        csv_file_positions = os.path.join(PROCESSED_DATA_DIR, "husconet_positions.csv")
        self.meta_data = pandas.read_csv(csv_file_positions, index_col='station')

    def load_husconet_station_as_station_dict(self, station_name, start_date=None, end_date=None):
        station_df = load_husconet_station(station_name, start_date, end_date)
        meta_data = self.meta_data.loc[station_name]
        logging.debug("load {station}".format(station=station_name))
        return {
                "name": station_name,
                "data_frame": station_df,
                "meta_data": {
                    "position": {
                        "lat": meta_data.lat,
                        "lon": meta_data.lon
                    }
                }
            }

    def load_all_stations(self, start_date=None, end_date=None, limit=0):
        station_dicts = []
        for i, station_name in enumerate(self.meta_data.index):
            if limit != 0 and i >= limit:
                break
            station_dicts.append(self.load_husconet_station_as_station_dict(station_name, start_date, end_date))
        logging.debug("loaded husconet_dicts: %i" % len(station_dicts))
        return station_dicts


def load_husconet_station(husconet_station_name, start_date=None, end_date=None, attribute_to_load=None):
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "husconet",
        husconet_station_name + ".csv"
    )
    return load_husconet_file(csv_file, start_date, end_date, attribute_to_load=attribute_to_load)


def load_husconet_file(csv_file, start_date=None, end_date=None, attribute_to_check=None, attribute_to_load=None):
    """

    :param start_date: The start date
    :type start_date: str | datetime.datetime | None
    :param end_date: The end date
    :type end_date: str | datetime.datetime | None
    :param csv_file: Path to the csv file
    :type csv_file: str
    :param attribute_to_check: Check whether this attribute is 0, more reliable than the '.empty' property
    :type attribute_to_check: str | None
    :param attribute_to_load: Load only a single attribute from the csv if provided, else load all existent attributes
    :type attribute_to_load: str | None
    :return: The loaded data frame within the time span
    :rtype: ``pandas.DataFrame``
    """
    if isinstance(start_date, str):
        start_date = dateutil.parser.parse(start_date)
    if isinstance(end_date, str):
        end_date = dateutil.parser.parse(end_date)

    if not hasattr(load_husconet_file, "cache"):
        load_husconet_file.cache = {}
    key = (csv_file + "s" + repr(start_date) + "e" + repr(end_date) + "a" + repr(attribute_to_check)
           + "l" + repr(attribute_to_load))
    if key in load_husconet_file.cache:
        return load_husconet_file.cache[key]

    if attribute_to_load is None:
        husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    else:
        husconet_station_df = pandas.read_csv(
            csv_file,
            usecols=["datetime", attribute_to_load],
            index_col="datetime",
            parse_dates=["datetime"]
        )
    husconet_station_df = husconet_station_df.tz_localize("UTC").tz_convert(GermanWinterTime()).tz_localize(None)

    if start_date is not None:
        before_start = husconet_station_df[:(start_date - datetime.timedelta(minutes=1))]
        if attribute_to_check is not None and before_start[attribute_to_check].count() > 0:
            logging.warning("Husconet data found before start date '{start_date}'".format(start_date=start_date))
            logging.info(before_start.describe())
            before_start.info()
        elif not before_start.empty:
            logging.warning("Husconet data found before start date '{start_date}'".format(start_date=start_date))
            logging.info(before_start.describe())
            before_start.info()
        husconet_station_df = husconet_station_df[start_date:]

    if end_date is not None:
        after_end_date = (end_date + datetime.timedelta(days=1))
        after_end_df = husconet_station_df[after_end_date:]
        if attribute_to_check is not None and after_end_df[attribute_to_check].count() > 0:
            logging.warning("Husconet data found after end date '{end_date}'".format(end_date=after_end_date))
            logging.info(after_end_df.describe())
            after_end_df.info()
        elif not after_end_df.empty:
            logging.warning("Husconet data found after end date '{end_date}'".format(end_date=after_end_date))
            logging.info(after_end_df.describe())
            after_end_df.info()
        husconet_station_df = husconet_station_df[:after_end_date]

    load_husconet_file.cache[key] = husconet_station_df.copy()
    return husconet_station_df
