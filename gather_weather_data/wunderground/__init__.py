"""
The wunderground module for gathering weather data
"""

import os

import pandas


WUNDERGROUND_RAW_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        os.pardir,
        "wunderground_raw_data"
)

PROCESSED_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        os.pardir,
        "processed_data"
)


class WundergroundProperties:

    # The name to search for in the system environment
    wunderground_api_key_environment_variable_name = "WUNDERGROUND_API_KEY"

    # The cached api key (from first invocation)
    wunderground_api_key = None

    # The pattern of the url
    wunderground_api_url_pattern = "https://api.wunderground.com/api/{wunderground_api_key}/"

    # The cached api url (from first invocation)
    wunderground_api_url = None

    @classmethod
    def get_api_key(cls):
        if cls.wunderground_api_key is None:
            if cls.wunderground_api_key_environment_variable_name in os.environ:
                cls.wunderground_api_key = os.environ[cls.wunderground_api_key_environment_variable_name]
            else:
                raise RuntimeError("No wunderground api key found in environment variables")
        return cls.wunderground_api_key

    @classmethod
    def get_api_url(cls):
        if cls.wunderground_api_url is None:
            api_key = cls.get_api_key()
            cls.wunderground_api_url = cls.wunderground_api_url_pattern.format(wunderground_api_key=api_key)
        return cls.wunderground_api_url


def get_all_stations():
    """

    :return: All stations which have been detected before
    """
    csv_file = os.path.join(PROCESSED_DATA_DIR, "private_weather_stations.csv")
    if not os.path.isfile(csv_file):
        raise RuntimeError("No private weather station list found. Run 'list_private_weather_stations.py' before")
    stations = pandas.read_csv(csv_file, usecols=["station"])
    return stations["station"].values
