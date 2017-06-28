"""
Filter weather data
"""

import pandas


def load_station():
    data_frame = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
