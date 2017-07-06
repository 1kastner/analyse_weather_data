"""

"""

import os
import logging

import numpy
import pandas

PROCESSED_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir,
    "processed_data"
)


def insert_nans(station_df):
    """
    Only when NaNs are present, the line is discontinued.
    
    :param station_df: 
    :return: 
    """
    reference_df = pandas.DataFrame(
        index=pandas.date_range(station_df.index[0], station_df.index[-1], freq='D', name="datetime"),
    )

    return station_df.join(reference_df, how="outer")
