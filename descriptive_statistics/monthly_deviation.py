"""
Shows used ellipse.
"""

from filter_weather_data.filters.remove_indoor_stations import *


def show_monthly_deviation():
    start_date = "2016-01-01T00:00:00"
    end_date = "2016-12-31T00:00:00"
    filter_stations([], start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
