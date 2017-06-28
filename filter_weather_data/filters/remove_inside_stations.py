"""
Inspired by the paper 'Crowdsourcing air temperature from citizen weather stations for urban climate research' by Fred 
Meier, Daniel Fenner, Tom Grassman, Marco Otto, Dieter Scherer (2017).

I have no affiliation with any of the people or institutes but I did this as a personal project on my own.
"""

import os
import logging
import statistics

import pandas
import numpy

from . import PROCESSED_DATA_DIR
from . import load_station


class Ellipse:
    def __init__(self, center_x, center_y, radius_x_axis, radius_y_axis):
        """
        
        :param center_x: center of ellipse on x axis
        :param center_y: center of ellipse on y axis
        :param radius_x_axis: radius of ellipse on x axis
        :param radius_y_axis: radius of ellipse on y axis
        """
        self.center_x = center_x
        self.center_y = center_y
        self.radius_x_axis = radius_x_axis
        self.radius_y_axis = radius_y_axis

    def __contains__(self, point):
        """
        
        :param point: A sorted collection with two entries
        :return: Is it inside the ellipse
        """
        point_x = point[0]
        point_y = point[1]
        inside_rectangle = (
            (self.center_x - self.radius_x_axis <= point_x <= self.center_x + self.radius_x_axis)
            and
            (self.center_y - self.radius_y_axis <= point_y <= self.center_y + self.radius_y_axis)
        )
        if not inside_rectangle:
            logging.debug("x: " + str(self.center_x - self.radius_x_axis) + ", " + str(point_x) + ", " + str(
                self.center_x + self.radius_x_axis))
            logging.debug("y: " + str(self.center_y - self.radius_y_axis) + ", " + str(point_y) + ", " + str(
                self.center_y + self.radius_y_axis))
            return False
        inside_ellipse = False
        if inside_rectangle:
            inside_ellipse = (
                (
                    ((point_x - self.center_x) ** 2 / self.radius_x_axis ** 2)
                    +
                    ((point_y - self.center_y) ** 2 / self.radius_y_axis ** 2)

                ) <= 1)
        return inside_rectangle and inside_ellipse


def check_station(station_df, reference_interval):
    """
    
    :param station_df: The station to check (pandas data frame)
    :param reference_interval: Given a value pair (a, b) it says whether those two values are inside the interval
    :return: Is the station ok
    """
    for year in station_df.index.year.unique():
        year_key = "{year}".format(year=year)
        year_df = station_df.loc[year_key]

        minimum_temperatures_per_month = []
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            logging.debug(month_key)
            month_df = station_df.loc[month_key]
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = station_df.loc[day_key]
                t_min = day_df.temperature.min()
                if not numpy.isnan(t_min):
                    minimum_temperatures_per_month.append(t_min)
            month_df = station_df[month_key]
            minimum_temperature_mean = statistics.mean(minimum_temperatures_per_month)
            temperature_standard_deviation = month_df.temperature.std()
            if (temperature_standard_deviation, minimum_temperature_mean) not in reference_interval[month_key]:
                return False
    return True


def get_reference_interval(start_date, end_date):
    """
    
    :param start_date: The first value to load (included)
    :param end_date: The last value to load (included)
    :return: This creates the norm whether the provided minimum temperature and daily deviation can be considered usual
    """
    reference_csv_path = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average.csv")
    reference_df = pandas.read_csv(reference_csv_path, index_col="datetime", parse_dates=["datetime"])
    reference_df = reference_df[start_date:end_date]
    result = {}

    minimum_temperatures_per_month = []
    daily_standard_deviation = []
    for year in reference_df.index.year.unique():
        year_key = str(year)
        year_df = reference_df.loc[year_key]
        for month in year_df.index.month.unique():
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key]
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = month_df.loc[day_key]
                t_min = day_df.temperature.min()
                if not numpy.isnan(t_min):  # check here because NaNs propagate in the statistics module
                    minimum_temperatures_per_month.append(t_min)
                daily_standard_deviation.append(day_df.temperature.std())

            minimum_temperature_mean = statistics.mean(minimum_temperatures_per_month)
            minimum_temperature_std = statistics.stdev(minimum_temperatures_per_month)
            daily_standard_deviation_mean = statistics.mean(daily_standard_deviation)
            daily_standard_deviation_std = statistics.stdev(daily_standard_deviation)

            result[month_key] = Ellipse(
                minimum_temperature_mean,
                minimum_temperature_std * 5,
                daily_standard_deviation_mean,
                daily_standard_deviation_std * 5
            )

    return result


def filter_stations(stations, start_date, end_date, time_zone):
    """

    :param stations: The name of the stations, e.g. ['IHAMBURG69']
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    :param time_zone: The time zone
    """
    reference_interval = get_reference_interval(start_date, end_date)
    filtered_stations = []
    for station in stations:
        station_df = load_station(station, start_date, end_date, time_zone=time_zone)
        logging.debug(station)
        if check_station(station_df, reference_interval):
            filtered_stations.append(station)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    time_zone = "CET"
    stations_inside_reference = filter_stations(['IHAMBURG69', 'IBNNINGS2'], start_date, end_date, time_zone)
    print(stations_inside_reference)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
