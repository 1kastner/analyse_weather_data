"""

Run as script with '-m filter_weather_data.filters.remove_indoor_stations'
"""

import logging
import statistics

import numpy

from . import load_average_reference_values
from . import StationRepository


class Ellipse:
    def __init__(self, center_x, radius_x_axis, center_y, radius_y_axis):
        """
        
        :param center_x: center of ellipse on x axis
        :param radius_x_axis: radius of ellipse on x axis
        :param center_y: center of ellipse on y axis
        :param radius_y_axis: radius of ellipse on y axis
        """
        self.center_x = center_x
        self.center_y = center_y
        self.radius_x_axis = radius_x_axis
        self.radius_y_axis = radius_y_axis
        logging.debug("min x: " + str(self.center_x - self.radius_x_axis))
        logging.debug("max x: " + str(self.center_x + self.radius_x_axis))
        logging.debug("min y: " + str(self.center_y - self.radius_y_axis))
        logging.debug("max y: " + str(self.center_y + self.radius_y_axis))

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
            if inside_ellipse and not inside_ellipse:
                logging.debug("inside rectangle but not inside ellipse")
        return inside_rectangle and inside_ellipse


def check_station(station_df, reference_interval):
    """
    
    :param station_df: The station to check (pandas data frame)
    :param reference_interval: Given a value pair (a, b) it says whether those two values are inside the interval
    :return: Is the station ok
    """
    for year in station_df.index.year.unique():
        year_key = "{year}".format(year=year)
        year_df = station_df.loc[year_key:year_key]

        for month in year_df.index.month.unique():
            daily_minimum_temperatures_per_month = []
            daily_standard_deviation_per_month = []
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key:month_key]
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = station_df.loc[day_key:day_key]
                t_min = day_df.temperature.min()
                daily_minimum_temperatures_per_month.append(t_min)
                t_std = day_df.temperature.std()
                if numpy.isnan(t_std):
                    logging.warning("No robust statistics possible, check frequent reporting!")
                    raise RuntimeError("No robust statistics possible, check frequent reporting!")
                else:
                    daily_standard_deviation_per_month.append(t_std)
            minimum_temperature_mean = statistics.mean(daily_minimum_temperatures_per_month)
            temperature_standard_deviation = statistics.mean(daily_standard_deviation_per_month)
            if (minimum_temperature_mean, temperature_standard_deviation) not in reference_interval[month_key]:
                logging.debug(month_key)
                return False
    return True


def get_reference_interval(start_date, end_date, time_zone):
    """
    
    :param start_date: The first value to load (included)
    :param end_date: The last value to load (included)
    :param time_zone: The time zone, e.g. 'CET'
    :return: This creates the norm whether the provided minimum temperature and daily deviation can be considered usual
    """
    reference_df = load_average_reference_values("temperature", time_zone)
    reference_df = reference_df[start_date:end_date]
    result = {}

    for year in reference_df.index.year.unique():
        year_key = str(year)
        year_df = reference_df.loc[year_key:year_key]
        for month in year_df.index.month.unique():
            daily_minimum_temperatures_per_month = []
            daily_standard_deviation_per_month = []
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = year_df.loc[month_key:month_key]
            for day in month_df.index.day.unique():
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = month_df.loc[day_key:day_key]
                t_min = day_df.temperature.min()
                daily_minimum_temperatures_per_month.append(t_min)
                t_std = day_df.temperature.std()
                if not numpy.isnan(t_std):
                    daily_standard_deviation_per_month.append(t_std)

            minimum_temperature_mean = statistics.mean(daily_minimum_temperatures_per_month)
            minimum_temperature_std = statistics.stdev(daily_minimum_temperatures_per_month)
            daily_standard_deviation_mean = statistics.mean(daily_standard_deviation_per_month)
            daily_standard_deviation_std = statistics.stdev(daily_standard_deviation_per_month)

            logging.debug(month_key)
            result[month_key] = Ellipse(
                minimum_temperature_mean,
                minimum_temperature_std * 5,
                daily_standard_deviation_mean,
                daily_standard_deviation_std * 5
            )
    return result


def filter_stations(station_dicts, start_date, end_date, time_zone):
    """

    :param station_dicts: The station dicts
    :param start_date: The date to start (included) 
    :param end_date: The date to stop (included)
    :param time_zone: The time zone
    """
    reference_interval = get_reference_interval(start_date, end_date, time_zone)
    filtered_stations = []

    for station_dict in station_dicts:
        logging.debug("indoor " + station_dict["name"])
        if check_station(station_dict["data_frame"], reference_interval):
            filtered_stations.append(station_dict)
    return filtered_stations


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    time_zone = "CET"
    stations = ['IHAMBURG69', 'IBNNINGS2']
    station_repository = StationRepository()
    station_dicts = [station_repository.load_station(station, start_date, end_date, time_zone=time_zone) for station
                     in stations]
    stations_inside_reference = filter_stations(station_dicts, start_date, end_date, time_zone)
    print([station_dict["name"] for station_dict in stations_inside_reference])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
