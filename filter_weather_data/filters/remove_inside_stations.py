"""

"""

import logging
import statistics

import numpy


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
    for year in station_df.index.year.unique():
        minimum_temperatures_per_month = []
        for month in station_df.index.month.unique():
            logging.debug(month)
            for day in station_df.index.day:
                day_key = "{year}-{month}-{day}".format(year=year, month=month, day=day)
                day_df = station_df.loc[day_key]
                t_min = day_df.temperature.min()
                if not numpy.isnan(t_min):
                    minimum_temperatures_per_month.append(t_min)
            month_key = "{year}-{month}".format(year=year, month=month)
            month_df = station_df[month_key]
            minimum_temperature_mean = statistics.mean(minimum_temperatures_per_month)
            temperature_standard_deviation = month_df.temperature.std()
            if (temperature_standard_deviation, minimum_temperature_mean) not in reference_interval:
                return False
    return True


def get_reference_interval():



    minimum_temperatures = []
    stds = []
    for day in range(calendar.monthrange(2016, month)[1]):
        day += 1  # month starts with 1st, not 0th day
        required_date = "2016-{m}-{d}".format(m=month, d=day)
        #day_df = eddh_df.get(required_date)
        day_df = husconet_df.get(required_date)
        if day_df is None:
            print("nothing for ", required_date)
        t_min = day_df.temperature.min()
        minimum_temperatures.append(t_min)
        std = day_df.temperature.std()
        stds.append(std)
    t_min_point = statistics.mean(minimum_temperatures)
    t_min_std = statistics.stdev(minimum_temperatures)
    std_point = statistics.mean(stds)
    std_std = statistics.stdev(stds)
    ellipse = Rectangle(t_min_point, t_min_std, std_point, std_std)
    ellipse.minimum_temperatures = minimum_temperatures
