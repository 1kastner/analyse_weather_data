"""
Depends on 
  filter_weather_data.filters.preparation.average_husconet_temperature
  filter_weather_data.filters.preparation.average_husconet_radiation
"""

import numpy
import pandas
from matplotlib import pyplot

from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import load_average_reference_values
from filter_weather_data.filters.remove_unshaded_stations import SUNSHINE_MINIMUM_THRESHOLD


def plot_station(station, start_date, end_date, time_zone):
    """
    Plots the regression of the temperature difference on solar radiation

    :param station: The station name which station should be plotted
    :param start_date: The start date of the plot
    :param end_date: The end date of the plot
    :param time_zone: The time zone of the reference net
    """
    station_repository = StationRepository()
    station_dict = station_repository.load_station(station, start_date, end_date, time_zone, minutely=True)
    station_df = station_dict["data_frame"]

    reference_temperature_df = load_average_reference_values("temperature", time_zone)
    reference_temperature_df = reference_temperature_df[start_date:end_date]

    reference_radiation_df = load_average_reference_values("radiation", time_zone)
    reference_radiation_df = reference_radiation_df[start_date:end_date]

    temp_df = station_df.join(reference_temperature_df, how='inner', rsuffix="_reference_temperature")
    delta_temperature = (temp_df.temperature_reference_temperature - temp_df.temperature).rename("temperature_delta")
    delta_df = pandas.concat([temp_df, delta_temperature], axis=1)

    delta_df = delta_df.join(reference_radiation_df, how='left')
    df_only_sunshine = delta_df[(delta_df.radiation > SUNSHINE_MINIMUM_THRESHOLD)]
    df_only_sunshine = df_only_sunshine.dropna(axis=0, how='any')

    X = df_only_sunshine.temperature_delta
    Y = df_only_sunshine.temperature_reference_temperature

    pyplot.scatter(X, Y)
    pyplot.plot(X, numpy.poly1d(numpy.polyfit(X, Y, 1))(X))
    pyplot.show()


if __name__ == "__main__":
    plot_station("ISCHENEF11", "2016-07-01", "2016-08-31", "CET")
