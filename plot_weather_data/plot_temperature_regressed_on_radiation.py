"""
Depends on 
  filter_weather_data.filters.preparation.average_husconet_temperature
  filter_weather_data.filters.preparation.average_husconet_radiation
"""
import os
import logging

import numpy
import pandas
from matplotlib import pyplot

from gather_weather_data.husconet import load_husconet_temperature_average
from gather_weather_data.husconet import load_husconet_radiation_average
from filter_weather_data.filters import PROCESSED_DATA_DIR
from filter_weather_data.filters import StationRepository
from filter_weather_data.filters.remove_unshaded_stations import SUNSHINE_MINIMUM_THRESHOLD


def plot_station(station, start_date, end_date):
    """
    Plots the regression of the temperature difference on solar radiation

    :param station: The station name which station should be plotted
    :param start_date: The start date of the plot
    :param end_date: The end date of the plot
    """
    summary_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_station_summaries_frequent"
    )
    outdoor_stations = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations",
        "station_dicts_outdoor.csv"
    )
    station_repository = StationRepository(outdoor_stations, summary_dir)
    station_dict = station_repository.load_station(station, start_date, end_date)
    station_df = station_dict["data_frame"]

    reference_temperature_df = load_husconet_temperature_average(start_date, end_date)
    reference_radiation_df = load_husconet_radiation_average(start_date, end_date)

    temp_df = station_df.join(reference_temperature_df, how='inner', rsuffix="_reference_temperature")
    delta_temperature = (temp_df.temperature - temp_df.temperature_reference_temperature).rename("temperature_delta")
    delta_df = pandas.concat([temp_df, delta_temperature], axis=1)

    delta_df = delta_df.join(reference_radiation_df, how='left')
    df_only_sunshine = delta_df[(delta_df.radiation > SUNSHINE_MINIMUM_THRESHOLD)]
    df_only_sunshine = df_only_sunshine.dropna(axis=0, how='any')

    X = df_only_sunshine.temperature_delta
    Y = df_only_sunshine.temperature_reference_temperature

    fig = pyplot.figure()
    fig.canvas.set_window_title(station + "temperature regressed on radiation")

    pyplot.scatter(X, Y, marker="x", color="gray")
    pyplot.plot(X, numpy.poly1d(numpy.polyfit(X, Y, 1))(X), color="blue", alpha=.8, label=station)
    pyplot.gca().set_ylabel(r'Globalstrahlung ($\frac{W}{m^2}$)')
    pyplot.gca().set_xlabel('Temperaturdifferenz Crowdsourced - Referenznetzwerk (°C)')
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    good_examples = ['IHAMBURG638', 'IHAMBURG32', 'IHAMBURG1789', 'IHAMBURG1308', 'IHAMBURG269', 'IHAMBURG851',
                     'IHAMBURG57', 'IHAMBURG360', 'INORDERS2', 'IHAMBURG847', 'IHAMBURG1389', 'IHAMBURG723',
                     'IHAMBURG1028', 'IHHSCHNE3', 'IHAMBURG373', 'IHAMBURG1088', 'IHAMBURG423', 'IHAMBURG452',
                     'IHAMBURG1799', 'IHAMBURG1074']

    plot_station('IHAMBURG1074', "2016-07-01", "2016-09-30")
