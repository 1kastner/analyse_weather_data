"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging
import os

import pandas
from matplotlib import pyplot
import seaborn
import matplotlib.ticker as mticker

from gather_weather_data.husconet import GermanWinterTime
from filter_weather_data.filters import StationRepository
from filter_weather_data.filters import PROCESSED_DATA_DIR


seaborn.set(style='ticks')


def plot_stations(data, start_date, end_date, time_zone=None, limit=0):
    """
    Plots all HUSCONET weather stations in the background.
    """
    plot_df = pandas.DataFrame()

    fig = pyplot.figure()
    fig.canvas.set_window_title("vor_und_nach_dem_filtern")

    for title, weather_station, summary_dir in data:
        station_repository = StationRepository(weather_station, summary_dir)
        station_dicts = station_repository.load_all_stations(start_date, end_date, time_zone=time_zone, limit=limit)
        temperatures = [station_dict['data_frame'].temperature for station_dict in station_dicts]
        plot_df[title] = pandas.concat(temperatures, ignore_index=True)

    logging.debug("start plotting")
    ax = seaborn.boxplot(data=plot_df, width=.5)
    ax.set(ylabel="Temperatur (°C)")
    ax.yaxis.set_major_locator(mticker.MultipleLocator(10))  # draw line every 10 °C
    pyplot.grid(color='.8')  # a very light gray
    pyplot.show()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    filtered_stations_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )
    before = (
        "ungefiltert",
        os.path.join(PROCESSED_DATA_DIR, "private_weather_stations.csv"),
        os.path.join(PROCESSED_DATA_DIR, "station_summaries")
    )
    start = (
        "alle Temperaturen \nüber -31,7 °C",
        os.path.join(filtered_stations_dir, "station_dicts_with_valid_position.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_no_extreme_values")
    )
    frequent_reports = (
        "regelmäßige Daten",
        os.path.join(filtered_stations_dir, "station_dicts_frequent.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
    )
    only_outdoor = (
        "draußen",
        os.path.join(filtered_stations_dir, "station_dicts_outdoor.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
    )
    only_outdoor_and_shaded = (
        "gefiltert",
        os.path.join(filtered_stations_dir, "station_dicts_shaded.csv"),
        os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_of_shaded_stations")
    )
    start_date = "2016-01-01"
    end_date = "2016-12-31"
    #plot_stations([before], start_date, end_date, time_zone=GermanWinterTime())
    plot_stations([start, only_outdoor_and_shaded], start_date, end_date)
