"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging
import os

import pandas
from matplotlib import pyplot
from matplotlib import ticker as mticker
import seaborn

from gather_weather_data.husconet import HUSCONET_STATIONS
from filter_weather_data.filters import PROCESSED_DATA_DIR

seaborn.set(style='ticks')


def plot_stations():
    """
    Plots all HUSCONET weather stations as boxplots.
    """
    plot_df = pandas.DataFrame()

    fig = pyplot.figure()
    fig.canvas.set_window_title("husconet boxplots")

    for husconet_station in HUSCONET_STATIONS:
        csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", husconet_station + ".csv")
        logging.debug("loading " + csv_file)
        husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
        plot_df[husconet_station] = husconet_station_df.temperature

    logging.debug("start plotting")
    ax = seaborn.boxplot(data=plot_df, width=.5)
    ax.set(xlabel="HUSCONET Station", ylabel="Temperatur (°C)")
    ax.yaxis.set_major_locator(mticker.MultipleLocator(5))  # draw line every 5 °C
    pyplot.grid(color='.8')  # a very light gray
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_stations()
