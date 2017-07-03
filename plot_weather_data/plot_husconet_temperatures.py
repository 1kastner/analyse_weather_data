"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging
import os

import pandas
from matplotlib import pyplot
import matplotlib.dates as mdates

from gather_weather_data.husconet import HUSCONET_STATIONS
from filter_weather_data.filters import PROCESSED_DATA_DIR


def plot_stations(time_zone):
    """
    Plots all HUSCONET weather stations in the background.
    """

    ax = pyplot.subplot()

    for husconet_station in HUSCONET_STATIONS:
        csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", husconet_station + ".csv")
        logging.debug("loading " + csv_file)
        husconet_station_df = (pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
                               .tz_localize("UTC").tz_convert(time_zone).tz_localize(None))
        husconet_station_df.temperature.plot(label=husconet_station, alpha=.2)

    logging.debug("start plotting")
    ax.set_ylabel('Temperature in °C')
    ax.set_xlabel('Zeit')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m.%Y'))
    pyplot.legend()
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_stations(time_zone="CET")
