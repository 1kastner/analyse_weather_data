"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging

from matplotlib import pyplot
import matplotlib.dates as mdates

from gather_weather_data.husconet import HUSCONET_STATIONS
from gather_weather_data.husconet import load_husconet_station


def plot_stations():
    """
    Plots all HUSCONET weather stations in the background.
    """

    for station_name in HUSCONET_STATIONS[:1]:
        station_df = load_husconet_station(station_name, "2016-01-01", "2016-01-08")
        station_df.info()
        logging.debug("plotting {station} from {start} to {end}"
                      .format(station=station_name, start=station_df.index.min(), end=station_df.index.max()))
        ax = station_df.temperature.plot(label=station_name, alpha=.2)

    logging.debug("start plotting")

    ax.set_ylabel('Temperature in °C')
    ax.set_xlabel('')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m.%Y %H:%M'))
    pyplot.legend()
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_stations()
