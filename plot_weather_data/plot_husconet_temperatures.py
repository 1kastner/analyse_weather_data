"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging

from matplotlib import pyplot

from gather_weather_data.husconet import HUSCONET_STATIONS
from gather_weather_data.husconet import OFFICIAL_HUSCONET_NAME
from gather_weather_data.husconet import load_husconet_station
from . import style_year_2016_plot


def plot_stations():
    """
    Plots all HUSCONET weather stations
    """

    fig = pyplot.figure()
    fig.canvas.set_window_title("husconet year 2016")
    pyplot.rcParams['savefig.dpi'] = 300

    for station_name in HUSCONET_STATIONS:
        station_df = load_husconet_station(station_name, "2016-01-01", "2016-12-31", "temperature")
        logging.debug("plotting {station} from {start} to {end}"
                      .format(station=station_name, start=station_df.index.min(), end=station_df.index.max()))
        official_station_name = OFFICIAL_HUSCONET_NAME[station_name]
        pyplot.plot(station_df.index, station_df.temperature, label=official_station_name, alpha=.3, linewidth=.4)

    logging.debug("start plotting")

    style_year_2016_plot(pyplot.gca())
    leg = pyplot.legend()
    for line in leg.get_lines():
        line.set_linewidth(1)  # .4 is too small
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_stations()
