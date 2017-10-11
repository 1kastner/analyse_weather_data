"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""
import logging
import itertools

from matplotlib import pyplot
from matplotlib import dates as mdates

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

    temperatures = []
    for station_name in HUSCONET_STATIONS:
        station_df = load_husconet_station(station_name, "2016-05-04T00:00", "2016-05-05T12:01", "temperature")
        logging.debug("plotting {station} from {start} to {end}"
                      .format(station=station_name, start=station_df.index.min(), end=station_df.index.max()))
        official_station_name = OFFICIAL_HUSCONET_NAME[station_name]
        pyplot.plot(station_df.index, station_df.temperature, label=official_station_name, linewidth=1)
        temperatures.append(station_df.temperature)

    logging.debug("start plotting")

    ax = pyplot.gca()
    style_year_2016_plot(ax)
    ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
    fig.autofmt_xdate(bottom=0.2, rotation=50, ha='right')
    ax.set_xlabel('')
    leg = pyplot.legend()
    for line in leg.get_lines():
        line.set_linewidth(1)  # .4 is too small
    pyplot.show()

    max_temperature_diff = 0
    for t1, t2 in itertools.combinations(temperatures, 2):
        _max_temperature_diff = (t1 - t2).max()
        if _max_temperature_diff > max_temperature_diff:
            max_temperature_diff = _max_temperature_diff
    print("max temperature diff", max_temperature_diff)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_stations()
