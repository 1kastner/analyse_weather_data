"""
Depends on filter_weather_data.filters.preparation.average_husconet_temperature
"""

import logging

from matplotlib import pyplot

from filter_weather_data.filters import StationRepository
from gather_weather_data.husconet import GermanWinterTime
from gather_weather_data.husconet import load_husconet_temperature_average
from . import insert_nans
from . import style_year_2016_plot


def plot_station(station, start_date, end_date):
    """
    Plots measured values in the foreground and the average of all HUSCONET weather stations in the background.
    
    :param station: The station name which station should be plotted
    :param start_date: The start date of the plot
    :type start_date: str | datetime.datetime
    :param end_date: The end date of the plot
    :type end_date: str | datetime.datetime
    """
    station_repository = StationRepository()
    station_dict = station_repository.load_station(station, start_date, end_date, GermanWinterTime())
    station_df = station_dict['data_frame']
    station_df = insert_nans(station_df)
    pyplot.plot(station_df.index, station_df.temperature, label=station)
    logging.debug("plotting {station} from {start} to {end}"
                  .format(station=station, start=station_df.index.min(), end=station_df.index.max()))

    husconet_station_df = load_husconet_temperature_average(start_date, end_date)
    pyplot.plot(husconet_station_df.index, husconet_station_df.temperature, alpha=0.3, label="Referenznetzwerk")
    logging.debug("plotting HUSCONET from {start} to {end}"
                  .format(start=station_df.index.min(), end=station_df.index.max()))

    style_year_2016_plot(pyplot.gca())
    pyplot.legend()
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    plot_station("IHAMBURG69", "2016-01-01", "2016-01-31")
    #plot_station("ISCHENEF11", "2016-07-01", "2016-07-31")
