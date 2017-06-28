"""

"""
import os

import pandas
from matplotlib import pyplot

from filter_weather_data.filters import load_station
from filter_weather_data.filters import PROCESSED_DATA_DIR


def plot_station(station, start_date, end_date):
    """
    Plots measured values in the foreground and the average of all HUSCONET weather stations in the background.
    
    :param station: The station name which station should be plotted
    :param start_date: The start date of the plot
    :param end_date: The end date of the plot
    """

    station_df = load_station(station, start_date, end_date)
    station_df.temperature.plot()

    csv_file = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average.csv")
    husconet_station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
    husconet_station_df.temperature.plot(alpha=0.3)
    pyplot.show()


if __name__ == "__main__":
    plot_station("IHAMBURG69", "2016-01-01", "2016-01-31")
