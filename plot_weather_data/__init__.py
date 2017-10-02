"""

"""

import os

import pandas
from matplotlib import pyplot
from matplotlib import dates as mdates
import matplotlib.ticker as mticker

PROCESSED_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir,
    "processed_data"
)


def insert_nans(station_df):
    """
    Only when NaNs are present, the line is discontinued.
    
    :param station_df: 
    :return: 
    """
    reference_df = pandas.DataFrame(
        index=pandas.date_range(station_df.index[0], station_df.index[-1], freq='H', name="datetime"),
    )

    return station_df.join(reference_df, how="outer")


class GermanDateFormatter(mdates.DateFormatter):
    """
    As the Windows locales are wrong (no dot after abbreviations like what the Duden tells us to do)
    this is the home-brew solution
    """

    def __init__(self):
        super().__init__(self)
        import locale
        locale.setlocale(locale.LC_ALL, 'de')
        self.month_formatter = mdates.DateFormatter('%b')

    def strftime(self, dt, fmt=None):
        windows_month_name = dt.strftime("%b")
        if windows_month_name == "Mrz":
            return "März"
        if windows_month_name == "Mai":
            return "Mai"
        if windows_month_name == "Jun":
            return "Juni"
        if windows_month_name == "Jul":
            return "Juli"
        if windows_month_name == "Sep":
            return "Sept."
        abbreviated_month_name = windows_month_name + "."
        return abbreviated_month_name


def style_year_2016_plot(ax):
    ax.set_ylabel('Temperatur (°C)')
    ax.set_xlabel('2016')
    ax.margins(x=0)
    ax.yaxis.set_major_locator(mticker.MultipleLocator(5))  # draw line every 5 °C
    pyplot.grid(color='.9')  # a very light gray
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(GermanDateFormatter())
