
"""

"""
import logging
import datetime

import numpy
import pandas
from matplotlib import pyplot
import dateutil.parser

from pykrige.ok import OrdinaryKriging
import geopy
import geopy.distance

from filter_weather_data import RepositoryParameter, get_repository_parameters
from filter_weather_data.filters import StationRepository


def plot_variogram(X, Y, Z, title):
    ok = OrdinaryKriging(X, Y, Z, variogram_model='spherical', verbose=False, enable_plotting=False, nlags=8)
    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    ax.plot(ok.lags, ok.semivariance, 'ko-')
    pyplot.ylabel("$\gamma(h)$")
    pyplot.xlabel("$h$ (in km)")
    pyplot.grid(color='.8')  # a very light gray
    fig.canvas.set_window_title(title)
    logging.debug("plotting preparation done")
    pyplot.show()


def convert_to_meter_distance(latitudes, longitudes):
    """
    This is some kind of northing/easting because the smallest longitude/latitude values are used.
    However, the edge cases are not considered.

    :param latitudes:
    :param longitudes:
    :return:
    """
    min_lat = min(latitudes)
    min_lon = min(longitudes)
    X, Y = [], []
    for lat, lon in zip(latitudes, longitudes):
        this_point = geopy.Point(lat, lon)
        x_meter = geopy.distance.distance(geopy.Point(lat, min_lon), this_point).km
        y_meter = geopy.distance.distance(geopy.Point(min_lat, lon), this_point).km
        logging.debug("convert {lat}:{lon} to {x_meter}:{y_meter}".format(lat=lat, lon=lon, x_meter=x_meter,
                                                                          y_meter=y_meter))
        X.append(x_meter)
        Y.append(y_meter)
    return X, Y


def sample_up(df, start_date, end_date, decay):
    df_year = pandas.DataFrame(index=pandas.date_range(start_date, end_date, freq='T', name="datetime"))
    df = df.join(df_year, how="outer")
    df.temperature.fillna(method="ffill", limit=decay, inplace=True)
    return df


def load_data(repository_parameter, date):
    start_date = dateutil.parser.parse(date) - datetime.timedelta(hours=1)  # first value to load
    end_date = date  # load until this date (station repository adds some margin)
    t = date  # check the values at this given time
    station_repository = StationRepository(*get_repository_parameters(repository_parameter))
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date
    )
    latitudes, longitudes, Z = [], [], []
    for station_dict in station_dicts:
        station_dict["data_frame"] = sample_up(station_dict["data_frame"], start_date, end_date, 30)  # 30 minutes decay
        #for i, t in enumerate(station_dict["data_frame"].index):

        temperature = station_dict["data_frame"].loc[t].temperature
        if numpy.isnan(temperature):
            continue
        position = station_dict["meta_data"]["position"]
        latitudes.append(position["lat"])
        longitudes.append(position["lon"])
        Z.append(temperature)
    X, Y = convert_to_meter_distance(latitudes, longitudes)
    logging.debug("conversion to kilometer distances done")
    return X, Y, Z


def demo():
    date = '2016-06-01T13:00'
    #param = RepositoryParameter.START
    param = RepositoryParameter.ONLY_OUTDOOR_AND_SHADED
    X, Y, Z = load_data(param, date)
    title = date + " " + param.value
    print("title", title)
    print("X", X)
    print("Y", Y)
    print("Z", Z)
    print("Z min", min(Z))
    print("Z max", max(Z))
    print(len(Z))
    if input("continue? too large Z values might eat up all your memory. (y) or other: ") == "y":
        plot_variogram(X, Y, Z, title)


def demo2():
    """
    Shows that convert_to_meter_distance presents good results (edge cases not considered).

    """
    lat_lon_label_rows = [
        (53.64159, 9.94502, "center"),
        (53.84159, 9.94502, "north"),
        (53.44159, 9.94502, "south"),
        (53.64159, 9.74502, "west"),
        (53.64159, 10.14502, "east")
    ]
    lats, lons, labels = [], [], []
    for lat, lon, label in lat_lon_label_rows:
        lats.append(lat)
        lons.append(lon)
        labels.append(label)

    x_meters, y_meters = convert_to_meter_distance(lats, lons)

    fig, ax = pyplot.subplots()
    ax.scatter(x_meters, y_meters)
    for x, y, label in zip(x_meters, y_meters, labels):
        ax.annotate(label, (x, y))
    pyplot.show()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
    demo2()
