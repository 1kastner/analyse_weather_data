"""

"""

import pandas
import numpy
from scipy.interpolate import griddata
from matplotlib import pyplot


def sample_up(df, start_date, end_date, decay):
    df_year = pandas.DataFrame(index=pandas.date_range(start_date, end_date, freq='T', name="datetime"))
    df = df.join(df_year, how="outer")
    df.temperature.fillna(method="ffill", limit=decay, inplace=True)
    return df


def grid_data(margin, station_dicts, t, bins_per_step):
    xs, ys, zs = [], [], []
    for station_dict in station_dicts:
        position = station_dict["meta_data"]["position"]
        lat, lon = position["lat"], position["lon"]
        temperature = station_dict["data_frame"].temperature.loc[t]
        xs.append(lon)
        ys.append(lat)
        zs.append(temperature)
    x_min = min(xs) - margin
    x_max = max(xs) + margin
    y_min = min(ys) - margin
    y_max = max(ys) + margin
    xs = numpy.array(xs)
    ys = numpy.array(ys)
    zs = numpy.array(zs)

    xi = numpy.linspace(x_min, x_max, (x_max - x_min) * bins_per_step)
    yi = numpy.linspace(y_min, y_max, (y_max - y_min) * bins_per_step)
    indices = ~numpy.isnan(zs)  # indices of not-NaN values.
    zi = griddata(
                  (xs[indices], ys[indices]),  # ignore NaNs for NN interpolation
                  zs[indices],
                  (xi[None, :], yi[:, None]),
                  method='nearest'
                  )
    return x_min, x_max, xs, xi, y_min, y_max, ys, yi, zs, zi


def plot(margin, x_min, x_max, xs, xi, y_min, y_max, ys, yi, zs, zi):
    jet = pyplot.cm.get_cmap("jet")
    pyplot.contour(xi, yi, zi, linewidths=0.5, colors='k')
    pyplot.contourf(xi, yi, zi, cmap=jet)
    pyplot.colorbar()

    marker_size = 25
    indices = ~numpy.isnan(zs)
    # good values
    pyplot.scatter(xs[indices], ys[indices], marker='o', c=zs[indices], cmap=jet, s=marker_size, edgecolor="k")
    # bad values, isnan
    pyplot.scatter(xs[~indices], ys[~indices], marker='o', c="k", s=marker_size)

    pyplot.xlim(x_min, x_max)
    pyplot.ylim(y_min, y_max)
    pyplot.title("Interpolation")
    pyplot.show()


def demo():
    from filter_weather_data.filters import StationRepository
    from gather_weather_data.husconet import GermanWinterTime
    date = '2016-12-01T00:00'
    start_date = date  # first value to load
    end_date = date  # load until this date (plus some margin)
    t = date  # check the values at this given time
    bins_per_step = 10000
    station_repository = StationRepository()
    station_dicts = station_repository.load_all_stations(
        start_date,
        end_date,
        time_zone=GermanWinterTime(),
        # limit=300
    )
    for station_dict in station_dicts:
        station_dict["data_frame"] = sample_up(station_dict["data_frame"], start_date, end_date, 30)  # 30 minutes decay
    margin = 0.01
    values = grid_data(margin, station_dicts, t, bins_per_step)
    plot(margin, *values)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    demo()
