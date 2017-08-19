"""
This is rather a script than something reusable because it is highly dependent on the data provided.

Run with
-m gather_weather_data.husconet.get_pandas_compatible_csv_from_logger
to start it as a standalone script.
"""

import os
import logging

import pandas

from . import HUSCONET_STATIONS
from . import HUSCONET_RAW_DATA_DIR
from . import PROCESSED_DATA_DIR


def load_husconet_station(station):
    """
    
    :param station: Name of station.
    :return: Pandas data frame
    """

    csv_file = os.path.join(HUSCONET_RAW_DATA_DIR, "{station}_TT_201601010000-201612312359.txt".format(station=station))
    df = pandas.read_csv(csv_file, names=["temperature"], na_values=["99999"])
    logging.debug("temperature")

    csv_file = os.path.join(HUSCONET_RAW_DATA_DIR, "{station}_RH_201601010000-201612312359.txt".format(station=station))
    df_relative_humidity = pandas.read_csv(csv_file, names=["humidity"], na_values=["99999"])
    df = df.assign(humidity=df_relative_humidity.humidity)
    logging.debug("humidity")

    csv_file = os.path.join(HUSCONET_RAW_DATA_DIR, "{station}_P_201601010000-201612312359.txt".format(station=station))
    df_pressure = pandas.read_csv(csv_file, names=["pressure"], na_values=["99999"])
    df = df.assign(pressure=df_pressure.pressure)
    logging.debug("pressure")

    csv_file = os.path.join(HUSCONET_RAW_DATA_DIR, "{station}_G_201601010000-201612312359.txt".format(station=station))
    df_pressure = pandas.read_csv(csv_file, names=["radiation"], na_values=["99999"])
    df = df.assign(radiation=df_pressure.radiation)
    logging.debug("radiation")

    # The logger are configured to use German standard time (no daylight saving).
    df.index = pandas.date_range('2016-01-01T00:00', '2016-12-31T23:59', freq='T', name="datetime", tz="CET")

    position_file = os.path.join(HUSCONET_RAW_DATA_DIR, "{station}_position.txt".format(station=station))
    with open(position_file) as f:
        lat, lon = [float(x) for x in f.read().split(",")]
    return df, lat, lon


def load_stations():
    csv_file_positions = os.path.join(PROCESSED_DATA_DIR, "husconet_positions.csv")
    output_dir = os.path.join(PROCESSED_DATA_DIR, "husconet")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    with open(csv_file_positions, "w") as f:
        f.write("station,lat,lon\n")
        for station in HUSCONET_STATIONS:
            logging.debug(station)
            df, lat, lon = load_husconet_station(station)
            f.write(station + "," + str(lat) + "," + str(lon) + "\n")
            csv_file_weather = os.path.join(PROCESSED_DATA_DIR, "husconet", station + ".csv")
            df.to_csv(csv_file_weather)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_stations()
