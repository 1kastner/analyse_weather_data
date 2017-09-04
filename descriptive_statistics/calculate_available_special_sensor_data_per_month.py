#! /usr/bin/python3
"""

Runt it with
-m descriptive_statistics.calculate_available_data
for the main script.
"""

import sys
import os.path
import logging

import pandas

from filter_weather_data import RepositoryParameter
from filter_weather_data import get_repository_parameters
from filter_weather_data.filters import StationRepository
from filter_weather_data import PROCESSED_DATA_DIR


def setup_logger():
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    path_to_file_to_log_to = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "log",
        "calculate_available_data.log"
    )
    file_handler = logging.FileHandler(path_to_file_to_log_to)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    logging.info("### Start new logging")
    return log


def get_available_precipitation(station_dict):
    result = {}
    df = station_dict["data_frame"]
    for year in df.index.year.unique():
        year_key = str(year)
        year_df = df.loc[year_key:year_key]  # avoids getting a series if a single entry exists
        if year_df.empty:
            continue
        if year_df.precipitation.count() != 0:
            for month in year_df.index.month.unique():
                month_key = "{year}-{month}".format(year=year, month=month)
                column = df.loc[month_key].precipitation
                precipitation_count = (column != 0).count()
                if precipitation_count > 0:
                    result["precipitation_" + month_key] = precipitation_count
    return result


def get_available_wind(station_dict):
    result = {}
    df = station_dict["data_frame"]
    for year in df.index.year.unique():
        year_key = str(year)
        year_df = df.loc[year_key:year_key]  # avoids getting a series if a single entry exists
        if year_df.empty:
            continue
        if year_df.windspeed.count() != 0:
            for month in year_df.index.month.unique():
                month_key = "{year}-{month}".format(year=year, month=month)
                column = df.loc[month_key].windspeed
                windspeed_count = (column != 0).count()
                if windspeed_count > 0:
                    result["windspeed_" + month_key] = windspeed_count
    return result


def gather_statistics(repository_parameter, start_date, end_date):
    logging.info("repository: %s" % repository_parameter.value)
    station_repository = StationRepository(*get_repository_parameters(repository_parameter))
    available_precipitation = {}
    available_wind = {}
    station_dicts = station_repository.load_all_stations(
        start_date=start_date,
        end_date=end_date,
        limit_to_temperature=False,
        #  limit=10  # for testing purposes
    )
    logging.info("total: %i" % len(station_dicts))
    stations_with_precipitation = set()
    stations_with_wind = set()
    while True:
        if len(station_dicts) == 0:
            break
        station_dict = station_dicts.pop()  # free memory whenever you can
        precipitation = get_available_precipitation(station_dict)
        if len(precipitation):
            available_precipitation[station_dict["name"]] = precipitation
            stations_with_precipitation.add(station_dict["name"])
        wind = get_available_wind(station_dict)
        if len(wind):
            available_wind[station_dict["name"]] = wind
            stations_with_wind.add(station_dict["name"])
    df_precipitation = pandas.DataFrame(available_precipitation)
    df_wind = pandas.DataFrame(available_wind)
    result_file_precipitation = os.path.join(
        PROCESSED_DATA_DIR,
        "misc",
        "precipitation_per_month_%s.csv" % repository_parameter.value
    )
    df_precipitation.to_csv(result_file_precipitation)
    result_file_wind = os.path.join(
        PROCESSED_DATA_DIR,
        "misc",
        "wind_per_month_%s.csv" % repository_parameter.value
    )
    df_wind.to_csv(result_file_wind)

    station_dicts_wind = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations",
        "station_dicts_wind.csv"
    )
    df_data = []
    for station_with_wind in stations_with_wind:
        meta_info = station_repository.get_meta_info(stations_with_wind)
        df_data.append({
            "station": station_with_wind,
            "lat": meta_info.lat[0],
            "lon": meta_info.lon[0]
        })
    df = pandas.DataFrame(df_data)
    df.set_index("station", inplace=True)
    df.to_csv(station_dicts_wind)

    station_dicts_precipitation = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations",
        "station_dicts_precipitation.csv"
    )
    df_data = []
    for station_with_precipitation in stations_with_precipitation:
        meta_info = station_repository.get_meta_info(stations_with_precipitation)
        df_data.append({
            "station": station_with_precipitation,
            "lat": meta_info.lat[0],
            "lon": meta_info.lon[0]
        })
    df = pandas.DataFrame(df_data)
    df.set_index("station", inplace=True)
    df.to_csv(station_dicts_precipitation)


def run():
    """
    Runs a sample run
    """

    start_date = "2016-01-01"
    end_date = "2016-12-31"

    logging.info("start")
    gather_statistics(RepositoryParameter.START_FULL_SENSOR, start_date, end_date)


if __name__ == "__main__":
    setup_logger()
    run()
