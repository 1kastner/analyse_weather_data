"""

Run with 
-m filter_weather_data.start_filtering_pipe
if you want to see the demo.
"""

import os
import logging

import pandas

from .filters import PROCESSED_DATA_DIR
from .filters import StationRepository
from .filters.preparation.average_husconet_radiation import average_solar_radiation_across_husconet_stations
from .filters.preparation.average_husconet_temperature import average_temperature_across_husconet_stations
from .filters.remove_wrongly_positioned_stations import filter_stations as filter_wrongly_positioned_stations
from .filters.remove_empty_stations import filter_stations as filter_empty_stations
from .filters.remove_infrequently_reporting_stations import filter_stations as filter_infrequently_reporting_stations
from .filters.remove_indoor_stations import filter_stations as filter_indoor_stations
from .filters.remove_unshaded_stations import filter_stations as filter_unshaded_stations


def prepare():
    """
    Prepares files if they do not yet exist.
    
    :return: 
    """
    average_solar_radiation_across_husconet_stations()
    average_temperature_across_husconet_stations()


def save_station_dicts_as_metadata_csv(station_dicts, csv_path):
    """
    
    :param station_dicts: The station dicts
    :param csv_path: The file name
    """

    df = pandas.DataFrame(columns=["station", "lat", "lon"], index=["station"])
    for station_dict in station_dicts:
        station_name = station_dict["name"]
        lat = station_dict["meta_data"]["position"]["lat"]
        lon = station_dict["meta_data"]["position"]["lon"]
        pandas.concat([df, pandas.DataFrame(columns=["station", "lat", "lon"], index=["station"],
                                            data={"station": station_name, "lat": lat, "lon": lon})])
    df.to_csv(csv_path)


def save_station_dicts_as_time_span_summary(station_dicts):
    """
    
    :param station_dicts: The station dicts
    """
    output_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_station_summaries"
    )
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    for station_dict in station_dicts:
        station_name = station_dict["name"]
        df = station_dict["data_frame"]
        csv_file = os.path.join(output_dir, station_name + ".csv")
        df.to_csv(csv_file)


def run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, force_overwrite=False):

    station_repository = StationRepository(private_weather_stations_file_name)
    station_dicts = station_repository.load_all_stations(start_date, end_date, time_zone, True)

    output_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    # POSITION
    csv_path_with_valid_position = os.path.join(
        output_dir,
        "station_dicts_with_valid_position"
    )
    if not os.path.isfile(csv_path_with_valid_position) or force_overwrite:
        with_position_station_dicts = filter_wrongly_positioned_stations(station_dicts)
        save_station_dicts_as_metadata_csv(with_position_station_dicts, csv_path_with_valid_position)
    else:
        with_position_station_dicts = station_dicts
    logging.debug("valid position " + str([station_dict["name"] for station_dict in with_position_station_dicts]))

    # EMPTY
    csv_path_not_empty = os.path.join(
        output_dir,
        "station_dicts_not_empty"
    )
    if not os.path.isfile(csv_path_not_empty) or force_overwrite:
        not_empty_station_dicts = filter_empty_stations(station_dicts)
        save_station_dicts_as_metadata_csv(not_empty_station_dicts, csv_path_not_empty)
    else:
        not_empty_station_dicts = with_position_station_dicts
    logging.debug("some data " + str([station_dict["name"] for station_dict in not_empty_station_dicts]))

    # INFREQUENT
    csv_path_not_infrequent = os.path.join(
        output_dir,
        "station_dicts_not_infrequent"
    )
    if not os.path.isfile(csv_path_not_infrequent) or force_overwrite:
        not_infrequent_station_dicts = filter_infrequently_reporting_stations(not_empty_station_dicts)
        save_station_dicts_as_metadata_csv(not_infrequent_station_dicts, csv_path_not_infrequent)
    else:
        not_infrequent_station_dicts = not_empty_station_dicts
    logging.debug("frequent " + str([station_dict["name"] for station_dict in not_infrequent_station_dicts]))

    # INDOOR
    csv_path_not_indoor = os.path.join(
        output_dir,
        "station_dicts_not_indoor"
    )
    if not os.path.isfile(csv_path_not_indoor) or force_overwrite:
        not_indoor_station_dicts = filter_indoor_stations(not_infrequent_station_dicts, start_date, end_date, time_zone)
        save_station_dicts_as_metadata_csv(not_indoor_station_dicts, csv_path_not_indoor)
    else:
        not_indoor_station_dicts = not_infrequent_station_dicts
    logging.debug("outdoor " + str([station_dict["name"] for station_dict in not_indoor_station_dicts]))

    # UNSHADED
    csv_path_not_unshaded = os.path.join(
        output_dir,
        "station_dicts_not_unshaded"
    )
    if not os.path.isfile(csv_path_not_unshaded) or force_overwrite:
        not_unshaded_station_dicts = filter_unshaded_stations(not_infrequent_station_dicts, start_date, end_date,
                                                              time_zone)
        save_station_dicts_as_metadata_csv(not_unshaded_station_dicts, csv_path_not_unshaded)
    else:
        not_unshaded_station_dicts = not_indoor_station_dicts
    logging.debug("shaded " + str([station_dict["name"] for station_dict in not_unshaded_station_dicts]))

    # save for future processing
    save_station_dicts_as_time_span_summary(not_unshaded_station_dicts)


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    private_weather_stations_file_name = "private_weather_stations.csv"
    time_zone = "CET"
    run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
