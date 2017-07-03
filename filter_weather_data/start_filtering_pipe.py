"""

Run with 
-m filter_weather_data.start_filtering_pipe
if you want to see the demo.
"""

import os
import logging

from .filters import PROCESSED_DATA_DIR
from .filters import StationRepository
from .filters.preparation.average_husconet_radiation import average_solar_radiation_across_husconet_stations
from .filters.preparation.average_husconet_temperature import average_temperature_across_husconet_stations
from .filters.remove_wrongly_positioned_stations import filter_stations as filter_wrongly_positioned_stations
from .filters.remove_extreme_values import filter_stations as filter_extreme_values
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
    eol = "\n"
    with open(csv_path, "w") as f:
        f.write("station,lat,lon" + eol)
        for station_dict in station_dicts:
            station_name = station_dict["name"]
            lat = str(station_dict["meta_data"]["position"]["lat"])
            lon = str(station_dict["meta_data"]["position"]["lon"])
            f.write(station_name + "," + lat + "," + lon + eol)


def save_station_dicts_as_time_span_summary(station_dicts, output_dir=None):
    """
    
    :param station_dicts: The station dicts
    :param output_dir: The output directory for the summary
    """
    if output_dir is None:
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


def show_mini_statistics(old_station_dicts, new_station_dicts):
    old_stations = [station["name"] for station in old_station_dicts]
    new_stations = [station["name"] for station in new_station_dicts]
    filtered_stations = list(set(old_stations) - set(new_stations))
    logging.info("before: " + str(len(old_station_dicts)))
    logging.info("after:  " + str(len(new_station_dicts)))
    logging.info("diff:   " + str(len(filtered_stations)))
    logging.debug("before:            ")
    logging.debug([station for station in old_stations])
    logging.debug("removed by filter: ")
    logging.debug([station for station in filtered_stations])
    logging.debug("remaining stations: ")
    logging.debug([station for station in new_stations])


class FilterApplier:

    def __init__(self, output_dir, force_overwrite, start_date, end_date, time_zone):
        self.output_dir = output_dir
        self.force_overwrite = force_overwrite
        self.start_date = start_date
        self.end_date = end_date
        self.time_zone = time_zone

    def apply_invalid_position_filter(self, station_dicts, meta_info_df):
        """
        Filters out stations which have invalid positions
        
        :param station_dicts: The station dicts
        :param meta_info_df: The meta info.
        :return: Good stations
        """
        csv_path_with_valid_position = os.path.join(
            self.output_dir,
            "station_dicts_with_valid_position.csv"
        )
        with_valid_position_station_dicts = filter_wrongly_positioned_stations(station_dicts, meta_info_df)
        if not os.path.isfile(csv_path_with_valid_position) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(with_valid_position_station_dicts, csv_path_with_valid_position)
        return with_valid_position_station_dicts

    @staticmethod
    def apply_extreme_record_filter(station_dicts, minimum, maximum):
        """
        Remove extreme values.
        
        :param station_dicts: The station dicts
        :return: Good stations
        """
        filter_extreme_values(station_dicts, minimum, maximum)
        return station_dicts

    def apply_infrequent_record_filter(self, station_dicts):
        """
        Filters out stations which have infrequent records, less than 80% per day or 80% per month

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_not_infrequent = os.path.join(
            self.output_dir,
            "station_dicts_frequent.csv"
        )
        frequent_station_dicts = filter_infrequently_reporting_stations(station_dicts)
        if not os.path.isfile(csv_path_not_infrequent) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(frequent_station_dicts, csv_path_not_infrequent)
        return frequent_station_dicts

    def apply_not_indoor_filter(self, station_dicts):
        """
        Filters out stations which look like they are positioned inside

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_not_indoor = os.path.join(
            self.output_dir,
            "station_dicts_not_indoor.csv"
        )
        indoor_station_dicts = filter_indoor_stations(station_dicts, self.start_date, self.end_date, self.time_zone)
        if not os.path.isfile(csv_path_not_indoor) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(indoor_station_dicts, csv_path_not_indoor)
        return indoor_station_dicts

    def apply_unshaded_filter(self, station_dicts):
        """
        Filters out stations which look like they are exposed to direct sunlight

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_not_unshaded = os.path.join(
            self.output_dir,
            "station_dicts_not_unshaded.csv"
        )
        if not os.path.isfile(csv_path_not_unshaded) or self.force_overwrite:
            shaded_station_dicts = filter_unshaded_stations(station_dicts, self.start_date, self.end_date,
                                                                  self.time_zone)
            save_station_dicts_as_metadata_csv(shaded_station_dicts, csv_path_not_unshaded)
        else:
            shaded_station_dicts = station_dicts
        return shaded_station_dicts


def run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, force_overwrite=False,
             minimum=-100, maximum=+100):

    output_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    prepare()

    station_repository = StationRepository(private_weather_stations_file_name)
    all_found_station_dicts = station_repository.load_all_stations(start_date, end_date, time_zone, True)
    meta_info_df = station_repository.get_all_stations()
    logging.debug("start: " + str(len(all_found_station_dicts)))

    filter_applier = FilterApplier(output_dir, force_overwrite, start_date, end_date, time_zone)
    filter_applier.apply_extreme_record_filter(all_found_station_dicts, minimum, maximum)

    # POSITION
    with_valid_position_station_dicts = filter_applier.apply_invalid_position_filter(all_found_station_dicts,
                                                                                     meta_info_df)
    logging.debug("position - empty")
    show_mini_statistics(all_found_station_dicts, with_valid_position_station_dicts)

    # INFREQUENT
    frequent_station_dicts = filter_applier.apply_infrequent_record_filter(with_valid_position_station_dicts)
    logging.debug("position - infrequent")
    show_mini_statistics(with_valid_position_station_dicts, frequent_station_dicts)

    # INDOOR
    indoor_station_dicts = filter_applier.apply_not_indoor_filter(frequent_station_dicts)
    logging.debug("infrequent - indoor")
    show_mini_statistics(frequent_station_dicts, indoor_station_dicts)

    # UNSHADED
    shaded_station_dicts = filter_applier.apply_infrequent_record_filter(indoor_station_dicts)
    logging.debug("indoor - shaded")
    show_mini_statistics(indoor_station_dicts, shaded_station_dicts)

    # save for future processing
    save_station_dicts_as_time_span_summary(shaded_station_dicts)


def demo():
    start_date = "2016-01-01T00:00:00+01:00"
    end_date = "2016-12-31T00:00:00+01:00"
    private_weather_stations_file_name = "private_weather_stations.csv"
    time_zone = "CET"
    run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, True, -50, 50)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
