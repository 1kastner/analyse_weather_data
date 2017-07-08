"""

Run with 
-m filter_weather_data.start_filtering_pipe
if you want to see the demo.
"""

import os
import logging

from gather_weather_data.husconet import GermanWinterTime

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
    logging.info("# stations before: " + str(len(old_station_dicts)))
    logging.info("# stations after:  " + str(len(new_station_dicts)))
    logging.info("# stations diff:   " + str(len(filtered_stations)))
    no_rows_old = sum([station["data_frame"].temperature.count() for station in old_station_dicts])
    no_rows_new = sum([station["data_frame"].temperature.count() for station in new_station_dicts])
    logging.info("# rows before: " + str(no_rows_old))
    logging.info("# rows after:  " + str(no_rows_new))
    logging.info("# rows diff:   " + str(no_rows_old - no_rows_new))
    logging.debug("before:")
    logging.debug([station for station in old_stations])
    logging.debug("removed by filter:")
    logging.debug([station for station in filtered_stations])
    logging.debug("remaining stations:")
    logging.debug([station for station in new_stations])
    if len(new_station_dicts):
        logging.debug("example df")
        new_station_dicts[0]["data_frame"].info()
    return filtered_stations


class FilterApplier:

    def __init__(self, output_dir, force_overwrite, start_date, end_date):
        self.output_dir = output_dir
        self.force_overwrite = force_overwrite
        self.start_date = start_date
        self.end_date = end_date

    def apply_extreme_record_filter(self, station_dicts, minimum, maximum):
        """
        Remove extreme values.

        :param minimum: Filter out anything below this minimum
        :param maximum: Filter out anything above this maximum
        :param station_dicts: The station dicts
        :return: Good stations
        """
        filter_extreme_values(station_dicts, minimum, maximum)
        if self.force_overwrite:
            output_dir_for_summaries = os.path.join(
                PROCESSED_DATA_DIR,
                "filtered_station_summaries_no_extreme_values"
            )
            save_station_dicts_as_time_span_summary(station_dicts, output_dir_for_summaries)
        return station_dicts

    def apply_invalid_position_filter(self, station_dicts, meta_info_df):
        """
        Filters out stations which have invalid positions
        
        :param station_dicts: The station dicts
        :param meta_info_df: The meta info.
        :return: Good stations
        """
        with_valid_position_station_dicts = filter_wrongly_positioned_stations(station_dicts, meta_info_df)
        csv_path_with_valid_position = os.path.join(
            self.output_dir,
            "station_dicts_with_valid_position.csv"
        )
        if not os.path.isfile(csv_path_with_valid_position) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(with_valid_position_station_dicts, csv_path_with_valid_position)
        return with_valid_position_station_dicts

    def apply_infrequent_record_filter(self, station_dicts):
        """
        Filters out stations which have infrequent records, less than 80% per day or 80% per month

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_frequent = os.path.join(
            self.output_dir,
            "station_dicts_frequent.csv"
        )
        frequent_station_dicts = filter_infrequently_reporting_stations(station_dicts)
        if not os.path.isfile(csv_path_frequent) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(frequent_station_dicts, csv_path_frequent)
        if self.force_overwrite:
            output_dir_for_summaries = os.path.join(
                PROCESSED_DATA_DIR,
                "filtered_station_summaries_frequent"
            )
            save_station_dicts_as_time_span_summary(frequent_station_dicts, output_dir_for_summaries)
        return frequent_station_dicts

    def apply_not_indoor_filter(self, station_dicts):
        """
        Filters out stations which look like they are positioned inside

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_not_indoor = os.path.join(
            self.output_dir,
            "station_dicts_outdoor.csv"
        )
        outdoor_station_dicts = filter_indoor_stations(station_dicts, self.start_date, self.end_date)
        if not os.path.isfile(csv_path_not_indoor) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(outdoor_station_dicts, csv_path_not_indoor)
        return outdoor_station_dicts

    def apply_unshaded_filter(self, station_dicts):
        """
        Filters out stations which look like they are exposed to direct sunlight

        :param station_dicts: The station dicts
        :return: Good stations
        """
        csv_path_shaded = os.path.join(
            self.output_dir,
            "station_dicts_shaded.csv"
        )
        shaded_station_dicts = filter_unshaded_stations(station_dicts, self.start_date, self.end_date)
        if not os.path.isfile(csv_path_shaded) or self.force_overwrite:
            save_station_dicts_as_metadata_csv(shaded_station_dicts, csv_path_shaded)
        if self.force_overwrite:
            output_dir_for_summaries = os.path.join(
                PROCESSED_DATA_DIR,
                "filtered_station_summaries_of_shaded_stations"
            )
            save_station_dicts_as_time_span_summary(shaded_station_dicts, output_dir_for_summaries)
        return shaded_station_dicts


def save_filtered_out_stations(name, stations):
    output = os.path.join(
        PROCESSED_DATA_DIR,
        "removed_stations"
    )
    if not os.path.isdir(output):
        os.mkdir(output)
    csv_file = os.path.join(
        output,
        name + ".csv"
    )
    with open(csv_file, "w") as f:
        f.write("\n".join([station for station in stations]))


def run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, force_overwrite=False,
             minimum=-100.0, maximum=+100.0):

    output_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    prepare()

    station_repository = StationRepository(private_weather_stations_file_name)
    station_dicts = station_repository.load_all_stations(start_date, end_date, time_zone)
    meta_info_df = station_repository.get_all_stations()

    filter_applier = FilterApplier(output_dir, force_overwrite, start_date, end_date)

    # START
    logging.debug("position - empty")
    no_rows_start = sum([station["data_frame"].temperature.count() for station in station_dicts])
    logging.info("# start: " + str(no_rows_start))

    # EXTREME
    filter_applier.apply_extreme_record_filter(station_dicts, minimum, maximum)

    # POSITION
    with_valid_position_station_dicts = filter_applier.apply_invalid_position_filter(station_dicts, meta_info_df)
    logging.debug("position - empty")
    filtered_stations = show_mini_statistics(station_dicts, with_valid_position_station_dicts)
    save_filtered_out_stations("wrong_position", filtered_stations)

    # INFREQUENT
    frequent_station_dicts = filter_applier.apply_infrequent_record_filter(with_valid_position_station_dicts)
    logging.debug("position - infrequent")
    filtered_stations = show_mini_statistics(with_valid_position_station_dicts, frequent_station_dicts)
    save_filtered_out_stations("infrequent", filtered_stations)

    # INDOOR
    indoor_station_dicts = filter_applier.apply_not_indoor_filter(frequent_station_dicts)
    logging.debug("infrequent - indoor")
    filtered_stations = show_mini_statistics(frequent_station_dicts, indoor_station_dicts)
    save_filtered_out_stations("indoor", filtered_stations)

    # UNSHADED
    shaded_station_dicts = filter_applier.apply_unshaded_filter(indoor_station_dicts)
    logging.debug("indoor - shaded")
    filtered_stations = show_mini_statistics(indoor_station_dicts, shaded_station_dicts)
    save_filtered_out_stations("unshaded", filtered_stations)


def demo():
    start_date = "2016-01-01T00:00:00"
    end_date = "2016-12-31T00:00:00"
    private_weather_stations_file_name = "private_weather_stations.csv"
    time_zone = GermanWinterTime()
    hamburg_minimum = -29.1 - 20  # -29.1°C is the record, 20 degrees additionally for free
    hamburg_maximum = 37.3 + 40  # 37.3°C is the record, 40 degrees additionally for free (don't remove direct sunlight)
    run_pipe(private_weather_stations_file_name, start_date, end_date, time_zone, True, hamburg_minimum,
             hamburg_maximum)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
