"""

"""
import os
import enum


PROCESSED_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "processed_data"
)


class RepositoryParameter(enum.Enum):
    START = "start"
    FREQUENT_REPORTS = "frequent_reports"
    ONLY_OUTDOOR = "only_outdoor"
    ONLY_OUTDOOR_AND_SHADED = "only_outdoor_and_shaded"


def get_repository_parameters(name):
    filtered_stations_dir = os.path.join(
        PROCESSED_DATA_DIR,
        "filtered_stations"
    )

    if name == RepositoryParameter.START:
        return (
            os.path.join(filtered_stations_dir, "station_dicts_with_valid_position.csv"),
            os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_no_extreme_values")
        )
    if name == RepositoryParameter.FREQUENT_REPORTS:
        return (
            os.path.join(filtered_stations_dir, "station_dicts_frequent.csv"),
            os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
        )
    if name == RepositoryParameter.ONLY_OUTDOOR:
        return (
            os.path.join(filtered_stations_dir, "station_dicts_outdoor.csv"),
            os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_frequent")
        )
    if name == RepositoryParameter.ONLY_OUTDOOR_AND_SHADED:
        return (
            os.path.join(filtered_stations_dir, "station_dicts_shaded.csv"),
            os.path.join(PROCESSED_DATA_DIR, "filtered_station_summaries_of_shaded_stations")
        )
    else:
        raise RuntimeError("repository parameter must be one of 'start', 'frequent_reports',"
                           "'only_outdoor' or 'only_outdoor_and_shaded', but you provided "
                           + str(name))
