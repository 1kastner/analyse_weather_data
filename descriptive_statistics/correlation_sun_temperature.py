"""

"""

from matplotlib import pyplot

from filter_weather_data import RepositoryParameter
from filter_weather_data import get_repository_parameters
from filter_weather_data.filters.remove_unshaded_stations import *


def check_correlation(start_date, end_date):
    station_repository = StationRepository(*get_repository_parameters(RepositoryParameter.ONLY_OUTDOOR))
    station_dicts = station_repository.load_all_stations(start_date, end_date)
    reference_temperature_df = load_husconet_temperature_average(start_date, end_date)
    reference_radiation_df = load_husconet_radiation_average(start_date, end_date)

    r_and_p_values = [check_station(station_dict["data_frame"], reference_temperature_df,
                                             reference_radiation_df, just_check_correlation=True)
                               for station_dict in station_dicts]
    r_values = []
    for r_value, p_value in r_and_p_values:
        if p_value < .05:
            r_values.append(r_value)
    pyplot.hist(r_values, 10)
    pyplot.xlim(-1, 1)
    pyplot.xlabel("Korrelationskoeffizient $r$")
    pyplot.ylabel("Anzahl PWS")
    pyplot.show()


def demo():
    start_date = "2016-01-01T00:00:00"
    end_date = "2016-12-31T00:00:00"
    check_correlation(start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
