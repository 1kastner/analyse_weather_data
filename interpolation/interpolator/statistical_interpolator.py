"""

"""
import statistics
import logging


def inverted_distance_weight(temperatures, distances, p):
    """
    
    :param temperatures: The temperatures
    :param distances: The distances (in m)
    :param p: The power, normally 2 or 3
    :return: interpolated temperature
    """
    inverted_sum_distances = sum([distance ** -p for distance in distances])
    weights = [(distance ** -p) / inverted_sum_distances for distance in distances]
    temperature_idw = sum([w * t_i for w, t_i in zip(weights, temperatures)])
    return temperature_idw


def get_square_error_calculator(t_actual):
    """
    
    :param t_actual: The actual temperature
    :return: function to calculate the square error given the estimated temperature
    :rtype: lambda
    """
    def calculate_square_error(t_estimated):
        return (t_estimated - t_actual) ** 2
    return calculate_square_error


def get_interpolation_result(temperature_distance_tuples, t_actual, postfix=""):
    """
    
    :param temperature_distance_tuples: List of temperature and distance pairs
    :param t_actual: The actual temperature
    :param postfix: The postfix for different methods if needed
    :return: Several simplistic measurements.
    :rtype: dict
    """
    if not temperature_distance_tuples:  # No neighbours could be found
        return {}
    temperatures, distances = zip(*temperature_distance_tuples)
    calculate_square_error = get_square_error_calculator(t_actual)
    result = {
        "idw_p2" + postfix: calculate_square_error(inverted_distance_weight(temperatures, distances, 2)),
        "idw_p3" + postfix: calculate_square_error(inverted_distance_weight(temperatures, distances, 3)),
        "max" + postfix: calculate_square_error(max(temperatures)),
        "median" + postfix: calculate_square_error(statistics.median(temperatures)),
        "min" + postfix: calculate_square_error(min(temperatures))
    }

    return result


def demo():
    start_date = '2016-01-01T00:00'
    end_date = '2016-03-31T23:59'
    from filter_weather_data.filters import StationRepository
    from .nearest_k_finder import NearestKFinder
    station_repository = StationRepository()
    station_dicts = station_repository.load_all_stations(start_date, end_date, limit=20)
    chosen_index = 9
    search_for = station_dicts[chosen_index]
    print("picked", station_dicts[chosen_index]["name"], "to look for")
    del station_dicts[chosen_index]  # otherwise triangles will contain the searched point as well.
    k_nearest_finder = NearestKFinder(station_dicts, start_date, end_date)
    t = search_for["data_frame"].index.values[0]
    neighbours = k_nearest_finder.find_k_nearest_neighbours(search_for, t, 3)
    t_actual = search_for["data_frame"].loc[t].temperature
    result = get_interpolation_result(neighbours, t_actual)
    print("actual measurement:", t_actual)
    for neighbour in neighbours:
        temperature, distance = neighbour
        print("measured", temperature, "°C in", distance, "meters distance")
    for method, value in result.items():
        print("method", method, "value", value**.5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    demo()
