"""
List all private weather stations which are at a given location.

If you want to invoke the script directly and see the demo, add the option
'-m gather_weather_data.wunderground.list_private_weather_stations'
"""

import logging
import os
import time

import pandas as pd
import requests

from . import WundergroundProperties
from . import PROCESSED_DATA_DIR


def get_station_neighbours_at_location(lat, lon):
    """
    
    :param lat: The latitude of the station (nearest station selected)
    :param lon: The longitude of the station (nearest station selected)
    :return: list of private stations in the environment
    
    Example [("IHAMBURG32", 53.58, 10.0)]
    """
    api_url = WundergroundProperties.get_api_url()
    geolookup_url = api_url + "geolookup/q/{lat},{lon}.json".format(lat=lat, lon=lon)
    time.sleep(0.5)
    response = requests.get(geolookup_url)
    if response.status_code != 200:  # do a single retry
        time.sleep(0.5)  # API punishes too many requests
        response = requests.get(geolookup_url)
        if response.status_code != 200:
            logging.warning("error for {lat},{lon}".format(lat=lat, lon=lon))
            return []
    response_json = response.json()
    stations = response_json["location"]["nearby_weather_stations"]["pws"]["station"]
    result = []
    for station in stations:
        result.append(
            (
                station["id"],
                float(station["lat"]),
                float(station["lon"])
            )
        )
    return result


def get_station_neighbours_by_name(station):
    """
    
    :param station: The station which is already known.
    :type station: str
    :return: list of private stations in the environment

    Example [("IHAMBURG32", 53.58, 10.0)]
    """
    api_url = WundergroundProperties.get_api_url()
    geolookup_url = api_url + "geolookup/q/pws:{station}.json".format(station=station)
    time.sleep(0.5)
    response = requests.get(geolookup_url)
    if response.status_code != 200:  # do a single retry
        time.sleep(0.5)  # API punishes too many requests
        response = requests.get(geolookup_url)
        if response.status_code != 200:
            logging.warning("error for {station}".format(station=station))
            return []
    response_json = response.json()
    lat = response_json["location"]["lat"]
    lon = response_json["location"]["lon"]

    logging.info("by name: {station},{lat},{lon}".format(
        station=station,
        lat=lat,
        lon=lon
    ))
    return lat, lon


def list_private_weather_stations(lat_min, lat_max, lon_min, lon_max):
    """
    
    :param lat_min: minimum latitude (describing a window)
    :param lat_max: maximum latitude (describing a window)
    :param lon_min: minimum longitude (describing a window)
    :param lon_max: maximum longitude (describing a window)
    :return: dictionary of stations and their coordinates
    """
    # Where to gather the result
    private_weather_stations = dict()
    lat_start = (lat_min + lat_max) / 2
    lon_start = (lon_min + lon_max) / 2
    neighbours = get_station_neighbours_at_location(lat_start, lon_start)

    while len(neighbours):
        neighbour = neighbours.pop()
        neighbour_station_id, neighbour_lat, neighbour_lon = neighbour
        if neighbour_station_id in private_weather_stations:
            continue
        if lat_min <= neighbour_lat <= lat_max and lon_min <= neighbour_lon <= lon_max:
            logging.info("by lat,lon: {station},{lat},{lon}".format(
                station=neighbour_station_id,
                lat=neighbour_lat,
                lon=neighbour_lon
            ))
            private_weather_stations[neighbour_station_id] = (neighbour_lat, neighbour_lon)
            neighbours.extend(get_station_neighbours_at_location(neighbour_lat, neighbour_lon))
    return private_weather_stations


def save_coordinates(csv_file, private_weather_stations, force_overwrite=False):
    """
    
    :param force_overwrite: If disabled, keep old data and only add.
    :param csv_file: The path to the csv file where to save the stations and their coordinates to
    :param private_weather_stations: Dictionary like returned by ``list_private_weather_stations``
    """
    if os.path.isfile(csv_file) and os.path.getsize(csv_file) and not force_overwrite:
        df = pd.read_csv(csv_file, index_col="station")
    else:
        df = pd.DataFrame(columns=["station", "lat", "lon"])
    for station, (lat, lon) in private_weather_stations.items():
        df.loc[station] = (lat, lon)
    df.to_csv(csv_file)


def demo():
    """
    Search for all private weather stations in a subarea of Hamburg and save them.
    """
    private_weather_stations = list_private_weather_stations(53.390, 53.5, 9.702, 10.0)
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "private_weather_stations.csv"
    )
    save_coordinates(csv_file, private_weather_stations)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo()
