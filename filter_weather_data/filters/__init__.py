"""
Group some common functionality like loading data frames.
"""

import os
import datetime
import logging

import pandas
import dateutil.parser


PROCESSED_DATA_DIR = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        os.pardir,
        "processed_data"
)


# For how many minutes one (temperature) measurement is valid if no new measurement is given.
TEMPORAL_SPAN = 30


class StationRepository:

    summary_dir = os.path.join(PROCESSED_DATA_DIR, "station_summaries")

    stations_df = None

    def __init__(self, private_weather_stations_file_name="private_weather_stations.csv"):
        """
        
        :param private_weather_stations_file_name: Where to look up the station metadata
        """
        self.private_weather_stations_file_name = private_weather_stations_file_name

    def get_all_stations(self):
        """
    
        :return: All stations which have been detected before
        """
        if self.stations_df is None:
            csv_file = os.path.join(PROCESSED_DATA_DIR, self.private_weather_stations_file_name)
            if not os.path.isfile(csv_file):
                logging.warning("No such file: ", os.path.realpath(csv_file))
                raise RuntimeError("No private weather station list found. Run 'list_private_weather_stations.py'")
            self.stations_df = pandas.read_csv(csv_file, index_col="station")
        return self.stations_df

    def load_all_stations(self, start_date, end_date, time_zone, minutely):
        """
        
        :param start_date: The earliest day which must be included (potentially earlier)
        :type start_date: str | datetime.datetime
        :param end_date: The latest day which must be included (potentially later)
        :type end_date: str | datetime.datetime
        :param time_zone: The time zone, e.g. 'CET'
        :type time_zone: datetime.tzinfo | str
        :param minutely: Resample data frame minutely
        :type minutely: bool
        :return: 
        """
        station_dicts = []
        for station_name, lat, lon in self.get_all_stations().itertuples():
            station_dict = self.load_station(station_name, start_date, end_date, time_zone, minutely)
            if station_dict is not None:
                station_dicts.append(station_dict)
                logging.debug("load " + station_name)
        return station_dicts

    def load_station(self, station, start_date, end_date, time_zone=None, minutely=False):
        """
        This only looks at the dates and returns the corresponding summary (assuming naive dates, overlapping at 
        midnight is ignored).
        
        :param station: The station to load
        :param start_date: The earliest day which must be included (potentially earlier)
        :type start_date: str | datetime.datetime
        :param end_date: The latest day which must be included (potentially later)
        :type end_date: str | datetime.datetime
        :param time_zone: The time zone, e.g. 'CET'
        :type time_zone: datetime.tzinfo | str
        :param minutely: Resample data frame minutely
        :type minutely: bool
        :return: The searched data frame
        :rtype: ``pandas.DataFrame``
        """
        start_date, end_date = self._cast_date(start_date, end_date)
        searched_station_summary_file_name = self._search_summary_file(station, start_date, end_date)
        if searched_station_summary_file_name is None:
            logging.error("No file found: '{station}' ranging between {start_date} and {end_date}".format(
                station=station, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d")
            ))
            return None
        else:
            csv_file = os.path.join(self.summary_dir, searched_station_summary_file_name)
            station_df = pandas.read_csv(csv_file, index_col="datetime", parse_dates=["datetime"])
            station_df = self._handle_time_zone_related_issues(station_df, time_zone, start_date, end_date)
            if minutely:
                station_df = self._create_minutely_data_frame(station_df, start_date, end_date, time_zone)
            meta_data = self._get_metadata(station)
            return {
                "name": station,
                "data_frame": station_df,
                "meta_data": {
                    "position": {
                        "lat": meta_data.lat,
                        "lon": meta_data.lon
                    }
                }
            }

    @staticmethod
    def _cast_date(start_date, end_date):
        if type(start_date) is str:
            start_date = dateutil.parser.parse(start_date)
        if type(end_date) is str:
            end_date = dateutil.parser.parse(end_date)
        return start_date, end_date

    @classmethod
    def _search_summary_file(cls, station, start_date, end_date):
        searched_station_summary_file_name = None
        for station_summary_file_name in os.listdir(cls.summary_dir):
            if not station_summary_file_name.endswith(".csv"):
                continue
            if not station_summary_file_name.startswith(station):
                continue
            station_summary_file_name = station_summary_file_name[:-4]  # cut of '.csv'
            station, start_date_span_text, end_date_span_text = station_summary_file_name.split("_")
            start_date_span = datetime.datetime.strptime(start_date_span_text, "%Y%m%d").replace(
                tzinfo=start_date.tzinfo)
            end_date_span = datetime.datetime.strptime(end_date_span_text, "%Y%m%d").replace(tzinfo=end_date.tzinfo)
            if start_date_span <= start_date and end_date_span >= end_date:
                searched_station_summary_file_name = station_summary_file_name + ".csv"
        return searched_station_summary_file_name

    @staticmethod
    def _handle_time_zone_related_issues(station_df, time_zone, start_date, end_date):
        if time_zone is not None:
            station_df = station_df.tz_localize("UTC").tz_convert(time_zone)
        if not station_df.index.is_monotonic:  # Some JSON are damaged, so we need to sort them again
            station_df.sort_index(inplace=True)
        return station_df[start_date:end_date]

    @staticmethod
    def _create_minutely_data_frame(station_df, start_date, end_date, time_zone):
        time_span_df = pandas.DataFrame(index=pandas.date_range(start_date, end_date, req='T', name="datetime",
                                                                time_zone=time_zone))
        station_df = station_df.join([time_span_df], how="outer")
        station_df.fillna(method='ffill', inplace=True, limit=TEMPORAL_SPAN)
        return station_df

    def _get_metadata(self, station):
        if self.stations_df is None:
            self.get_all_stations()
        return self.stations_df.loc[station]


def load_average_reference_values(attribute, time_zone):
    """
    
    :param attribute: 'temperature' or 'radiation'
    :param time_zone: The time zone to use for the reference values, e.g. 'CET'
    :return: Pandas data frame
    """
    reference_csv_path = os.path.join(PROCESSED_DATA_DIR, "husconet", "husconet_average_{attribute}.csv".format(
        attribute=attribute))
    reference_df = pandas.read_csv(reference_csv_path, index_col="datetime", parse_dates=["datetime"])
    reference_df = reference_df.tz_localize("UTC").tz_convert(time_zone)
    return reference_df
