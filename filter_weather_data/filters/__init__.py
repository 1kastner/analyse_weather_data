"""
Group some common functionality like loading data frames.
"""

import os
import datetime
import logging

import numpy
import pandas
import dateutil.parser

from .. import PROCESSED_DATA_DIR


class StationRepository:

    summary_dir = os.path.join(PROCESSED_DATA_DIR, "station_summaries")

    stations_df = None

    cached_os_list_dir = None

    def __init__(self, private_weather_stations_file_name=None, summary_dir=None):
        """
        
        :param private_weather_stations_file_name: Where to look up the station metadata
        """
        if isinstance(private_weather_stations_file_name, tuple):
            raise RuntimeError("need text-like object")
        if private_weather_stations_file_name is None:
            private_weather_stations_file_name = os.path.join(
                PROCESSED_DATA_DIR,
                "private_weather_stations.csv"
            )
        if summary_dir is None:
            summary_dir = os.path.join(
                PROCESSED_DATA_DIR,
                "station_summaries"
            )
        self.private_weather_stations_file_name = private_weather_stations_file_name
        self.summary_dir = summary_dir
        self.cached_os_list_dir = os.listdir(self.summary_dir)

    def get_all_stations(self, limit=0):
        """
    
        :return: All stations which have been detected before as a data frame
        :rtype: ``pandas.DataFrame``
        """
        if self.stations_df is None:
            csv_file = os.path.join(PROCESSED_DATA_DIR, self.private_weather_stations_file_name)
            if not os.path.isfile(csv_file):
                logging.warning("No such file: ", os.path.realpath(csv_file))
                raise RuntimeError("No private weather station list found. Run 'list_private_weather_stations.py'")
            else:
                logging.debug("loading " + csv_file)
            self.stations_df = pandas.read_csv(csv_file, index_col="station")
        if limit:
            self.stations_df = self.stations_df.iloc[:limit]
        return self.stations_df

    def get_meta_info(self, station):
        """

        :param station:
        :return:
        """
        stations_df = self.get_all_stations()
        return stations_df.loc[station]

    def load_all_stations(self, start_date=None, end_date=None, time_zone=None, limit=0, limit_to_temperature=True):
        """

        :param limit_to_temperature: Only consider temperature - saves memory but drops interesting data
        :param start_date: The earliest day which must be included (potentially earlier)
        :type start_date: str | datetime.datetime | None
        :param end_date: The latest day which must be included (potentially later)
        :type end_date: str | datetime.datetime | None
        :param time_zone: The time zone, e.g. 'CET' or GermanWinterTime() or None for naive datetime objects
        :type time_zone: datetime.tzinfo | str | None
        :param limit: Limit to k stations to load
        :return: 
        """
        station_dicts = []
        logging.debug("using summary dir: " + self.summary_dir)
        for station_name, lat, lon in self.get_all_stations(limit).itertuples():
            station_dict = self.load_station(station_name, start_date, end_date, time_zone, limit_to_temperature)
            if station_dict is not None:
                station_dicts.append(station_dict)
                logging.debug("load " + station_name)
            else:
                self.stations_df.lat.loc[station_name] = numpy.nan  # mark the station to be removed
        self.stations_df = self.stations_df[self.stations_df.lat.notnull()]  # remove stations which no data
        logging.debug("loaded station_dicts: " + str(len(station_dicts)))
        return station_dicts

    def load_station(self, station, start_date, end_date, time_zone=None, limit_to_temperature=True):
        """
        This only looks at the dates and returns the corresponding summary (assuming naive dates, overlapping at 
        midnight is ignored).
        
        :param limit_to_temperature: Only consider temperature - saves memory but drops interesting data
        :param station: The station to load
        :param start_date: The earliest day which must be included (potentially earlier)
        :type start_date: str | datetime.datetime | None
        :param end_date: The latest day which must be included (potentially later)
        :type end_date: str | datetime.datetime | None
        :param time_zone: The time zone, e.g. 'CET' or GermanWinterTime() or None for naive datetime interpretation
        :type time_zone: datetime.tzinfo | str | None
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
        csv_file = os.path.join(self.summary_dir, searched_station_summary_file_name)
        if limit_to_temperature:
            usecols = ["datetime", "temperature"]
        else:
            usecols = None  # that is the default value in the function declaration
        station_df = pandas.read_csv(
            csv_file,
            usecols=usecols,
            index_col="datetime",
            parse_dates=["datetime"]
        )
        if station_df.empty or station_df.temperature.count() == 0:
            logging.debug("Not enough data for '{station}' at all".format(station=station))
            return None
        if time_zone is not None:
            station_df = station_df.tz_localize("UTC").tz_convert(time_zone).tz_localize(None)
        if not station_df.index.is_monotonic:  # Some JSONs are damaged, so we need to sort them again
            station_df.sort_index(inplace=True)
        if start_date is not None:
            before_start_date = (start_date - datetime.timedelta(minutes=1))
            before_start_df = station_df[:before_start_date]
            if not before_start_df.empty and before_start_df.temperature.count() > 0:
                pass

                # logging.warning("Data found for {station} before start date '{before_start_date}'"
                #                .format(station=station, before_start_date=before_start_date))
                # logging.info(before_start_df.describe())
                # before_start_df.info()
            station_df = station_df[start_date:]
        if end_date is not None:
            after_end_date = (end_date + datetime.timedelta(days=1))
            after_end_df = station_df[after_end_date:]
            if not after_end_df.empty or after_end_df.temperature.count() > 0:
                pass
                # logging.warning("Data found for {station} after end date '{after_end_date}'"
                #                .format(station=station, after_end_date=after_end_date))
                # logging.info(after_end_df.describe())
                # after_end_df.info()
            station_df = station_df[:after_end_date]
        if station_df.empty or station_df.temperature.count() == 0:
            logging.debug("Not enough data for '{station}' during provided time period".format(station=station))
            return None
        station_df = station_df.groupby(station_df.index).first()  # remove duplicates
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

    def _search_summary_file(self, station, start_date, end_date):
        searched_station_summary_file_name = None
        for station_summary_file_name in self.cached_os_list_dir:
            if not station_summary_file_name.endswith(".csv"):
                continue
            if not station_summary_file_name.startswith(station):
                continue
            station_summary_file_name = station_summary_file_name[:-4]  # cut of '.csv'
            file_name_parts = station_summary_file_name.split("_")
            if len(file_name_parts) == 1:
                searched_station_summary_file_name = file_name_parts[0] + ".csv"
                break
            elif len(file_name_parts) == 3:
                station_part, start_date_span_text, end_date_span_text = file_name_parts
                if station_part != station:
                    continue
                if start_date is None and end_date is None:
                    searched_station_summary_file_name = station_summary_file_name + ".csv"
                    break
                start_date_span = datetime.datetime.strptime(start_date_span_text, "%Y%m%d")
                start_date_span = start_date_span.replace(tzinfo=start_date.tzinfo)
                start_date_span = start_date_span.replace(hour=0, minute=0)
                end_date_span = datetime.datetime.strptime(end_date_span_text, "%Y%m%d")
                end_date_span = end_date_span.replace(tzinfo=end_date.tzinfo)
                end_date_span = end_date_span.replace(hour=23, minute=59)
                if start_date_span <= start_date and end_date_span >= end_date:
                    searched_station_summary_file_name = station_summary_file_name + ".csv"
                    break
        return searched_station_summary_file_name

    def _get_metadata(self, station):
        if self.stations_df is None:
            self.get_all_stations()
        return self.stations_df.loc[station]
