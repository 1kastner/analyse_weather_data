"""

"""
import datetime

import pandas
import dateutil.parser


class AbstractNeighbourFinder:

    # make a measurement valid for 30 minutes
    DECAY = 30

    def sample_up(self, station_dict, start_date, end_date):
        """
        
        :param station_dict: The station dict to sample up
        :param start_date: earliest date (included) for upsampling
        :param end_date: latest date (included) for upsampling
        :return: 
        """
        if "is_sampled_up" in station_dict and station_dict["is_sampled_up"]:
            return
        if isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)
        df = station_dict["data_frame"]
        real_end_date = end_date + datetime.timedelta(days=1)
        df_year = pandas.DataFrame(index=pandas.date_range(start_date, real_end_date, freq='T', name="datetime"))
        df = df.join(df_year, how="outer")
        df.temperature.fillna(method="ffill", limit=self.DECAY, inplace=True)
        station_dict["data_frame"] = df
        station_dict["is_sampled_up"] = True
