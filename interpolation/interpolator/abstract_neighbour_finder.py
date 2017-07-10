"""

"""
import pandas


class AbstractNeighbourFinder:

    # make a measurement valid for 30 minutes
    DECAY = 30

    def _sample_up(self, station_dict, start_date, end_date):
        df = station_dict["data_frame"]
        df_year = pandas.DataFrame(index=pandas.date_range(start_date, end_date, freq='T', name="datetime"))
        df = df.join(df_year, how="outer")
        df = df.resample('1T').asfreq()
        df.temperature = df.temperature.ffill(limit=self.DECAY)
        station_dict["data_frame"] = df
