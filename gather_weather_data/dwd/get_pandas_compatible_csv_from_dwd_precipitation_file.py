"""

Run with
-m gather_weather_data.dwd.get_pandas_compatible_csv_from_dwd_precipitation_file
to start it as a standalone script.
"""

import os

import pandas
import dateutil.parser

from . import DWD_RAW_DATA_DIR
#from gather_weather_data.dwd import DWD_RAW_DATA_DIR

HEADER = "STATIONS_ID; MESS_DATUM; QUALITAETS_NIVEAU; NIEDERSCHLAGSHOEHE;NIEDERSCHLAGSHOEHE_IND; SCHNEEHOEHE;eor\n"


def open_dwd_precipitation_file(path_to_file):
    with open(path_to_file) as f:
        if HEADER != f.readline():
            raise RuntimeError("I can not deal with this file")
        df_data = []
        for row in f.readlines():
            entries = [entry.strip() for entry in row.split(";") if entry != "eor"]
            date = dateutil.parser.parse(entries[1])
            precipitation = float(entries[3])
            df_data.append({"date": date, "precipitation": precipitation})
        df = pandas.DataFrame(df_data)
        df.set_index(df["date"], inplace=True)
        return df


def load_dwd_precipitation(start_date, end_date):
    file_name = "produkt_klima_Tageswerte_20151016_20170417_01975.txt"
    path_to_file = os.path.join(
        DWD_RAW_DATA_DIR,
        file_name
    )
    df = open_dwd_precipitation_file(path_to_file)
    df = df[start_date:end_date]
    return df


def demo():
    df = load_dwd_precipitation("2016-01-01", "2016-12-31")
    df.info()

if __name__ == "__main__":
    demo()
