"""

"""

import sys
import logging
import os.path
import datetime
import platform

import pandas
import numpy
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_absolute_error

from filter_weather_data import PROCESSED_DATA_DIR


if platform.uname()[1].startswith("ccblade"):  # the output files can turn several gigabyte so better not store them
                                               # on a network drive
    PROCESSED_DATA_DIR = "/export/scratch/1kastner"

pandas.set_option("display.max_columns", 500)
pandas.set_option("display.max_rows", 10)


def cloud_cover_converter(val):
    if val in ["SKC", "CLR", "NSC", "CAVOC"]:  # 0 octas
        return 0
    elif val == "FEW":  # 1-2 octas
        return 1
    elif val == "SCT":  # 3-4 octas
        return 2
    elif val == "BKN":  # 5-7 octas
        return 3
    elif val == "OVC":  # 8 octas
        return 4
    elif val == "VV":  # clouds can not be seen because of rain or fog
        return 5
    else:
        raise RuntimeError(val + "not found")


def load_data(file_name, start_date, end_date, verbose=False):
    """

    :param end_date:
    :param start_date:
    :param file_name: File name, e.g. training_data.csv, evaluation_data.csv
    :return: (input_data, target) scikit-conform data
    """
    csv_file = os.path.join(
        PROCESSED_DATA_DIR,
        "neural_networks",
        file_name
    )

    data_df = pandas.read_csv(
        csv_file,
        index_col="datetime",
        parse_dates=["datetime"],
        converters={"cloudcover_eddh": cloud_cover_converter}
    )

    data_df = data_df[start_date:end_date]

    cloud_cover_df = pandas.get_dummies(data_df.cloudcover_eddh, prefix="cloudcover_eddh")

    df_hour = pandas.get_dummies(data_df.index.hour, prefix="hour")

    data_df.reset_index(inplace=True)

    data_df = pandas.concat([
        data_df,
        df_hour,
        cloud_cover_df
    ], axis=1)

    # this is now binary encoded, so no need for it anymore
    del data_df["cloudcover_eddh"]

    # no data means no windgusts were measured, not the absence of measurement instruments
    data_df["windgust_eddh"].fillna(0, inplace=True)

    # drop columns with NaN, e.g. precipitation at airport is currently not reported at all
    data_df.dropna(axis='columns', how="all", inplace=True)

    # neural networks can not deal with NaN values
    data_df.dropna(axis='index', how="any", inplace=True)

    # try to predict temperature
    target_df = pandas.DataFrame(data_df.temperature)

    # based on information served by airport + learned patterns, so no data from the same private weather station itself
    input_df = data_df
    for attribute in data_df.columns:
        if (
                not attribute.endswith("_eddh") 
                and attribute not in ("lat", "lon") 
                and not attribute.startswith("hour_")
                and not attribute.startswith("month_")
                and not "cloudcover" in attribute
        ):
            input_df.drop(attribute, 1, inplace=True)

    if verbose:
        logging.debug(input_df.head(1))
        logging.debug(target_df.head(1))

    # only numpy arrays conform with scikit-learn
    input_data = input_df.values
    target = target_df.values

    return input_data, target


def train(mlp_regressor, start_date, end_date, verbose=False):
    input_data, target = load_data("training_data.csv", start_date, end_date, verbose=verbose)
    mlp_regressor.fit(input_data, target)
    predicted_values = mlp_regressor.predict(input_data)
    score = numpy.sqrt(mean_absolute_error(target, predicted_values))
    logging.info("Training RMSE: %.3f" % score)


def evaluate(mlp_regressor, start_date, end_date, verbose=False):
    input_data, target = load_data("evaluation_data.csv", start_date, end_date, verbose=verbose)
    predicted_values = mlp_regressor.predict(input_data)
    score = numpy.sqrt(mean_absolute_error(target, predicted_values))
    logging.info("Evaluation RMSE: %.3f" % score)


def run_experiment(hidden_layer_sizes, number_months=12, learning_rate=.001):
    """

    :param hidden_layer_sizes: The hidden layers, e.g. (40, 10)
    :return:
    """
    mlp_regressor = MLPRegressor(
        hidden_layer_sizes=hidden_layer_sizes,
        activation='relu',  # most likely linear effects
        solver='adam',  # good choice for large data sets
        alpha=0.0001,  # L2 penalty (regularization term) parameter.
        batch_size='auto',
        learning_rate_init=learning_rate,
        max_iter=200,

        shuffle=True,

        random_state=None,
        tol=0.0001,
        #verbose=True,
        verbose=False,
        warm_start=False,  # erase previous solution

        early_stopping=False,  # stop if no increase during validation
        validation_fraction=0.1,  # belongs to early_stopping

        beta_1=0.9,  # solver=adam
        beta_2=0.999,  # solver=adam
        epsilon=1e-08  # solver=adam
    )

    setup_logger(hidden_layer_sizes)
    logging.info("hidden_layer_sizes=%s" % str(hidden_layer_sizes))
    for month in range(1, number_months):
        month_learned = "2016-%02i" % month
        logging.info("learn month %s" % month_learned)
        train(mlp_regressor, month_learned, month_learned, verbose=(month == 1))
        month_not_yet_learned = "2016-%02i" % (month + 1)
        logging.info("validate with month %s" % month_not_yet_learned)
        evaluate(mlp_regressor, month_not_yet_learned, month_not_yet_learned)
    logging.info(mlp_regressor.get_params())


def setup_logger(hidden_layer_sizes):
    log = logging.getLogger('')

    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    file_name = "interpolation_{date}_neural_network_{hidden_layer_sizes}.log".format(
        hidden_layer_sizes="-".join([str(obj) for obj in hidden_layer_sizes]),
        date=datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-")
    )
    path_to_file_to_log_to = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "log",
        file_name
    )
    file_handler = logging.FileHandler(path_to_file_to_log_to)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    log.propagate = False

    log.info("### Start new logging")
    return log


if __name__ == "__main__":
    run_experiment((3,), number_months=2)
