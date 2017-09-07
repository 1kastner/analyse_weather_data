"""

"""

import os.path

import pandas
import numpy
from sklearn.neural_network import MLPRegressor

from filter_weather_data import PROCESSED_DATA_DIR


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


def load_data(file_name):
    """

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

    for i in range(6):
        column_name = 'cloudcover_%i' % i
        data_df[column_name] = (data_df["cloudcover_eddh"] == i)

    data_df["month"] = data_df.index.month
    data_df["hour"] = data_df.index.hour

    # this is now binary encoded, so no need for it anymore
    del data_df["cloudcover_eddh"]

    # drop columns with NaN, e.g. precipitation at airport is currently not reported at all
    data_df.dropna(axis='columns', how="all", inplace=True)

    # neural networks can not deal with NaN values
    data_df.dropna(axis='index', how="any", inplace=True)

    # try to predict temperature
    target_df = pandas.DataFrame(data_df.temperature)

    # based on information served by airport + learned patterns, so no data from the same private weather station
    input_df = data_df
    for attribute in data_df.columns:
        if not attribute.endswith("_eddh"):
            input_df = input_df.drop(attribute, 1)

    # only numpy arrays conform with scikit-learn
    input_data = input_df.values
    target = target_df.values

    return input_data, target


def train(mlp_regressor):
    input_data, target = load_data("training_data.csv")
    mlp_regressor.fit(input_data, target)
    score = numpy.sqrt(mlp_regressor.score_mse(input_data, target))
    print("Training Score Predicted: ", score)


def evaluate(mlp_regressor):
    input_data, target = load_data("evaluation_data.csv")
    score = numpy.sqrt(mlp_regressor.score_mse(input_data, target))
    print("Evaluation Score Predicted: ", score)


def run_experiment():
    mlp_regressor = MLPRegressor(
        hidden_layer_sizes=(10,),
        activation='relu',  # most likely linear effects
        solver='adam',  # good choice for large data sets
        alpha=0.0001,  # L2 penalty (regularization term) parameter.
        batch_size='auto',
        learning_rate_init=0.001,
        max_iter=200,

        shuffle=True,

        random_state=None,
        tol=0.0001,
        verbose=True,
        warm_start=False,  # erase previous solution

        early_stopping=False,  # stop if no increase during validation
        validation_fraction=0.1,  # belongs to early_stopping

        beta_1=0.9,  # solver=adam
        beta_2=0.999,  # solver=adam
        epsilon=1e-08  # solver=adam
    )
    train(mlp_regressor)
    evaluate(mlp_regressor)
    print(mlp_regressor.get_params())


if __name__ == "__main__":
    run_experiment()
