import pandas as pd

from zappa_distributor import my_map


def calculate_result(df, options):
    """
    Main function of our application: calculate the sum of each time series for each "id" separately
    and return the dataframe.
    :param df: The input data.
    :param options: The options for the calculation given by the user.
    :return: The output data.
    """
    chunksize = int(options.get("chunksize", 5))
    id_column = options.get("id_column", "id")
    value_column = options.get("value_column", "value")
    compression = options.get("compression", "False") == "True"

    grouped_data = df.groupby(id_column)[value_column]

    def convert_to_dict(x):
        series_id, series = x
        return {"id": series_id, "values": list(series.values), "name": series.name}

    grouped_data = map(convert_to_dict, grouped_data)

    result = my_map(feature_calculation, grouped_data, chunksize=chunksize, compression=compression)
    result = pd.DataFrame(result).set_index("id", drop=True)

    return result


def feature_calculation(chunk):
    """
    Here, the actual calculation of the sum happens for a chunk consisting of
    the id and the time series of the data.
    :param chunk: The id and the time series the sum will be calculated for.
    :return: A dict with the id and the result of the calculation.
    """
    timeseries = pd.Series(chunk["values"], name=chunk["name"])
    timeseries_id = chunk["id"]
    return {"id": timeseries_id, "result": timeseries.sum()}
