from flask import request
import pandas as pd


def get_dataframe():
    """Return the dataframe given by the user"""
    return pd.read_csv(request.stream)


def get_options():
    """Return the options given by the user"""
    return request.args


def print_result(result):
    """Return the result in a form, that can be returned to the user"""
    return result.to_csv(index="_index", header=True)
