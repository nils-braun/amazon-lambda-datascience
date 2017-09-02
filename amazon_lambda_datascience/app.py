from __future__ import print_function
from time import time

from amazon_lambda_datascience import aws_utils, df_utils


def add_routes(app):
    """Add all routes to the application"""
    @app.route('/', methods=['PUT'])
    def main():
        """
        Main method: calculate the features of the given csv dataframe.
        """
        df = aws_utils.get_dataframe()
        options = aws_utils.get_options()

        start = time()
        result = df_utils.calculate_result(df, options)
        end = time()

        print("Calculation needed", end - start, "seconds.")

        return aws_utils.print_result(result)
