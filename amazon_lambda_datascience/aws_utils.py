import json
import os
from flask import request
import pandas as pd
from zappa.async import get_func_task_path, LambdaAsyncResponse, AsyncException


def get_dataframe():
    """Return the dataframe given by the user"""
    return pd.read_csv(request.stream)


def get_options():
    """Return the options given by the user"""
    return request.args


def print_result(result):
    """Return the result in a form, that can be returned to the user"""
    return result.to_csv(index="_index", header=True)


def send_to_other_lambda(function_in_lambda, *args, **kwargs):
    """
    Call the function_in_lambda in another lambda instead of the running one
    and return the result.
    :param function_in_lambda: The function to call.
    :param args: The arguments to this function.
    :param kwargs: The lwargs to this function.
    :return: The result of this call.
    """
    lambda_function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
    aws_region = os.environ.get('AWS_REGION')

    task_path = get_func_task_path(function_in_lambda)
    result = LambdaSyncResponse(lambda_function_name=lambda_function_name,
                                aws_region=aws_region).send(task_path, args, kwargs).response

    return result


class LambdaSyncResponse(LambdaAsyncResponse):
    """
    Helper class inheriting from LambdaAsyncResponse to start a new lambda function.
    """
    def _send(self, message):
        """
        Send method overloading the one from LambdaAsyncResponse, to call the lambda
        synchronously. Still mostly copied from the original function.
        """
        message['command'] = 'zappa.async.route_lambda_task'
        payload = json.dumps(message, encoding="utf8").encode('utf-8')
        if len(payload) > 6000000:  # pragma: no cover
            raise AsyncException("Payload too large for sync Lambda call")

        self.response = json.loads(self.client.invoke(
            FunctionName=self.lambda_function_name,
            InvocationType='RequestResponse',
            Payload=payload
        )["Payload"].read())
