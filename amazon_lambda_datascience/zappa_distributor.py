import itertools
import json
import zlib
from functools import partial


def my_map(f, data, chunksize, compression):
    """
    Own implementation of the python "map" function looping
    over the data and calculating f on each of the items.
    Returns a list of the results calculated by each of the
    single calculations.
    :param f: The function to calculate.
    :param data: The data to loop over.
    :param chunksize: The chunksize the data is chunked into before doing the calculation.
    :return: A list of the results from every calculation.
    """
    partitioned_chunks = partition(data, chunk_size=chunksize)

    map_function = partial(feature_calculation_on_chunks, f=f)

    result = distribute_to_lambda(map_function, partitioned_chunks, compression=compression)
    reduced_result = list(itertools.chain.from_iterable(result))

    return reduced_result


def partition(data, chunk_size):
    """
    Helper function to chunk a list of data items with the given chunk size.
    This is done with the help of some iterator tools.
    :param data: The data to chunk.
    :param chunk_size: The size of one chunk. The last chunk may be smaller.
    :return: A generator producing the chunks of data.
    """
    # Create a generator out of the input list
    iterable = iter(data)
    while True:
        next_chunk = list(itertools.islice(iterable, chunk_size))
        if not next_chunk:
            return

        yield next_chunk


def feature_calculation_on_chunks(chunk_list, f):
    """
    Helper function to make the partitioning undone: loop over all chunks in the
    chunk list and calculate the features for each. Return a list of all results.
    :param chunk_list: The list of chunks to calculate for.
    :return: A list of results.
    """
    results = [f(chunk) for chunk in chunk_list]
    return results


def encode_payload(payload, compression):
    """
    Encode an arbitrary python object to be transportable in
    JSON objects.
    :param payload: The object to transport.
    :param compression: Turn on compression or not.
    :return: A string with the object representation.
    """
    json_string = json.dumps(payload)

    if compression:
        compressed_string = zlib.compress(json_string)
        return compressed_string
    else:
        return json_string


def decode_payload(compressed_string, compression):
    """
    Decode a string representation of a python object
    back to the object.
    :param compressed_string: The python representation of the object.
    :param compression: Was compression turned on during encoding?
    :return: The python object
    """
    if compression:
        json_string = zlib.decompress(compressed_string)
    else:
        json_string = compressed_string

    return json.loads(json_string)


def distribute_to_lambda(map_function, iterable_data, compression):
    """
    Method comparable to pythons `map` function, which
    calls a given map function on an iterable data set.
    However, the single items of the iterable are all given
    to different lambda functions in parallel.

    The lambdas are invoked using threading.

    The data needs to be in the format list of dictionaries of simple python types.

    :param map_function: The function that should be called on each item
    :param iterable_data: The list of items that is distributed
    :param compression: Turn on compression on streaming the data.
    :return: The list of results for each lambda.
    """
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool()

    prefilled_map = partial(run_lambda, map_function=map_function, compression=compression)
    results = pool.map(prefilled_map, iterable_data)

    return results


def run_lambda(data, map_function, compression):
    """
    Run a given map_function on the given data and return the result.

    For this:
    * the data is encoded (using compression or not),
    * a lambda function is invoked, which decoded the data, calls the map_function and encoded the result again
    * the result is decoded again and returned.
    :param data: The data that is sent to the lambda function
    :param map_function: The function that is called in the lambda on the data.
    :param compression: Turn on compression during streaming or not.
    :return: The result of the function call.
    """
    encoded_data = encode_payload(data, compression)
    # TODO: we are cheating a bit here, because we are just calling the function instead of sending it to another lambda
    encoded_result = function_in_lambda(encoded_data, map_function, compression)
    return decode_payload(encoded_result, compression)


def function_in_lambda(encoded_data, map_function, compression):
    """
    Helper function that is actually called in the lambda (instead of the map_function directly),
    because the input as well as the output data must be encoded/decoded.
    :param encoded_data: The encoded data that will be decoded before feeding into the map_function.
    :param map_function: The function that is called on the data.
    :param compression: Turn on compression during streaming.
    :return: The encoded result of the function call.
    """
    data = decode_payload(encoded_data, compression)
    result = map_function(data)
    return encode_payload(result, compression)