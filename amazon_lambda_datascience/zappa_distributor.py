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

    result = map(map_function, partitioned_chunks)
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

