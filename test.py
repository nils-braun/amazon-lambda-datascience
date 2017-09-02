from __future__ import print_function
import requests
import pandas as pd
from io import BytesIO
from time import time

if __name__ == '__main__':

    df = pd.read_csv("data.csv")
    start = time()
    url = "http://localhost:5000"
    with BytesIO() as stream:
        df.to_csv(stream)
        stream.seek(0)
        answer = requests.put(url, data=stream,
                              params={"chunksize": "10", "id_column": "id", "value_column": "value"})

    end = time()
    s = StringIO()
    s.write(answer.text)
    s.seek(0)
    result_df = pd.read_csv(s, index_col="id")
    s.close()

    print(result_df.head())
    print("The calculation took", end - start, "seconds")
