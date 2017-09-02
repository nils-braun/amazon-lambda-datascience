import pandas as pd
import numpy as np


def generate_data(length):
    for index in range(length):
        for time, random_value in enumerate(np.random.rand(100)):
            yield {"id": index, "value": random_value, "time": time}

if __name__ == '__main__':
    df = pd.DataFrame(generate_data(1000))
    df.to_csv("data.csv")
