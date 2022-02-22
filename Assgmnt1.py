import pandas as pd
from math import sin, cos, sqrt, atan2, radians
import numpy as np
from time import perf_counter, sleep, time
import multiprocessing as mp


def distance(latitude, longitude):
    R = 6373.0  # approximate radius of earth in km

    dlon = radians(longitude[1]) - radians(longitude[0])

    dlat = radians(latitude[1]) - radians(latitude[0])

    a = sin(dlat / 2) ** 2 + cos(radians(latitude[0])) * cos(radians(latitude[1])) * sin(dlon / 2) ** 2

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def gather_results(d, j):
    global data
    data["MMSI"].append(j)
    data["mean_distance"].append(np.mean(d))
    data["std_distance"].append(np.std(d))
    data["min_distance"].append(np.min(d))
    data["max_distance"].append(np.max(d))


def get_data(df, j):
    d = []
    for i in range(df.shape[0] - 1):
        lat = list(df['Latitude'])
        long = list(df['Longitude'])

        lat = lat[i:i + 2]

        long = long[i:i + 2]
        dist = distance(lat, long)
        d.append(dist)
    return d, j


def main():
    print(mp.cpu_count())

    starttime = perf_counter()
    print("Starting")

    aisdk_df = pd.read_csv("aisdk-2022-02-19.csv")

    aisdk_df = aisdk_df.head()

    print(aisdk_df.shape)

    # The Goal of the task is to calculate mean, std, min, max of marine vessels sailed distance, grouped by vessel.

    # global data
    data = {'MMSI': [], 'mean_distance': [], 'std_distance': [], 'min_distance': [], 'max_distance': []}

    # d = []

    mmsi_unique = aisdk_df['MMSI'].unique()
    mmsi_unique = mmsi_unique[:5]

    cpu_pool = mp.Pool(mp.cpu_count())

    for i, j in enumerate(mmsi_unique):
        part_df = aisdk_df.loc[aisdk_df['MMSI'] == j]
        cpu_pool.apply_async(get_data, args=(part_df, j), callback=gather_results)

    cpu_pool.close()
    cpu_pool.join()

    endtime = perf_counter()
    print(endtime - starttime)

    df = pd.DataFrame(data)

    df.head()



if __name__ == '__main__':
    main()
