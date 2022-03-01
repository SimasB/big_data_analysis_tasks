"""
Big Data Analysis
    Gonçalo Marques
    Multiprocessing template
"""

import pandas as pd
import numpy as np
import multiprocessing as mp


if __name__ == '__main__':
    print('Program Started!')
    filename = "C:/Users/gfmm2/Desktop/Erasmus/Cadeiras/Big Data Analysis/Practical/Feb 22/aisdk-2022-02-19.csv"
    chunksize = 1000000
    
    # Reading csv in iterate mode.   
    iter_csv = pd.read_csv(filename, iterator=True, chunksize=chunksize, usecols=["MMSI", "Latitude", "Longitude"])
    
    # Preparing CPU pool
    cpus = mp.cpu_count()
    print(cpus)
    pool = mp.Pool(cpus)
    
    total = {}
    counter = 1
    # Process each chunk individualy. It can be processes in parallel as well.
    for chunk_df in iter_csv:
        print(f"Chunk being processed {counter}, data index read: {counter * chunksize}")
        counter += 1
        
        # Geting all vessels IDs in chunck
        vessels = chunk_df["MMSI"].unique()
        
        # Calculating each vessel in parallel
        results = [pool.apply(distance_vessel, args=(vessel, chunk_df)) for vessel in vessels]
        
        # summing results paralel results with previous chunks results 
        emt = {total.update({k: v + total.get(k, 0)}) for d in results for k, v in d.items()}
        print(f'Test vessel distance in kilometers: {total.get(257316000)}')
        
        # Limiting number of chunks for debuging
        # if counter >= 2: break
    pool.close()
    print(total)
    df = pd.DataFrame.from_dict(total, orient='index')
    print(df.head(10))
    print(df.describe())