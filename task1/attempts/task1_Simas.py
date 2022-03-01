"""
Big Data Analysis
    Gonçalo Marques
    Multiprocessing template
"""

import pandas as pd
import numpy as np
import multiprocessing as mp
import collections

# Please, calculate how many words are unique and count each word in the data set.
# The separation of the word in a sentence should be done by a space character.
# What are the top 10 most frequent words? Please lowercase all words.

def count_words(abst):
    abst = abst.lower() # lowercasing all
    # for i in range(2, 50):
    #     spaces = " "*i
    #     abst = abst.replace(spaces, " ")
    words = abst.split(" ") # spliting all
    counter = collections.Counter(words) # dictionary of all words counted
    return counter


if __name__ == '__main__':
    print('Program Started!')
    filename = "covid_abstracts_test.csv"
    chunksize = 1000
    
    # Reading csv in iterate mode.   
    iter_csv = pd.read_csv(filename, iterator=True, chunksize=chunksize, usecols=["title", "abstract", "url"])
    
    # Preparing CPU pool
    cpus = mp.cpu_count()
    print(cpus)
    pool = mp.Pool(cpus)
    
    # total = {"title": [], "unique_count": [], "top10": []}
    count_all_words = {}
    # all_words = []
    c = 1
    # Process each chunk individualy. It can be processes in parallel as well.
    for chunk_df in iter_csv:
        print(f"Chunk being processed {c}, data index read: {c * chunksize}")
        
        # Geting all vessels IDs in chunck
        absts = ""
        for i in range(chunk_df.shape[0]):
            absts += list(chunk_df["abstract"])[i]
        print(type(absts), len(absts))
        # vessels = chunk_df["MMSI"].unique()
        
        # Calculating each vessel in parallel
        results = pool.apply(count_words, args=([absts]))
        
        # summing results paralel results with previous chunks results 
        # emt = {total.update({k: v + total.get(k, 0)}) for d in results for k, v in d.items()}

        for word in results:
            if word in count_all_words.keys():
                count_all_words[word] += results[word]
            else:
                count_all_words[word] = results[word]

        c += 1

    len_unique = len(count_all_words.keys())
    # top10 = sorted(counter, reverse = True)[:10]
    sorted_counter = {k: v for k, v in sorted(count_all_words.items(), key=lambda item: item[1], reverse = True) if k != ""}
    top10 = {}
    c = 1
    for k in sorted_counter:
        top10[k] = sorted_counter[k]
        c += 1
        if c > 10:
            break

    pool.close()

    print("Unique words: "+str(len_unique))
    print("Top10: "+str(top10))