"""
Big Data Analysis
    Group 7 - Version 1
    Assignment 1
"""

import pandas as pd
import multiprocessing as mp
import collections
import time
import matplotlib.pyplot as plt
import random

random.seed(0)

# Please, calculate how many words are unique and count each word in the data set.
# The separation of the word in a sentence should be done by a space character.
# What are the top 10 most frequent words? Please lowercase all words.

def count_words(abst):
    abst = abst.lower() # lowercasing all
    words = abst.split(" ") # spliting all
    counter = collections.Counter(words)
    return counter

if __name__ == '__main__':
    print('Program Started!')
    filename = "covid_abstracts_test.csv"
    chunksize = 1000
    
    # Number of cpus
    cpus = mp.cpu_count()
    print(f"{cpus} available")
    
    # Plot
    plot_cpus = range(1, cpus+1)
    plot_time = []
    
    for cpu in range(1, cpus+1):
        # Time
        start = time.time()
        
        # Reading csv in iterate mode.   
        iter_csv = pd.read_csv(filename, iterator=True, chunksize=chunksize, usecols=["title", "abstract", "url"])
        
        # Preparing CPU pool
        pool = mp.Pool(cpu)
        
        count_all_words = {}
        c = 1
        # Process each chunk individualy. It can be processes in parallel as well.
        for chunk_df in iter_csv:
            c += 1
            
            # concatenating all abtstracts in the chunck
            absts = ""
            for i in range(chunk_df.shape[0]):
                absts += list(chunk_df["abstract"])[i]
            absts = [absts]
            
            # pool apply
            results = pool.apply(count_words, args=(absts))
            
            # Append results
            for word in results:
                if word in count_all_words.keys():
                    count_all_words[word] += results[word]
                else:
                    count_all_words[word] = results[word]

        len_unique = len(count_all_words.keys())
        sorted_counter = {k: v for k, v in sorted(count_all_words.items(), key=lambda item: item[1], reverse = True) if k != ""}        
        top10 = {}
        c = 1
        for k in sorted_counter:
            top10[k] = sorted_counter[k]
            c += 1
            if c > 10:
                break
        pool.close()
        
        running_period = time.time() - start 
        plot_time.append(running_period)
        
        print(f"CPUs used: {cpu} with {running_period} seconds")

    print("Unique words: " + str(len_unique))
    print("Top10: " + str(top10))
        
    plt.plot(plot_cpus, plot_time)
    plt.title("Running Time vs Number of CPUs")
    plt.ylabel("Time (s)")
    plt.xlabel("CPUs")
    plt.savefig("plot.png")
    plt.show()