import pandas as pd
import bz2
import os
import numpy as np
import time
import matplotlib.pyplot as plt

def chunkify(filepath, chunk_size, outputname, timing=False):
    """
    This function chunk quotebank files to multiple smaller files. 
    
    INPUTS: 
    filepath: path of quotebank file to chunk. 
    chunk_size: (int) it is a row number where the file will be chuncked at
    outputname: is what every output chunk starts their name as
    timing:  is per chunk for benchmarks
    """
    batch_no = 1
    for chunk in pd.read_json(filepath, chunksize=chunk_size, lines=True, compression='bz2'):
        # Taking the time of loading each chunk
        if timing:
            before = time.time()

        output = 'Data/' + outputname + '-' + str(batch_no) + '.csv'

        chunk.to_csv(output, index=False)

        compression_level = 9
        # Source file for bz2 comrpession
        source_file = output
        destination_file = output + '.bz2'

        with open(source_file, 'rb') as data:
            # Reads the content of the file and makes a compressed copy
            compressed = bz2.compress(data.read(), compression_level)
        fh = open(destination_file, "wb")
        # Make a new compressed file with compressed content
        fh.write(compressed)
        fh.close()

        # Removes the .csv file to save space
        os.remove(output)

        if timing:
            after = time.time()
            print(after - before, 's')

        batch_no += 1

def find_csv_filenames(path_to_dir, year):
    """
    Finds all chunkfiles that belongs to a given year and is in a given directory
    """
    filenames = os.listdir(path_to_dir)
    return [filename for filename in filenames if filename.startswith("quotes-" + str(year) + "-")]    

def get_quotes(speaker, year, timing=False):
    """
    returns the dataset with only quotes from the given speaker from the files of a given year
    timing for the whole function for benchmarks
    """
    if timing:
        before = time.time()

    cd = 'Data/' # Set working directory
    filenames = find_csv_filenames(cd, year) #Get chunks from a given year
    file_arr = np.array(filenames) # change list to numpy array
    N = len(filenames) #Number of chunks
    df1 = pd.read_csv(cd + file_arr[0]) # load first chunk
    df_all = df1[df1["speaker"]==speaker] # Extract elon musk quotes from first chunk file
    # For loop through all chunks and concat data frames to have one data frame with all elon musk quotes
    for i in range(1,N):
        name_2load = cd + file_arr[i]
        current_df = pd.read_csv(name_2load)
        df_elo_current = current_df[current_df["speaker"]==speaker]
        df_all = pd.concat([df_all, df_elo_current], axis=0)

    if timing:
        after = time.time()
        print(after - before, 's')

    return df_all

def make_csv(dataFrame, speaker, year, compression='bz2'):
    """
    create a compressed csv of a dataframe of quotes for a speaker and a year
    """
    dataFrame.to_csv('Data/' + speaker + '-quotes-' + str(year) + '.csv.' + compression, index=False)

