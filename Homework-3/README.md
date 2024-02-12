# Week 3 Homework 

### Question 1

import pandas as pd
import numpy as np
import pandas_gbq
import logging
from google.oauth2 import service_account


def load_data(url):
        dfs = []
        for month in range(1,13):
            query = f'{url}-{str(month).zfill(2)}.parquet'
            dfs.append(pd.read_parquet(query))
        
        df_merged = pd.concat(dfs,ignore_index=True)
        print("df merged shape is ", df_merged.shape)
        return df_merged

df = load_data("https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022")

df merged shape is  (840402, 20)

### Question 2



### Question 3

Number of data with fare_amount is 0:  1622

