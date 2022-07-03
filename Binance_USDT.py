# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 20:59:16 2021

@author: Pastor
"""

## The data is taken using EST time

import requests                    # for "get" request to API
import json                        # parse json into a list
import pandas as pd                # working with data frames
import datetime as dt              # working with dates
import matplotlib.pyplot as plt    # plot data
import qgrid                       # display dataframe in notebooks
import os
import time
from threading import Thread

# Global variables
BASE_URL = 'https://api.binance.com'
symbols = []
pair = "USDT"#input('set the pair you want to downloand (USDT, BTC, ETH): >> ')
timeframe = "1d" #input('set the timeframe you want to download (1d, 1m): >> ') # timeframe use to get the data
n_pair = len(pair)
folder = r'D:\Dropbox\Pastor\data\binance_data_{0}'.format(timeframe) # folder where you want to store your data



# This function allow to get all symbols name pairs with USDT we can change it to use different pairs like BTC

resp = requests.get(BASE_URL + '/api/v1/ticker/allBookTickers')
tickers_list = json.loads(resp.content)
for ticker in tickers_list:
    if str(ticker['symbol'])[-n_pair:] == pair:
        symbols.append(ticker['symbol'])
        

# this function allow to get the data from binance on EST time

def get_binance_bars(symbol, interval, startTime, endTime):
 
    url = "https://api.binance.com/api/v3/klines"
 
    startTime = str(int(startTime.timestamp() * 1000))
    endTime = str(int(endTime.timestamp() * 1000))
    limit = '1000'
 
    req_params = {"symbol" : symbol, 'interval' : interval, 'startTime' : startTime, 'endTime' : endTime, 'limit' : limit}
 
    df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text))
 
    if (len(df.index) == 0):
        return None
     
    df = df.iloc[:, 0:6]
    df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
 
    df.open      = df.open.astype("float")
    df.high      = df.high.astype("float")
    df.low       = df.low.astype("float")
    df.close     = df.close.astype("float")
    df.volume    = df.volume.astype("float")
    
    df['adj_close'] = df['close']
     
    df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df.datetime]
 
    return df

# Get the last year, month and day using in the main file to get the data only after that time
# Ada is only use to get the last day available in the file


# loop to get the data for all symbols and combine them into a single file

for i in symbols:
    df_list = []
    
    # path_crypto needs to be updated with your working directory and folder where you want store the data
    path_crypto = '{0}\{1}.csv'.format(folder, i)
    if os.path.isfile(path_crypto) == True:
        crypto = pd.read_csv(path_crypto)
        t = crypto.Date.tail(1)
        year = int(t.str[0:4])
        month = int(t.str[5:7])
        day = int(t.str[8:10]) 
        
    else:
        year = 2000
        month = 1
        day = 1
        
    last_datetime = dt.datetime(year, month, day) # year, month, day
    while True:
        print(last_datetime, i)
        new_df = get_binance_bars(i, timeframe, last_datetime, dt.datetime.now())
        if new_df is None:
            break
        df_list.append(new_df)
        last_datetime = max(new_df.index) + dt.timedelta(1, 0)
        df = pd.concat(df_list)
        df.reset_index(level=0, inplace=True)
        df.columns = ['Date', 'datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'adj_close']
        
    if os.path.isfile(path_crypto) == True:
        df_main = pd.read_csv(path_crypto)
        df_update = pd.concat([df_main, df], sort = False)
        df = df_update.drop_duplicates(subset = ["Date"])
        print("-------------Update----------")
        
    df.to_csv(path_crypto, index = False)
      