# -*- coding: utf-8 -*-
"""
Created on Sun Jul  3 09:08:27 2022

@author: Pastor
"""

import pandas as pd
import os
import glob
import numpy as np

# use glob to get all the csv files 
# in the folder
path = "D:/Dropbox/Pastor/data/binance_data_1d"
csv_files = glob.glob(os.path.join(path, "*.csv"))
  

# loop over the list of csv files
list_files = []
list_names = []
for f in csv_files:
      
    # read the csv file
    df = pd.read_csv(f)
    if len(df) > 100:
        df["name"] = f[39:-4]
        # calculate momentum in which the price is up at least 12 out of 15 days
        df["ret"] = df["Close"].pct_change()
        df["mon"] = np.where(df["ret"] > 0, 1, 0)
        df["momentum_total"] = df["mon"].rolling(15).sum()
        df["momentum"] = np.where(df["momentum_total"]>11,1,0)
        
        # price: the price is up at least 20% over the past 15 days
        df["runmeanPrice"] = df["Close"].rolling(15).mean()
        df["price_mon"] = (df["Close"] - df["runmeanPrice"])/df["runmeanPrice"]*100
        df["pprice"] = np.where(df["price_mon"] > 20, 1, 0)
        
        # the volume has increase over the past 15 days by 20%
        df["runmeanVol"] = df["Volume"].rolling(15).mean()
        df["runmeanVol50"] = df["Volume"].rolling(50).mean()
        df["volume_mon"] = (df["runmeanVol"] - df["runmeanVol50"])/df["runmeanVol50"]*100
        df["vol"]= np.where(df["volume_mon"] > 20,1,0)
        
        # ant indicator
        df["gray"] = np.where(df["momentum"] ==1,1,0)
        df["blue"] = np.where((df["momentum"] == 1) & (df["pprice"] == 1), 1,0)
        df["yellow"] = np.where((df["momentum"]==1) & (df["vol"]==1),1,0)
        df["green"] = np.where((df["momentum"]==1) & (df["pprice"]==1) & (df["vol"]==1),1,0)
        
        df = df.tail(1)               
        list_files.append(df)
        
        
      
    # print the location and filename
    print('Location:', f)
    print('File Name:', f.split("\\")[-1])
          
merge = pd.concat(list_files)
merge.sort_values(by="Date", inplace=True)

# get the last day available to remove unlisted coins
last_day = merge["Date"].iloc[-1]
merge = merge[merge["Date"] == last_day]

merge.set_index("name", inplace=True)

merge.sort_values(by=["momentum_total", "volume_mon", "price_mon"], ascending=False, inplace=True)

merge.to_csv("C:/Users/Pastor/Desktop/ant_strategy/ant_indicator.csv")




    