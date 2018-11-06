# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 23:56:29 2018

@author: Fang
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import datetime as dt

import time


import os
main_dir = os.getcwd()

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\DataCollection')

from yahoo_query import *
from option_slam_earnings import *
from reuters_query import reuters_query


os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\Earnings')
from yahoo_earnings import *

os.chdir(main_dir)

#%%

focus_names = ['ADVM', 'OSTK', 'ETSY']

start_date = dt.datetime(2018,1,1)

start_time = time.time()


for ticker in focus_names:
    
    # Reuters Data
    curr_reuters = reuters_query(ticker)
    
    # Earnings Historical Results
    curr_earnings_report = earnings_report(ticker)
    curr_earnings_report.columns = [x.replace(ticker,'Stock') for x in curr_earnings_report.columns[:-1]] + ['52WeekSectorReturn']
    
    # Yahoo Query
    curr_yahoo = yahoo_query(ticker, start_date)
    curr_yahoo.full_info_query()
    
    break

print("--- %s seconds ---" % (time.time() - start_time))