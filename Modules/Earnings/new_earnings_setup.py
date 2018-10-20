# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 13:14:08 2018

@author: Fang

Module for collecting new earnings data for feeding into earnings_prediction module

"""

import os
import pandas as pd
import datetime as dt
import numpy as np

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\Earnings'
os.chdir(main_dir)
from yahoo_query import *

# Initializing Fin Statement Data
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Historical Queries\\US')

file_date = '2018-10-14'

annual_reports = pd.read_csv('us_annual_{}.csv'.format(file_date), index_col = 0)
annual_reports['index'] = pd.to_datetime(annual_reports.index)

quarterly_reports = pd.read_csv('us_quarterly_{}.csv'.format(file_date), index_col = 0)
quarterly_reports['index'] = pd.to_datetime(quarterly_reports.index)

key_stats = pd.read_csv('us_keystats_{}.csv'.format(file_date), index_col = 0)

os.chdir('..\\Stock Prices')

file_date = '2018-10-11'

stock_data = pd.read_csv('us_closes_{}.csv'.format(file_date), index_col = 0)
stock_data.index = pd.to_datetime(stock_data.index)

earnings_dates = pd.read_csv('os_earnings_dates_{}.csv'.format(file_date), index_col = 0)
earnings_dates['Earnings Dates'] = pd.to_datetime(earnings_dates['Earnings Dates'])

os.chdir(main_dir)

from option_slam_earnings import *

#%%