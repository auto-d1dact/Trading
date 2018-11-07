# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 10:05:54 2018

@author: Fang
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 16:01:12 2018

@author: Fang

functions list:
    
    historical_data(str[ticker], int[day_number], int[rolling_window], outsize[str]) --> DataFrame[daily_stock_data]
    
    current_volatility(list[ticker_lst], int[roll]) --> DataFrame[stock_volatilities]
"""

# Note to import from .py files, must follow structure
# from <.py filename excluding '.py'> import <class name>
# Optionslam creds: aspringfastlaner Options2018

import pandas as pd
import numpy as np
import os
import datetime as dt

main_dir = os.getcwd()

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\DataCollection')

from yahoo_query import *

#%%
# Function historical data from alpha advantage
def historical_data(ticker, start_date, day_number = 252, rolling_window = 20):
    yahoo_data = yahoo_query(ticker, start_date)
    yahoo_data.hist_prices_query()
    stockframe = yahoo_data.hist_prices
    stockframe['daily_ret'] = np.log(stockframe['{}_close'.format(ticker)]/stockframe['{}_close'.format(ticker)].shift(1))
    stockframe['intra_ret'] = np.log(stockframe['{}_close'.format(ticker)]/stockframe['{}_open'.format(ticker)])
    stockframe['ovrnt_ret'] = np.log(stockframe['{}_open'.format(ticker)]/stockframe['{}_close'.format(ticker)].shift(1))
    stockframe['daily_vol'] = stockframe.daily_ret.rolling(window=rolling_window,center=False).std()
    stockframe['intra_vol'] = stockframe.intra_ret.rolling(window=rolling_window,center=False).std()
    stockframe['ovrnt_vol'] = stockframe.ovrnt_ret.rolling(window=rolling_window,center=False).std()
    stockframe['daily_ann'] = stockframe.daily_vol*np.sqrt(252)
    stockframe['intra_ann'] = stockframe.intra_vol*np.sqrt((24/6.5)*252)
    stockframe['ovrnt_ann'] = stockframe.ovrnt_vol*np.sqrt((24/17.5)*252)
    stockframe['oc_diff'] = stockframe['{}_close'.format(ticker)] - stockframe['{}_open'.format(ticker)]
    stockframe['daily_dollar_vol'] = stockframe.daily_vol*stockframe['{}_close'.format(ticker)].shift(1)
    stockframe['daily_dollar_std'] = np.abs(stockframe.oc_diff/stockframe.daily_dollar_vol)

    return stockframe.tail(day_number).dropna()

# Function for building a dataframe of volatilities
## Daily, Intraday, Overnight
#def current_volatility(ticker_list, roll = 20):
#    
#    rows = []
#    failed_tickers = []
#    
#    def failed_check(failed_lst,rows):
#        if len(failed_lst) == 0:
#            return failed_lst, rows
#        else:
#            new_lst = []
#            new_rows = rows
#            for tick in failed_lst:
#                try: 
#                    curr_vol = historical_data(tick, outsize = 'compact').tail(1)[['daily_ann','intra_ann','ovrnt_ann','close',
#                                                                                   'daily_dollar_vol']]
#                    curr_vol.index.name = 'Tickers'
#                    curr_vol.index = [tick]
#                    new_rows.append(curr_vol)
#                except:
#                    new_lst.append(tick)
#            return failed_check(new_lst, rows)
#
#    for tick in ticker_list:
#        try: 
#            curr_vol = historical_data(tick, outsize = 'compact').tail(1)[['daily_ann','intra_ann','ovrnt_ann','close',
#                                                                           'daily_dollar_vol']]
#            curr_vol.index.name = 'Tickers'
#            curr_vol.index = [tick]
#            rows.append(curr_vol)
#        except:
#            failed_tickers.append(tick)
#            
#    failed_lst, rows = failed_check(failed_tickers, rows)
#        
#    return pd.concat(rows, axis = 0)
