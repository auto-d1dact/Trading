# -*- coding: utf-8 -*-
"""
Created on Mon Oct 22 11:38:24 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
import numpy as np

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)
from yahoo_query import *

# Initializing Data
file_date = '2018-11-24'

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Historical Queries\\Momentum Growth')
    
factors_df = pd.read_csv('momentum_growth-{}.csv'.format(file_date), index_col = 0)

os.chdir(main_dir)

#%%

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Single Name Pulls')

filtered = factors_df[(factors_df['profit_margin Year 1'] > 0) &
                      (factors_df['profit_margin Year 2'] > 0) &
                      (factors_df['profit_margin Year 3'] > 0) &
                      (factors_df['profit_margin Year 4'] > 0) &
                      (factors_df['revGrowth Year 1'] > 0) &
                      (factors_df['revGrowth Year 2'] > 0) &
                      (factors_df['revGrowth Year 3'] > 0) &
                      (factors_df['profitGrowth Year 1'] > 0) &
                      (factors_df['profitGrowth Year 2'] > 0) &
                      (factors_df['profitGrowth Year 3'] > 0) &
                      (factors_df['profitMarginChange Year 2'] > 0) &
                      (factors_df['profitMarginChange Year 3'] > 0) &
                      (factors_df['profitMarginChange Year 4'] > 0) &
                      (factors_df['debtEquityChange Year 2'] < 0) &
                      (factors_df['debtEquityChange Year 3'] < 0) &
                      (factors_df['debtEquityChange Year 4'] < 0) &
                      (factors_df['debtToEquity Year 1'] < 1) &
                      (factors_df['debtToEquity Year 2'] < 1) &
                      (factors_df['debtToEquity Year 3'] < 1) &
                      (factors_df['debtToEquity Year 4'] < 1) &
                      (factors_df['26WeekStockReturn'] > 0) &
                      (factors_df['12WeekStockReturn'] > 0) &
                      (factors_df['4WeekStockReturn'] > 0)]
#                      (factors_df['mktCap'] > 1000000) &
#                      (factors_df['mktCap'] < 10000000000)]

#filtered['sortino52Week'] =  filtered['52WeekStockReturn']/filtered['52WeekDownsideVol']
#filtered['sortino26Week'] =  filtered['26WeekStockReturn']/filtered['26WeekDownsideVol']
#filtered['sortino12Week'] =  filtered['12WeekStockReturn']/filtered['12WeekDownsideVol']
#filtered['sortino4Week'] =  filtered['4WeekStockReturn']/filtered['4WeekDownsideVol']
#
#filtered['sharpe52Week'] =  filtered['52WeekStockReturn']/filtered['52WeekVol']
#filtered['sharpe26Week'] =  filtered['26WeekStockReturn']/filtered['26WeekVol']
#filtered['sharpe12Week'] =  filtered['12WeekStockReturn']/filtered['12WeekVol']
#filtered['sharpe4Week'] =  filtered['4WeekStockReturn']/filtered['4WeekVol']
#
filtered = filtered[(filtered['PriceTo52WeekHigh'] > 0.9)]

#%%

datenow = dt.datetime.today().strftime('%Y-%m-%d')

filtered.to_csv('momentum_growth_picks-{}.csv'.format(datenow))

os.chdir(main_dir)