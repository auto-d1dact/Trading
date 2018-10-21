# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 22:43:37 2018

@author: Fang
"""

# Factors to add
'''
Earnings Growth past 3 years
Earnings Growth past 3 quarters
Profit Margins past 3 years
Profit Margins past 3 quarters
Revenue Growth past 3 years
Revenue Growth past 3 quarters


Stock, sector, market performance past 52 weeks
Stock, sector, market performance past 26 weeks
Stock, sector, market performance past 12 weeks
Stock, sector, market performance past 4 weeks

Stock vs Sector correlation past 200, 60, 20 days
Stock vs Market correlation past 200, 60, 20 days

Stock volatility past 52, 26, 12, 4 weeks
Stock downside volatility past 52, 26, 12, 4 weeks

Market Cap
Price to Equity
Debt to Equity

Buy, Hold, Sell ratings

Current Price/52 Week High of stock

RSI last 4 weeks
RSI last 12 weeks

'''

# xlu Utilities 
# xlre Real Estate
# xly Consumer Cyclical
# XLV Health Care
# XLB Materials
# XLI Industrials
# xlf Financials
# XLK Technology
# XLC Communications
# XLP Consumer Defensive
# XLE Energy

import os
import pandas as pd
import datetime as dt
import numpy as np

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)
from yahoo_query import *

# Initializing Fin Statement Data
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Historical Queries\\US')

file_date = '2018-10-20'

fsa_columns = ['Underlying','totalCurrentLiabilities', 'totalStockholderEquity', 'totalLiab', 'totalAssets', 
              'grossProfit', 'totalRevenue', 'netIncome', 'earnings']
fsq_columns = ['Underlying','totalCurrentLiabilities', 'totalStockholderEquity', 'totalLiab', 'totalAssets', 
              'grossProfit', 'totalRevenue', 'netIncome']
ks_columns = ['sector','forwardPE','forwardEps','pegRatio','recommendationKey','shortPercentOfFloat',
              'shortRatio','sharesOutstanding','currentPrice']

us_annual = pd.read_csv('us_annual_{}.csv'.format(file_date), index_col = 0)[fsa_columns].sort_index(ascending = True)
us_keystats = pd.read_csv('us_keystats_{}.csv'.format(file_date), index_col = 0)[ks_columns]
us_keystats['mktCap'] = us_keystats['currentPrice']*us_keystats['sharesOutstanding']
us_quarterly = pd.read_csv('us_quarterly_{}.csv'.format(file_date), index_col = 0)[fsq_columns].sort_index(ascending = True)

#%%

def finratios(df):
    if 'earnings' in df.columns.tolist():
        fin = df[['Underlying','earnings','netIncome','totalRevenue']]
    else:
        fin = df[['Underlying','netIncome','totalRevenue']]
        
    fin['debtToEquity'] = np.array(df['totalCurrentLiabilities'])/np.array(df['totalStockholderEquity'])
    fin['liabilitiesToAssets'] = np.array(df['totalLiab'])/np.array(df['totalAssets'])
    fin['gross_margin'] = np.array(df['grossProfit'])/np.array(df['totalRevenue'])
    fin['profit_margin'] = np.array(df['netIncome'])/np.array(df['totalRevenue'])
    
    return fin

annual = finratios(us_annual)
quarterly = finratios(us_quarterly)

# test.earnings_quarterly


#%%

for ticker in df.Underlying.drop_duplicates():
    curr_data = df[df.Underlying == ticker].sort_index()
    curr_len = len(curr_data) - 1
    if curr_len > 1:
        try:
            if sum(curr_data.earnings.pct_change() >= 0.05) == curr_len:
                leap_scores.loc[ticker, 'earningsGrowth'] = 1
        except:
            if sum(curr_data.epsActual.pct_change() > 0) == curr_len:
                leap_scores.loc[ticker, 'earningsGrowth'] = 1
        if sum(curr_data.profitMargin.tail(3) >= 0.1) == curr_len:
            leap_scores.loc[ticker, 'profitMargins'] = 1
        if sum(curr_data.profitMargin.pct_change() >= 0) == curr_len:
            leap_scores.loc[ticker, 'profitMarginChange'] = 1
        if sum(curr_data.roe.tail(3) >= 0.1) == curr_len:
            leap_scores.loc[ticker, 'roe'] = 1
        if sum(curr_data.roe.pct_change() >= 0) == curr_len:
            leap_scores.loc[ticker, 'roeChange'] = 1

