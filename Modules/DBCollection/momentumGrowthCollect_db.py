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

us_annual = pd.read_csv('us_annual_{}.csv'.format(file_date), index_col = 0)[fsa_columns]
us_annual.index = pd.to_datetime(us_annual.index)
us_annual = us_annual.sort_index(ascending = True)

us_keystats = pd.read_csv('us_keystats_{}.csv'.format(file_date), index_col = 0)[ks_columns].drop_duplicates()
us_keystats['mktCap'] = us_keystats['currentPrice']*us_keystats['sharesOutstanding']

us_quarterly = pd.read_csv('us_quarterly_{}.csv'.format(file_date), index_col = 0)[fsq_columns]
us_quarterly.index = pd.to_datetime(us_quarterly.index)
us_quarterly = us_quarterly.sort_index(ascending = True)

ks_columns = ['sector','forwardPE','forwardEps','pegRatio','recommendationKey','shortPercentOfFloat',
              'shortRatio','mktCap']
#%%

def finratios(df):
    if 'earnings' in df.columns.tolist():
        fin = df[['Underlying','netIncome','totalRevenue']]
    else:
        fin = df[['Underlying','netIncome','totalRevenue']]
        
    fin['debtToEquity'] = np.array(df['totalCurrentLiabilities'])/np.array(df['totalStockholderEquity'])
    fin['liabilitiesToAssets'] = np.array(df['totalLiab'])/np.array(df['totalAssets'])
    fin['gross_margin'] = np.array(df['grossProfit'])/np.array(df['totalRevenue'])
    fin['profit_margin'] = np.array(df['netIncome'])/np.array(df['totalRevenue'])
    
    return fin

annual = finratios(us_annual)
quarterly = finratios(us_quarterly)

#%%
def finstatement_factors(ticker):
    ######## Fin Statement Factors ################
    curr_annual = annual[annual['Underlying'] == ticker].sort_index(ascending = True).drop_duplicates()
    curr_quarterly = quarterly[quarterly['Underlying'] == ticker].sort_index(ascending = True).drop_duplicates()
    
    curr_annual['profitGrowth'] = curr_annual['netIncome'].pct_change()
    curr_annual['revGrowth'] = curr_annual['totalRevenue'].pct_change()
    curr_annual['debtEquityChange'] = curr_annual['debtToEquity'].pct_change()
    curr_annual['liabToAssetsChange'] = curr_annual['liabilitiesToAssets'].pct_change()
    curr_annual['grossMarginChange'] = curr_annual['gross_margin'].pct_change()
    curr_annual['profitMarginChange'] = curr_annual['profit_margin'].pct_change()
    
    del curr_annual['netIncome'], curr_annual['totalRevenue']
    
    curr_quarterly['profitGrowth'] = curr_quarterly['netIncome'].pct_change()
    curr_quarterly['revGrowth'] = curr_quarterly['totalRevenue'].pct_change()
    curr_quarterly['debtEquityChange'] = curr_quarterly['debtToEquity'].pct_change()
    curr_quarterly['liabToAssetsChange'] = curr_quarterly['liabilitiesToAssets'].pct_change()
    curr_quarterly['grossMarginChange'] = curr_quarterly['gross_margin'].pct_change()
    curr_quarterly['profitMarginChange'] = curr_quarterly['profit_margin'].pct_change()
    
    del curr_quarterly['netIncome'], curr_quarterly['totalRevenue']
    
    factors_annual = []
    
    for col in curr_annual.columns[1:]:
        annual_data = curr_annual[[col]].dropna().T
        
        if 'Change' in col:
            shift = 2
        else:
            shift = 1
            
        annual_data.columns = ['{0} Year {1}'.format(col,i + shift) for i,x in enumerate(annual_data.columns.tolist())]
        annual_data.index = [ticker]
        factors_annual.append(annual_data)
        
    
    factors_quarterly = []
    
    for col in curr_quarterly.columns[1:]:
        quarterly_data = curr_quarterly[[col]].dropna().T
        
        if 'Change' in col:
            shift = 2
        else:
            shift = 1
            
        quarterly_data.columns = ['{0} Quarter {1}'.format(col,i + shift) for i,x in enumerate(quarterly_data.columns.tolist())]
        quarterly_data.index = [ticker]
        factors_quarterly.append(quarterly_data)
        
    
    factors_annual = pd.concat(factors_annual, axis = 1)
    factors_quarterly = pd.concat(factors_quarterly, axis = 1)
    curr_keystats = us_keystats[us_keystats.index == ticker][ks_columns]
    
    factors = pd.concat([curr_keystats, factors_annual,factors_quarterly], axis = 1)
    
    return factors

#%%

######## Return Factors ################

start_date = dt.datetime(dt.datetime.now().year - 2,1,1)

spdr_lst = ['SPY','XLU','XLRE','XLY','XLV',
            'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
            'XLP', 'XLE']
    
sector_lst = []
for indticker in spdr_lst:
    curr_etf = yahoo_query(indticker, start_date)
    curr_etf.hist_prices_query()
    sector_lst.append(curr_etf.hist_prices[['{}_close'.format(indticker)]])
    
sector_prices = pd.concat(sector_lst, axis = 1)
sector_prices.columns = ['SPY','Utilities','Real Estate','Consumer Cyclical',
                         'Healthcare', 'Basic Materials', 'Industrials',
                         'Financial Services', 'Technology', 'Communication Services',
                         'Consumer Defensive', 'Energy']


#%%

def stockprice_factors(ticker):
    
    stock_prices = yahoo_query(ticker, start_date)
    stock_prices.hist_prices_query()
    stock_prices = stock_prices.hist_prices[['{}_close'.format(ticker)]]
    stock_prices = stock_prices.join(sector_prices[us_keystats.loc[ticker,'sector']]).join(sector_prices['SPY'])
    
    return_factors = stock_prices.pct_change(252).join(stock_prices.pct_change(126), 
                                                       rsuffix = '_b').join(stock_prices.pct_change(60), 
                                                                            rsuffix = '_q').join(stock_prices.pct_change(20),
                                                                                                 rsuffix = '_m')
    return_factors.columns = ['52WeekStockReturn','52WeekIndustryReturn','52WeekMarketReturn',
                              '26WeekStockReturn','26WeekIndustryReturn','26WeekMarketReturn',
                              '12WeekStockReturn','12WeekIndustryReturn','12WeekMarketReturn',
                              '4WeekStockReturn','4WeekIndustryReturn','4WeekMarketReturn']
    
    
    
    daily_returns = stock_prices.pct_change()
    daily_returns.columns = ['stock','industry','market']
    
    corr_factors = daily_returns[['stock']].rolling(200).cov(daily_returns.industry)/daily_returns[['industry']].rolling(200).var()
    corr_factors.columns = ['industryBeta200day']
    
    corr_factors['marketBeta200day'] = daily_returns[['stock']].rolling(200).cov(daily_returns.market)/daily_returns[['market']].rolling(200).var()
    
    corr_factors['industryBeta60day'] = daily_returns[['stock']].rolling(60).cov(daily_returns.industry)/daily_returns[['industry']].rolling(200).var()
    corr_factors['marketBeta60day'] = daily_returns[['stock']].rolling(60).cov(daily_returns.market)/daily_returns[['market']].rolling(200).var()
    
    corr_factors['industryBeta20day'] = daily_returns[['stock']].rolling(20).cov(daily_returns.industry)/daily_returns[['industry']].rolling(200).var()
    corr_factors['marketBeta20day'] = daily_returns[['stock']].rolling(20).cov(daily_returns.market)/daily_returns[['market']].rolling(200).var()
    
    
    vol_factors = daily_returns[['stock']].rolling(252).std()*np.sqrt(252)
    vol_factors.columns = ['52WeekVol']
    vol_factors['26WeekVol'] = daily_returns[['stock']].rolling(126).std()*np.sqrt(252)
    vol_factors['12WeekVol'] = daily_returns[['stock']].rolling(60).std()*np.sqrt(252)
    vol_factors['4WeekVol'] = daily_returns[['stock']].rolling(20).std()*np.sqrt(252)
    
    
    price_factors = return_factors.join(corr_factors).join(vol_factors).tail(1)
    
    price_factors['52WeekDownsideVol'] = daily_returns.tail(252)[daily_returns.stock < 0].stock.std()*np.sqrt(252)
    price_factors['26WeekDownsideVol'] = daily_returns.tail(126)[daily_returns.stock < 0].stock.std()*np.sqrt(252)
    price_factors['12WeekDownsideVol'] = daily_returns.tail(60)[daily_returns.stock < 0].stock.std()*np.sqrt(252)
    price_factors['4WeekDownsideVol'] = daily_returns.tail(20)[daily_returns.stock < 0].stock.std()*np.sqrt(252)
    
    price_factors['PriceTo52WeekHigh'] = stock_prices['{}_close'.format(ticker)].tail(1).values[0]/max(stock_prices['{}_close'.format(ticker)].tail(252))
    
    rsi = stock_prices[['{}_close'.format(ticker)]] - stock_prices[['{}_close'.format(ticker)]].shift(1)
    rsi.columns = ['stock']
    rsi['gain'] = np.round(rsi.stock,4)*(rsi.stock >= 0)
    rsi['loss'] = np.round(rsi.stock,4)*(rsi.stock < 0)
    rsi = rsi.replace(0, np.nan)
    rsi['avg12weekgain'] = rsi['gain'].rolling(60, min_periods=1).apply(np.nanmean)
    rsi['avg4weekgain'] = rsi['gain'].rolling(20, min_periods=1).apply(np.nanmean)
    
    rsi['avg12weekloss'] = rsi['loss'].rolling(60, min_periods=1).apply(np.nanmean)
    rsi['avg4weekloss'] = rsi['loss'].rolling(20, min_periods=1).apply(np.nanmean)
    
    rsi['rs12Week'] = rsi['avg12weekgain']/rsi['avg12weekloss']
    rsi['rs4Week'] = rsi['avg4weekgain']/rsi['avg4weekloss']
    
    rsi['rsi12Week'] = 100 - (100/(1 + rsi['rs12Week']))
    rsi['rsi4Week'] = 100 - (100/(1 + rsi['rs4Week']))
    
    rsi = rsi[['rsi12Week','rsi4Week']].tail(1)
    price_factors = price_factors.join(rsi)
    price_factors.index = [ticker]
    
    return price_factors


#%%

if __name__ == '__main__':
    
    fs_factors = []
    
    for ticker in annual.Underlying.drop_duplicates():
        fs_factors.append(finstatement_factors(ticker))
        
    fs_factors = pd.concat(fs_factors, axis = 0)
    
    price_factors = []
    
    i = 0
    total_length = len(annual.Underlying.drop_duplicates())
    for ticker in annual.Underlying.drop_duplicates():
        try:
            price_factors.append(stockprice_factors(ticker))
        except:
            continue
        i += 1
        print('{0:.2f}% Completed'.format(i/total_length*100))
        
    price_factors = pd.concat(price_factors, axis = 0)
    
    #%%
    factors_df = fs_factors.join(price_factors)
    
    
    os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Historical Queries\\Momentum Growth')
    
    datenow = dt.datetime.today().strftime('%Y-%m-%d')
    
    factors_df.to_csv('momentum_growth-{}.csv'.format(datenow))
    
    os.chdir(main_dir)
