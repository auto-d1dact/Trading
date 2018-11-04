# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 22:06:45 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
import requests
import numpy as np

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

cad_names = pd.read_csv('cad_names.csv')['Symbol'].tolist()
us_names = pd.read_csv('us_names.csv')['Symbol'].tolist()

os.chdir(main_dir)

from yahoo_query import *

#%%

def past_earnings_dates(ticker):
    
    site = 'https://www.optionslam.com/earnings/stocks/' + ticker
    soup = bs(requests.get(site).text, "lxml")
    table = soup.find('table', {'cellpadding': '2', 'cellspacing': '2', 'border': '0'})

    earnings_dates = []
    earnings_times = []

    for row in table.find_all('tr'):
        try:
            earnings_row = str(row.find('td', {'nowrap': ""})).split('>')
            if 'nowrap' in earnings_row[0]:
                date = earnings_row[1].strip().splitlines()[0]
                try:
                    dt.datetime.strptime(date.strip(), '%b. %d, %Y')
                    earnings_dates.append(date)
                    earnings_times.append(earnings_row[2].strip().splitlines()[0])
                except:
                    dt.datetime.strptime(date.strip(), '%B %d, %Y')
                    earnings_dates.append(date)
                    earnings_times.append(earnings_row[2].strip().splitlines()[0])
        except:
            continue
    
    out_df = pd.DataFrame({ticker: earnings_times}, index = earnings_dates)
    out_df.index = pd.to_datetime(out_df.index)
    return out_df.sort_index(ascending = True)


def earnings_report(ticker, corr_window = 60):
    past_earnings = past_earnings_dates(ticker)

    start_date = past_earnings.index[0].date() - dt.timedelta(days = 370)
    start_date = dt.datetime(start_date.year, start_date.month, start_date.day)
    yahoo_info = yahoo_query(ticker, start_date)
    yahoo_info.hist_prices_query()
    yahoo_info.full_info_query()
    #yahoo_info.minute_query()

    spdr_lst = ['SPY','XLU','XLRE','XLY','XLV',
                'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
                'XLP', 'XLE']

    sector_lst = ['SPY','Utilities','Real Estate','Consumer Cyclical',
                  'Healthcare', 'Basic Materials', 'Industrials',
                  'Financial Services', 'Technology', 'Communication Services',
                  'Consumer Defensive', 'Energy']

    sector_dict = dict(zip(sector_lst, spdr_lst))

    stock_sector = yahoo_info.profile.loc[ticker, 'sector']

    daily_prices = []
    #minute_prices = []

    for etf in ['SPY', sector_dict[stock_sector]]:
        curr_etf = yahoo_query(etf, start_date)
        curr_etf.hist_prices_query()
        #curr_etf.minute_query()
        daily_prices.append(curr_etf.hist_prices[['{}_close'.format(etf)]])
        #minute_prices.append(curr_etf.minute_prices[['{}_close'.format(etf)]])

    daily_prices = pd.concat([yahoo_info.hist_prices[['{}_close'.format(ticker)]]] + daily_prices, axis = 1)
    #minute_prices = pd.concat([yahoo_info.minute_prices[['{}_close'.format(ticker)]]] + minute_prices, axis = 1)
    
    ret52Week = daily_prices.pct_change(252)
    ret52Week.columns = ['{}_52WeekReturn'.format(ticker),'SPY_52WeekReturn','{}_52WeekReturn'.format(sector_dict[stock_sector])]
    dailyRet = daily_prices.pct_change()
    dailyRet['{} Beta'.format(stock_sector)] = dailyRet['{}_close'.format(ticker)].rolling(corr_window).cov(dailyRet['{}_close'.format(sector_dict[stock_sector])])/dailyRet['{}_close'.format(sector_dict[stock_sector])].rolling(corr_window).var()
    dailyRet['MarketBeta'] = dailyRet['{}_close'.format(ticker)].rolling(corr_window).cov(dailyRet['SPY_close'])/dailyRet['SPY_close'].rolling(corr_window).var()
    dailyRet = dailyRet.dropna()

    del dailyRet['{}_close'.format(ticker)], dailyRet['SPY_close'], dailyRet['{}_close'.format(sector_dict[stock_sector])]
    dailyRet = dailyRet.join(ret52Week).dropna()

    closeToOpenRet = yahoo_info.hist_prices[['{}_close'.format(ticker),'{}_open'.format(ticker)]]
    closeToOpenRet['{}_closeToOpen'.format(ticker)] = closeToOpenRet['{}_open'.format(ticker)].shift(-1)/closeToOpenRet['{}_close'.format(ticker)] - 1
    del closeToOpenRet['{}_close'.format(ticker)], closeToOpenRet['{}_open'.format(ticker)]
    
    past_earnings['{}_closeToOpen'.format(ticker)] = np.nan

    for idx, row in past_earnings.iterrows():
        if row[ticker] == 'AC':
            try:
                past_earnings.loc[idx, '{}_closeToOpen'.format(ticker)] = closeToOpenRet[closeToOpenRet.index == idx].values[0]
            except:
                continue
        else:
            try:
                past_earnings.loc[idx, '{}_closeToOpen'.format(ticker)] = closeToOpenRet[closeToOpenRet.index < idx].tail(1).values[0]
            except:
                continue
                
    #final_ret = dailyRet.tail(1)
    #final_ret.index = pd.DatetimeIndex([past_earnings.index[-1].date()])
    
    out_df = past_earnings.join(dailyRet)#.shift(1))
    out_df.columns = ['CallTime','PostEarningsReturn','SectorBeta','MarketBeta','Stock52WeekReturn',
                      'SPY52WeekReturn','Sector52WeekReturn']
    out_df['Underlying'] = ticker
    return out_df