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

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\Earnings')

# from finstatement_cleaning import ratio_calc

def ratio_calc(in_df):
    df = in_df[['index','Underlying']]
    df['current_ratio'] = in_df['totalCurrentAssets']/in_df['totalCurrentLiabilities']
    df['total_debt_equity_ratio'] = in_df['totalCurrentLiabilities']/in_df['totalStockholderEquity']

    ## Liquidity Ratios
    df['day_payable_outstanding'] = 365*(in_df['accountsPayable']/in_df['costOfRevenue'])

    ## Capital Structure Ratios
    df['total_liabilities_total_assets'] = in_df['totalLiab']/in_df['totalAssets']

    ## Income Statement Ratios
    df['gross_margin'] = in_df['grossProfit']/in_df['totalRevenue']
    df['operating_margin'] = in_df['operatingIncome']/in_df['totalRevenue']
    df['net_profit_margin'] = in_df['netIncome']/in_df['totalRevenue']
    
    ## Cash Flow Ratios
    df['changeInCash'] = in_df['changeInCash']/in_df['cash']
    df['changeToLiabilities'] = in_df['changeToLiabilities']/in_df['totalLiab']
    df['changeToNetincome'] = in_df['changeToNetincome']/in_df['netIncome']
    df['changeToOperatingActivities'] = in_df['changeToOperatingActivities']/in_df['totalCashFromOperatingActivities']
    
    return df

def ratio_changes(df):
    changes = []
    for ticker in df.Underlying.drop_duplicates().tolist():
        curr_report = df[df.Underlying == ticker].sort_values('index')
        copy_report = curr_report[['index','Underlying']]
        del curr_report['index'], curr_report['Underlying']
        copy_report = copy_report.join(curr_report.pct_change().shift(1).dropna(how = 'all'), how = 'inner')
        copy_report['year_join'] = copy_report['index'].dt.year - 1


        curr_annual = annual_ratios[annual_ratios['Underlying'] == ticker].sort_values('index')
        curr_annual['year_join'] = curr_annual['index'].dt.year
        del curr_annual['index'], curr_annual['Underlying']
        curr_annual = curr_annual.join(curr_annual.pct_change(), rsuffix = '_change')
        del curr_annual['year_join_change']
        copy_report = copy_report.set_index('year_join').join(curr_annual.set_index('year_join'), lsuffix = '_quarterly', rsuffix = '_annual')

        copy_report.index = pd.to_datetime(copy_report['index'])

        changes.append(copy_report)


    return pd.concat(changes,axis = 0)

os.chdir(main_dir)

from fundamental_data_collect import download_yahoo_data

latest_earnings_names = earnings_df[earnings_df['Call Time'] == 'Before Market Open'].index.tolist()

earnings_df, annual_df, keyStats_df, failed_list = download_yahoo_data(latest_earnings_names, retries = 3)

annual_df['index'] = pd.to_datetime(annual_df.index)
latest_annual_ratios = ratio_calc(annual_df)

earnings_df['index'] = pd.to_datetime(earnings_df.index)
latest_quarter_ratios = ratio_calc(earnings_df)

