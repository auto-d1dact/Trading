# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 13:28:41 2018

@author: Fang
"""

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

## Solvency Ratios

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


#%%
annual_ratios = ratio_calc(annual_reports)
quarterly_ratios = ratio_calc(quarterly_reports)


#%%
fin_statements = ratio_changes(quarterly_ratios)
sectors = key_stats[['sector']]
sectors['Underlying'] = sectors.index 

fin_statements = pd.merge(fin_statements, sectors, on = 'Underlying')

#%%
earnings_rets = []

for ticker in fin_statements['Underlying'].drop_duplicates().tolist():
    try:
        curr_earnings = earnings_report(ticker)
        curr_earnings.columns = ['CallTime','PostEarningsReturn', 'IndustryBeta',
                                 'MarketBeta','Stock52WeekReturn','SPY52WeekReturn',
                                 'Industry52WeekReturn']
        curr_earnings['Underlying'] = ticker
        earnings_rets.append(curr_earnings)
    except:
        continue
    print("completed: {} %".format('%.2f' % (len(earnings_rets)/len(fin_statements['Underlying'].drop_duplicates().tolist()))))
    
earnings_rets = pd.concat(earnings_rets, axis = 0)

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Historical Queries\\Stock Prices')
earnings_rets.to_csv('earnings_returns_{}.csv'.format(file_date))

earnings_rets['EarningsDate'] = pd.to_datetime(earnings_rets.index)

#%%
#earnings_rets = pd.read_csv('returns_cleaned.csv', index_col = 0)
#earnings_rets['Earnings_Date'] = pd.to_datetime(earnings_rets['Earnings_Date'])

for idx, row in earnings_rets.iterrows():
    
    curr_date = idx.date()
    
    if curr_date.month <= 3:
        qend = dt.date(curr_date.year, 3, 31)
        
    elif curr_date.month <= 6:
        qend = dt.date(curr_date.year, 6, 30)
        
    elif curr_date.month <= 9:
        qend = dt.date(curr_date.year, 9, 30)
    
    else:
        qend= dt.date(curr_date.year, 12, 31)
        
    earnings_rets.loc[idx, 'index'] = qend

earnings_rets['index'] = pd.to_datetime(earnings_rets['index'])

#%% 
for idx, row in fin_statements.iterrows():
    
    curr_date = row['index'].date()
    
    if curr_date.month <= 3:
        qend = dt.date(curr_date.year, 3, 31)
        
    elif curr_date.month <= 6:
        qend = dt.date(curr_date.year, 6, 30)
        
    elif curr_date.month <= 9:
        qend = dt.date(curr_date.year, 9, 30)
    
    else:
        qend= dt.date(curr_date.year, 12, 31)
        
    fin_statements.loc[idx, 'index'] = qend
    

#%%
quarter_ends = []
keys = []

for quarter in fin_statements['index'].drop_duplicates().sort_index():
    
    curr_dt = quarter.date()
    if curr_dt.month == 3:
        months = ['{0}{1}'.format(x, curr_dt.year) for x in range(1, 4)]
            
#     elif curr_dt.month == 1:
#         months = ['{0}{1}'.format(x, curr_dt.year) for x in [11, 12, 1]]
#     elif curr_dt.month == 2:
#         months = ['{0}{1}'.format(x, curr_dt.year) for x in [12, 1, 2]]
    else:
        months = ['{0}{1}'.format(x, curr_dt.year) for x in range(curr_dt.month - 2, curr_dt.month + 1)]
    
    for i in months:
        quarter_ends.append(curr_dt)
        keys.append(i)
        
q_keys = dict(zip(keys,quarter_ends))
    

cleaned_data = pd.merge(fin_statements, earnings_rets,  how='left', 
                        left_on=['index','Underlying'], right_on = ['index','Underlying'])

cleaned_data = cleaned_data.replace([np.inf, -np.inf], np.nan)

cleaned_data = cleaned_data[(cleaned_data.PostEarningsReturn.isnull() == False)].dropna().reset_index(drop = True)

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Historical Queries\\Stock Prices')
file_date = '2018-10-14'
cleaned_data.to_csv('earnings_input_data-{}.csv'.format(file_date))

os.chdir(main_dir)