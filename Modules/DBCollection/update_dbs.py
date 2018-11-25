# -*- coding: utf-8 -*-
"""
Created on Sat Nov 24 17:11:45 2018

@author: Fang
"""

import pandas as pd
import numpy as np
from sqlalchemy import *
from sqlalchemy import create_engine
import os
import datetime as dt

import time

#%%
module_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(module_dir)

from reuters_query import reuters_query
from option_slam_earnings import earnings_report
from alphaquery import alphaquery
from yahoo_query import yahoo_query

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

tsx = pd.read_csv('TSX.csv')['Symbol'].tolist()
nyse = pd.read_csv('NYSE.csv')['Symbol'].tolist()
nasdaq = pd.read_csv('NASDAQ.csv')['Symbol'].tolist()
amex = pd.read_csv('AMEX.csv')['Symbol'].tolist()

us_names = nyse + nasdaq + amex

os.chdir('..\\DBs')

#%%

start_time = time.time()

stock_queries = {}

for ticker in ['NVDA']:#['AAPL','AMD','GME','WDAY']:
    
    try:
        curr_reuters = reuters_query(ticker)
        
        curr_yahoo = yahoo_query(ticker, start_date = dt.datetime(2016,1,1))
        curr_yahoo.full_info_query()
        
        curr_alphaquery = alphaquery(ticker)
        curr_alphaquery = curr_alphaquery.vol_df
        curr_alphaquery['PullDate'] = dt.datetime.now().date()
        
        curr_earnings_history = earnings_report(ticker).dropna()
        curr_earnings_history.columns = ['earningsTime','closeToOpenReturn','industryBeta','marketBeta',
                                         'stock52WeekReturn','market52WeekReturn','industry52WeekReturn']
        curr_earnings_history['Underlying'] = ticker
        
        if len(curr_earnings_history['Underlying']) != len(curr_earnings_history['Underlying'].drop_duplicates()):
            curr_earnings_history = curr_earnings_history.head(len(curr_earnings_history) - 1)
        
        stock_queries[ticker] = [curr_reuters, curr_yahoo, curr_alphaquery, curr_earnings_history]
    except:
        print("Failed on {}".format(ticker))

print("--- %s seconds ---" % (time.time() - start_time))

#%%
aq_engine = create_engine('sqlite:///alphaquery.db', echo=False)
earningHist_engine = create_engine('sqlite:///earningsHistory.db', echo=False)
yahoo_engine = create_engine('sqlite:///yahoo.db', echo=False)
reuters_engine = create_engine('sqlite:///reuters.db', echo=False)

#vixcentral_data = pd.read_sql_query('SELECT Date, F1, F2, F3, F4, F5, F6, F7, F8, F9 FROM vixCentral', 
#                                            con=engine, index_col = 'Date')

for ticker, queries in stock_queries.items():
    queries[3].to_sql('postEarningsReturns', con=earningHist_engine, if_exists='append', index_label = 'earningsDate')
    queries[2].to_sql('aq_vols', con=aq_engine, if_exists='append')
    

#%%
test_earn = pd.read_sql_query('SELECT * FROM postEarningsReturns', con=earningHist_engine, index_col = 'earningsDate')
test_earn.index = pd.to_datetime(test_earn.index)


new_earnings = pd.concat([test_earn[['Underlying']], curr_earnings_history[['Underlying']]]).reset_index().drop_duplicates().set_index('index')

new_earnings.join(curr_earnings_history)
#%%


#%%

curr_yahoo.earnings_annual

curr_yahoo.earnings_quarterly
curr_yahoo.profile
curr_yahoo.cashFlowStatementAnnual
curr_yahoo.cashFlowStatementQuarter
#curr_yahoo.institutionOwners
curr_yahoo.majorHolderInfo
curr_yahoo.recommendationTrend
curr_yahoo.keyStats
curr_yahoo.purchaseActivity
curr_yahoo.insiderTxns
curr_yahoo.incomeStatementAnnual
curr_yahoo.incomeStatementQuarter
curr_yahoo.balanceSheetAnnual
curr_yahoo.balanceSheetQuarter
#curr_yahoo.fundOwnership
curr_yahoo.insiderHolders
#curr_yahoo.currEarnings
#curr_yahoo.dividends
curr_yahoo.finData


#%%

curr_reuters.overall_df
curr_reuters.revenues_eps_df
curr_reuters.sales_ests
curr_reuters.earnings_ests
curr_reuters.LTgrowth_ests
curr_reuters.valuations
curr_reuters.dividends
curr_reuters.growthrate
curr_reuters.finstrength
curr_reuters.profitability
curr_reuters.efficiency
curr_reuters.management
curr_reuters.growth_summary
curr_reuters.performance_summary
curr_reuters.institution_holdings
curr_reuters.recommendations
curr_reuters.analyst_recs
curr_reuters.sales_analysis
curr_reuters.earnings_analysis
curr_reuters.LTgrowth_analysis
curr_reuters.sales_surprises
curr_reuters.earnings_surprises
curr_reuters.sales_trend
curr_reuters.earnings_trend
curr_reuters.revenue_revisions
curr_reuters.earnings_revisions
curr_reuters.insiders_txns
