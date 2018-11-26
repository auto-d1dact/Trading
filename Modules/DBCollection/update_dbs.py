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

#%% Functions for Writing to databases

def update_table(con_engine, latest_data, table_name, index_column, composite_index = False, finstate = True, **datecols):
    
    table_exists = True
    try:
        table_db = pd.read_sql_query('SELECT * FROM {}'.format(table_name), 
                                     con=con_engine, index_col = index_column)
        for col,datetype in datecols.items():
            if col == 'index':
                table_db.index = pd.to_datetime(table_db.index)
            else:
                table_db[col] = pd.to_datetime(table_db[col])
                
        #print(table_db)
    except:
        table_exists = False
    
    if table_exists:
        if composite_index:
            for col,datetype in datecols.items():
                if datetype != 'index':
                    update_table = pd.concat([table_db, latest_data], axis = 0)
                    if finstate:
                        update_table = update_table.reset_index().drop_duplicates([datetype,'Underlying']).set_index(datetype)
                    else:
                        update_table.index.names = [index_column]
                        update_table = update_table.reset_index().drop_duplicates([datetype,index_column]).set_index(index_column)
                    
        else:
            update_table = pd.concat([table_db,latest_data], axis = 0).reset_index().drop_duplicates().set_index(index_column)
    else:
        update_table = latest_data
    
    update_table.to_sql(table_name, con=con_engine, 
                        if_exists='replace', index_label = index_column)
    
    return

#%%
def alphaquery_update(query):
    aq_engine = create_engine('sqlite:///alphaquery.db', echo=False)
    curr_alphaquery = query.vol_df
    curr_alphaquery['PullDate'] = dt.datetime.now().date()
    curr_alphaquery.to_sql('aq_vols', con=aq_engine, if_exists='append')
    return 

def earnHistory_update(earningsReport, ticker):
    earningHist_engine = create_engine('sqlite:///earningsHistory.db', echo=False)
    
    curr_earnings_history = earningsReport.copy()
    
    curr_earnings_history.columns = ['earningsTime','closeToOpenReturn','industryBeta','marketBeta',
                                     'stock52WeekReturn','market52WeekReturn','industry52WeekReturn']
    curr_earnings_history['Underlying'] = ticker
        
    if len(curr_earnings_history['Underlying']) != len(curr_earnings_history.reset_index()[['index','Underlying']].drop_duplicates()):
        curr_earnings_history = curr_earnings_history.head(len(curr_earnings_history) - 1)
    
    table_in_db = True
    try:
        earnings_table_db = pd.read_sql_query('SELECT * FROM postEarningsReturns', con=earningHist_engine, index_col = 'earningsDate')
        earnings_table_db.index = pd.to_datetime(earnings_table_db.index)
    except:
        table_in_db = False
    
    if table_in_db:
        curr_earnings_history = pd.concat([earnings_table_db,
                                           curr_earnings_history]).reset_index().drop_duplicates(['index','Underlying']).set_index('index')
    
    curr_earnings_history.to_sql('postEarningsReturns', con=earningHist_engine, 
                                 if_exists='replace', index_label = 'earningsDate')
    return

#%%
def update_yahoo(ticker, curr_yahoo):
    
    yahoo_engine = create_engine('sqlite:///yahoo.db', echo=False)
    
    try:
        earnings_annual = curr_yahoo.earnings_annual
        earnings_annual.index.names = ['year']
        earnings_annual['Underlying'] = ticker
        table_name = 'annualEarnings'
        index_column = 'year'
        update_table(yahoo_engine, earnings_annual, table_name, index_column)
    except:
        None
    
    try:
        earnings_quarterly = curr_yahoo.earnings_quarterly
        earnings_quarterly.index.names = ['quarter']
        earnings_quarterly['Underlying'] = ticker
        table_name = 'quarterlyEarnings'
        index_column = 'quarter'
        update_table(yahoo_engine, earnings_quarterly, table_name, index_column, True, True, index = index_column)
    except:
        None
        
    try:
        cashFlowStatementAnnual = curr_yahoo.cashFlowStatementAnnual.sort_index(ascending = True)
        cashFlowStatementAnnual.index.names = ['year']
        cashFlowStatementAnnual['Underlying'] = ticker
        table_name = 'annualCashFlow'
        index_column = 'year'
        update_table(yahoo_engine, cashFlowStatementAnnual, table_name, index_column, True,True, index = index_column)
    except:
        None
        
    try:
        cashFlowStatementQuarter = curr_yahoo.cashFlowStatementQuarter.sort_index(ascending = True)
        cashFlowStatementQuarter.index.names = ['quarter']
        cashFlowStatementQuarter['Underlying'] = ticker
        table_name = 'quarterlyCashFlow'
        index_column = 'quarter'
        update_table(yahoo_engine, cashFlowStatementQuarter, table_name, index_column, True,True, index = index_column)
    except:
        None
    
    try:
        incomeStatementAnnual = curr_yahoo.incomeStatementAnnual
        incomeStatementAnnual.index.names = ['year']
        incomeStatementAnnual['Underlying'] = ticker
        table_name = 'annualIncomeStatement'
        index_column = 'year'
        update_table(yahoo_engine, incomeStatementAnnual, table_name, index_column, True,True, index = index_column)
    except:
        None
    
    try:
        incomeStatementQuarter = curr_yahoo.incomeStatementQuarter
        incomeStatementQuarter.index.names = ['quarter']
        incomeStatementQuarter['Underlying'] = ticker
        table_name = 'quarterlyIncomeStatement'
        index_column = 'quarter'
        update_table(yahoo_engine, incomeStatementQuarter, table_name, index_column, True,True, index = index_column)
    except:
        None
    
    try:
        balanceSheetAnnual = curr_yahoo.balanceSheetAnnual
        balanceSheetAnnual.index.names = ['year']
        balanceSheetAnnual['Underlying'] = ticker
        table_name = 'annualBalanceSheet'
        index_column = 'year'
        update_table(yahoo_engine, balanceSheetAnnual, table_name, index_column, True,True, index = index_column)
    except:
        None
    
    try:
        balanceSheetQuarter = curr_yahoo.balanceSheetQuarter
        balanceSheetQuarter.index.names = ['quarter']
        balanceSheetQuarter['Underlying'] = ticker
        table_name = 'quarterlyBalanceSheet'
        index_column = 'quarter'
        update_table(yahoo_engine, balanceSheetQuarter, table_name, index_column, True,True, index = index_column)
    except:
        None
    
    try:
        profile = curr_yahoo.profile
        profile['PullDate'] = dt.datetime.now().date()
        profile['PullDate'] = pd.to_datetime(profile['PullDate'])
        profile.index.names = ['Underlying']
        table_name = 'profiles'
        index_column = 'Underlying'
        update_table(yahoo_engine, profile, table_name, index_column, True, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        institutionOwners = curr_yahoo.majorHolderInfo
        institutionOwners['PullDate'] = dt.datetime.now().date()
        institutionOwners['PullDate'] = pd.to_datetime(institutionOwners['PullDate'])
        institutionOwners.index.names = ['Underlying']
        table_name = 'institutionOwners'
        index_column = 'Underlying'
        update_table(yahoo_engine, institutionOwners, table_name, index_column, True, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        recommendationTrend = curr_yahoo.recommendationTrend
        recommendationTrend['PullDate'] = dt.datetime.now().date()
        recommendationTrend['PullDate'] = pd.to_datetime(recommendationTrend['PullDate'])
        recommendationTrend['Underlying'] = ticker
        recommendationTrend.index.names = ['index']
        table_name = 'recommendationTrend'
        index_column = 'index'
        update_table(yahoo_engine, recommendationTrend, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        keyStats = curr_yahoo.keyStats
        keyStats.index.names = ['Underlying']
        keyStats['PullDate'] = dt.datetime.now().date()
        keyStats['PullDate'] = pd.keyStats(recommendationTrend['PullDate'])
        table_name = 'keyStats'
        index_column = 'Underlying'
        update_table(yahoo_engine, keyStats, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        purchaseActivity = curr_yahoo.purchaseActivity
        purchaseActivity['PullDate'] = dt.datetime.now().date()
        purchaseActivity.index.names = ['Underlying']
        purchaseActivity['PullDate'] = pd.to_datetime(purchaseActivity['PullDate'])
        table_name = 'purchaseActivity'
        index_column = 'Underlying'
        update_table(yahoo_engine, purchaseActivity, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        insiderTxns = curr_yahoo.insiderTxns
        insiderTxns['Underlying'] = ticker
        insiderTxns['startDate'] = pd.to_datetime(insiderTxns['startDate'])
        insiderTxns = insiderTxns.reset_index().set_index('Underlying')
        table_name = 'insiderTxns'
        index_column = 'Underlying'
        update_table(yahoo_engine, insiderTxns, table_name, index_column, False, False, startDate = 'startDate')
    except:
        None
    
    try:
        insiderHolders = curr_yahoo.insiderHolders.copy()
        insiderHolders['latestTransDate'] = pd.to_numeric(insiderHolders['latestTransDate'])
        insiderHolders['positionDirectDate'] = pd.to_numeric(insiderHolders['positionDirectDate'])
        insiderHolders['latestTransDate'] = insiderHolders['latestTransDate'].apply(lambda x: dt.datetime.utcfromtimestamp(x).strftime('%m/%d/%Y'))
        insiderHolders['positionDirectDate'] = insiderHolders['positionDirectDate'].apply(lambda x: dt.datetime.utcfromtimestamp(x).strftime('%m/%d/%Y'))
        insiderHolders['latestTransDate'] = pd.to_datetime(insiderHolders['latestTransDate'])
        insiderHolders['positionDirectDate'] = pd.to_datetime(insiderHolders['positionDirectDate'])
        insiderHolders['Underlying'] = ticker
        insiderHolders = insiderHolders.set_index('Underlying')
        table_name = 'insiderHolders'
        index_column = 'Underlying'
        update_table(yahoo_engine, insiderHolders, table_name, index_column, False, False, latestTransDate = 'latestTransDate', positionDirectDate = 'positionDirectDate')
    except:
        None
    
    try:
        finData = curr_yahoo.finData
        finData['PullDate'] = dt.datetime.now().date()
        finData['PullDate'] = pd.to_datetime(finData['PullDate'])
        finData.index.names = ['Underlying']
        table_name = 'finData'
        index_column = 'Underlying'
        update_table(yahoo_engine, finData, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    return

#%% Collecting Data


start_time = time.time()

for ticker in ['MOMO','PDCO','GME','NVDA']:
    
    try:
        curr_reuters = reuters_query(ticker)
        
        curr_yahoo = yahoo_query(ticker, start_date = dt.datetime(2016,1,1))
        curr_yahoo.full_info_query()
        update_yahoo(ticker, curr_yahoo)
        
        curr_alphaquery = alphaquery(ticker)
        alphaquery_update(curr_alphaquery)
        
        curr_earnings_history = earnings_report(ticker).dropna()
        
        earnHistory_update(curr_earnings_history, ticker)
        
    except:
        print("Failed on {}".format(ticker))

print("--- %s seconds ---" % (time.time() - start_time))
    


#%%
reuters_engine = create_engine('sqlite:///reuters.db', echo=False)

try:
    overall_df = curr_reuters.overall_df
    overall_df['PullDate'] = dt.datetime.now().date()
    overall_df['PullDate'] = pd.to_datetime(overall_df['PullDate'])
    overall_df.index.names = ['Underlying']
    table_name = 'finData'
    index_column = 'Underlying'
    update_table(reuters_engine, overall_df, table_name, index_column, False, False, PullDate = 'PullDate')
except:
    None

try:
    revenues_eps_df = curr_reuters.revenues_eps_df
except:
    None

try:
    sales_ests = curr_reuters.sales_ests
except:
    None

try:
    earnings_ests = curr_reuters.earnings_ests
except:
    None

try:
    LTgrowth_ests = curr_reuters.LTgrowth_ests
except:
    None
    
try:    
    valuations = curr_reuters.valuations
except:
    None
    
try:
    dividends = curr_reuters.dividends
except:
    None

try:
    growthrate = curr_reuters.growthrate
except:
    None

try:
    finstrength = curr_reuters.finstrength
except:
    None

try:
    profitability = curr_reuters.profitability
except:
    None

try:
    efficiency = curr_reuters.efficiency
except:
    None

try:
    management = curr_reuters.management
except:
    None

try:
    growth_summary = curr_reuters.growth_summary
except:
    None

try:
    performance_summary = curr_reuters.performance_summary
except:
    None

try:
    institution_holdings = curr_reuters.institution_holdings
except:
    None

try:
    recommendations = curr_reuters.recommendations
except:
    None

try:
    analyst_recs = curr_reuters.analyst_recs
except:
    None

try:
    sales_analysis = curr_reuters.sales_analysis
except:
    None

try:
    earnings_analysis = curr_reuters.earnings_analysis
except:
    None

try:
    LTgrowth_analysis = curr_reuters.LTgrowth_analysis
except:
    None

try:
    sales_surprises = curr_reuters.sales_surprises
except:
    None

try:
    earnings_surprises = curr_reuters.earnings_surprises
except:
    None

try:
    sales_trend = curr_reuters.sales_trend
except:
    None

try:
    earnings_trend = curr_reuters.earnings_trend
except:
    None

try:
    revenue_revisions = curr_reuters.revenue_revisions
except:
    None

try:
    earnings_revisions = curr_reuters.earnings_revisions
except:
    None

try:
    insiders_txns = curr_reuters.insiders_txns
except:
    None

