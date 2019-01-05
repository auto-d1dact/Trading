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
import calendar
import time

#%%
module_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\DataCollection'

os.chdir(module_dir)

print(os.getcwd())
from reuters_query import reuters_query
from option_slam_earnings import earnings_report
from alphaquery import alphaquery
from yahoo_query import yahoo_query

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

#tsx = pd.read_csv('TSX.csv')['Symbol']#.tolist()
nyse = pd.read_csv('NYSE.csv')['Symbol']#.tolist()
nasdaq = pd.read_csv('NASDAQ.csv')['Symbol']#.tolist()
amex = pd.read_csv('AMEX.csv')['Symbol']#.tolist()

us_names = pd.concat([nyse,nasdaq,amex]).drop_duplicates().values.tolist()
exclude_names = pd.read_csv('exclude_names.csv').iloc[:,0].tolist()
us_names = list(filter(lambda x: x not in exclude_names, us_names))

os.chdir('..\\DBs')


#%%
yahoo_table_dict = {'earnings_annual': ['annualEarnings', 'year'],
                    'earnings_quarterly': ['quarterlyEarnings', 'quarter'],
                    'profile': ['profiles', 'PullDate'],
                    'cashFlowStatementAnnual': ['annualCashFlow', 'year'],
                    'cashFlowStatementQuarter': ['quarterlyCashFlow', 'quarter'],
                    'majorHolderInfo': ['institutionOwners', 'PullDate'],
                    'recommendationTrend': ['recommendationTrend', 'PullDate'],
                    'keyStats': ['keyStats', 'PullDate'],
                    'purchaseActivity': ['purchaseActivity', 'PullDate'],
                    'insiderTxns': ['insiderTxns', 'startDate'],
                    'incomeStatementAnnual': ['annualIncomeStatement', 'year'],
                    'incomeStatementQuarter': ['quarterlyIncomeStatement', 'quarter'],
                    'balanceSheetAnnual': ['annualBalanceSheet', 'year'],
                    'balanceSheetQuarter': ['quarterlyBalanceSheet', 'quarter'],
                    'insiderHolders': ['insiderHolders', 'latestTransDate'],
                    'finData': ['finData','PullDate']}

reuters_table_dict = {'overall_df':['overviews', 'PullDate'],
                      'revenues_eps_df':['epsAndRevenues', 'Quarter'],
                      'sales_ests':['salesEstimates', 'PullDate'],
                      'earnings_ests':['earningsEstimates', 'PullDate'],
                      'LTgrowth_ests':['longTermGrowthEstimates', 'PullDate'],
                      'valuations':['valuations', 'PullDate'],
                      'dividends':['dividends', 'PullDate'],
                      'growthrate':['growthRates', 'PullDate'],
                      'finstrength':['financialStrength', 'PullDate'],
                      'profitability':['profitability', 'PullDate'],
                      'efficiency':['efficiency', 'PullDate'],
                      'management':['managementAbilities', 'PullDate'],
                      'growth_summary':['growthSummary', 'PullDate'],
                      'performance_summary':['performanceSummary','PullDate'],
                      'institution_holdings':['institutionHoldings','PullDate'],
                      'recommendations':['recommendations','PullDate'],
                      'analyst_recs':['analystRecommendations','PullDate'],
                      'sales_analysis':['salesAnalysis','PullDate'],
                      'earnings_analysis':['earningsAnalysis','PullDate'],
                      'LTgrowth_analysis':['longTermGrowthAnalysis','PullDate'],
                      'sales_surprises':['salesSurprises','PullDate'],
                      'earnings_surprises':['earningsSurprises','ReportDate'],
                      'sales_trend':['salesTrend','PullDate'],
                      'earnings_trend':['earningsTrend','PullDate'],
                      'revenue_revisions':['revenueRevisions','PullDate'],
                      'earnings_revisions':['earningsRevisions','PullDate'],
                      'insiders_txns':['insiderTxns','TradingDate']}


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
    
    table_in_db = True
    try:
        query = 'SELECT earningsDate, Underlying FROM postEarningsReturns WHERE Underlying = "{}"'.format(ticker)
        earnings_table_db = pd.read_sql_query(query, con=earningHist_engine, index_col = 'earningsDate')
        earnings_table_db.index = pd.to_datetime(earnings_table_db.index)
    except:
        table_in_db = False
    
    if table_in_db:
        curr_earnings_history = curr_earnings_history[list(filter(lambda x: x != 'Underlying',curr_earnings_history.columns.tolist()))]
        curr_earnings_history = curr_earnings_history.join(earnings_table_db, how = 'left').fillna('na')
        curr_earnings_history = curr_earnings_history[curr_earnings_history['Underlying'] == 'na']
        curr_earnings_history['Underlying'] = ticker
        
        
    curr_earnings_history.to_sql('postEarningsReturns', con=earningHist_engine, 
                                 if_exists='append', index_label = 'earningsDate')
    return

#%%
def update_yahoo(ticker, curr_yahoo, db_dict):
    engine = create_engine('sqlite:///yahoo.db', echo = False)
    
    def convertUTCtime(x):
        if x != 0:
            return dt.datetime.utcfromtimestamp(x).strftime('%m/%d/%Y')
        else:
            return np.nan
    
    for attr, value in curr_yahoo.__dict__.items():
        
        if len(value) == 0:
            continue
        
        if attr not in db_dict.keys():
            continue
        
        curr_table_props = db_dict[attr]
        curr_table_name = curr_table_props[0]
        curr_table_date_col = curr_table_props[1]
        
        pulled_table = value.copy()
        if curr_table_date_col == 'PullDate': # With Pull Date, simply append to DB
            pulled_table['PullDate'] = dt.datetime.now().date()
            pulled_table['PullDate'] = pd.to_datetime(pulled_table['PullDate'])
            
            if curr_table_name == 'recommendationTrend':
                pulled_table['Underlying'] = ticker
                pulled_table.index.names = ['index']
            else:
                pulled_table.index.names = ['Underlying']
        else: # Without pull date, must check if records exist already
            pulled_table['Underlying'] = ticker
            if curr_table_name == 'insiderTxns':
                query = 'SELECT * FROM {0} WHERE Underlying = "{1}"'.format(curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = 'Underlying')
                earnings_table_db['startDate'] = pd.to_datetime(earnings_table_db['startDate'])
                earnings_table_db['Missing'] = 0
                
                pulled_table['startDate'] = pd.to_datetime(pulled_table['startDate'])
                pulled_table = pulled_table.reset_index().set_index('Underlying')
                
                pulled_table = pulled_table.merge(earnings_table_db, how = 'left', on = pulled_table.columns.tolist())
                pulled_table = pulled_table[pulled_table['Missing'].isnull()]
                del pulled_table['Missing']
                
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.reset_index(drop = True).set_index('Underlying')
                pulled_table.index.names = ['Underlying']
                            
            elif curr_table_name == 'insiderHolders':
                query = 'SELECT * FROM {0} WHERE Underlying = "{1}"'.format(curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = 'Underlying')
                earnings_table_db['latestTransDate'] = pd.to_datetime(earnings_table_db['latestTransDate'])
                earnings_table_db['positionDirectDate'] = pd.to_datetime(earnings_table_db['positionDirectDate'])
                earnings_table_db['Missing'] = 0
                
                pulled_table['latestTransDate'] = pd.to_numeric(pulled_table['latestTransDate'])
                pulled_table['positionDirectDate'] = pd.to_numeric(pulled_table['positionDirectDate'])
                pulled_table['latestTransDate'] = pulled_table['latestTransDate'].fillna(0).apply(lambda x: convertUTCtime(x))
                pulled_table['latestTransDate'] = pulled_table.latestTransDate.astype(object).where(pulled_table.latestTransDate.notnull(), np.nan)
                
                pulled_table['positionDirectDate'] = pulled_table['positionDirectDate'].fillna(0).apply(lambda x: convertUTCtime(x))
                pulled_table['positionDirectDate'] = pulled_table.positionDirectDate.astype(object).where(pulled_table.positionDirectDate.notnull(), np.nan)
                
                pulled_table['latestTransDate'] = pd.to_datetime(pulled_table['latestTransDate'])
                pulled_table['positionDirectDate'] = pd.to_datetime(pulled_table['positionDirectDate'])
                pulled_table = pulled_table.set_index('Underlying')
                
                pulled_table = pulled_table.merge(earnings_table_db, how = 'left', on = pulled_table.columns.tolist())
                pulled_table = pulled_table[pulled_table['Missing'].isnull()]
                del pulled_table['Missing']
                
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.reset_index(drop = True).set_index('Underlying')
                pulled_table.index.names = ['Underlying']
                
            else:
                query = 'SELECT {0}, Underlying FROM {1} WHERE Underlying = "{2}"'.format(curr_table_date_col, curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = curr_table_date_col)
                
                if curr_table_name != 'annualEarnings':
                    earnings_table_db.index = pd.to_datetime(earnings_table_db.index)
                
                pulled_table.index.names = [curr_table_date_col]
                pulled_table = pulled_table[list(filter(lambda x: x != 'Underlying', pulled_table.columns.tolist()))]
                pulled_table = pulled_table.join(earnings_table_db, how = 'left').fillna('na')
                pulled_table = pulled_table[pulled_table['Underlying'] == 'na']
                pulled_table['Underlying'] = ticker
        
        if len(pulled_table) > 0:
            pulled_table.to_sql(curr_table_name, con=engine, if_exists='append')
    

#%%
def update_reuters(curr_reuters, ticker, db_dict):
    engine = create_engine('sqlite:///reuters.db', echo=False)
    
    def datefromstring(datestring):
        
        curr_date = dt.datetime.strptime(datestring.replace("'",''), '%b %y')
        date_range = calendar.monthrange(curr_date.year,curr_date.month)
        date = dt.datetime(curr_date.year,curr_date.month,date_range[1])
        
        return date
    
    for attr, value in curr_reuters.__dict__.items():
        
        if len(value) == 0:
            continue
        
        if attr not in db_dict.keys():
            continue
        curr_table_props = db_dict[attr]
        curr_table_name = curr_table_props[0]
        curr_table_date_col = curr_table_props[1]
        pulled_table = value.copy()
        
        if curr_table_date_col == 'PullDate': # With Pull Date, simply append to DB
            pulled_table['PullDate'] = dt.datetime.now().date()
            pulled_table['PullDate'] = pd.to_datetime(pulled_table['PullDate'])
            
            
            if curr_table_name in ['overviews', 'longTermGrowthEstimates','institutionHoldings','recommendations','longTermGrowthAnalysis']:
                pulled_table.index.names = ['Underlying']
            else:
                pulled_table = pulled_table.set_index('Underlying')
        else: # Without pull date, must check if records exist already
            if curr_table_name == 'epsAndRevenues':
                query = 'SELECT Underlying, Quarter, "Fiscal Year" FROM {0} WHERE Underlying = "{1}"'.format(curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = 'Underlying')
                earnings_table_db['Quarter'] = pd.to_datetime(earnings_table_db['Quarter'])
                earnings_table_db['Missing'] = 0
                
                pulled_table['Quarter'] = pulled_table['Quarter'].apply(lambda x: datefromstring(x))
                pulled_table['Quarter'] = pd.to_datetime(pulled_table['Quarter'])
                pulled_table = pulled_table.set_index('Underlying')
                pulled_table.columns = pulled_table.columns.tolist()[:-1] + ['Fiscal Year']
                
                pulled_table = pulled_table.merge(earnings_table_db, how = 'left', on = ['Quarter','Fiscal Year'])
                pulled_table = pulled_table[pulled_table['Missing'].isnull()]
                del pulled_table['Missing']
                
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.set_index('Underlying')
                
            elif curr_table_name == 'insiderTxns':
                query = 'SELECT * FROM {0} WHERE Underlying = "{1}"'.format(curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = 'Underlying')
                earnings_table_db['TradingDate'] = pd.to_datetime(earnings_table_db['TradingDate'])
                earnings_table_db['Missing'] = 0
                
                pulled_table['Trading Date'] = pd.to_datetime(pulled_table['Trading Date'])
                pulled_table = pulled_table.set_index('Underlying')
                pulled_table.columns = [x.replace(' ','') for x in pulled_table.columns.tolist()]
                pulled_table = pulled_table.groupby(['Name','Title','TradingDate','Type']).agg({'Price':'mean','SharesTraded':'sum'}).reset_index()
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.set_index('Underlying')
                
                pulled_table = pulled_table.merge(earnings_table_db, how = 'left', on = pulled_table.columns.tolist())
                pulled_table = pulled_table[pulled_table['Missing'].isnull()]
                del pulled_table['Missing']
                
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.set_index('Underlying')
            
            elif curr_table_name == 'earningsSurprises':
                query = 'SELECT Underlying, ReportDate FROM {0} WHERE Underlying = "{1}"'.format(curr_table_name, ticker)
                earnings_table_db = pd.read_sql_query(query, con = engine, index_col = 'Underlying')
                earnings_table_db['Missing'] = 0
                
                pulled_table = pulled_table.set_index('Underlying')
                pulled_table = pulled_table.merge(earnings_table_db, how = 'left', on = 'ReportDate')
                pulled_table = pulled_table[pulled_table['Missing'].isnull()]
                del pulled_table['Missing']
                
                pulled_table['Underlying'] = ticker
                pulled_table = pulled_table.set_index('Underlying')
                
        if len(pulled_table) > 0:
                pulled_table.to_sql(curr_table_name, con=engine, if_exists='append')


#%% Collecting Data

if __name__ == '__main__':
    
    start_time = time.time()
    i = 0
    failed_reuters = []
    failed_yahoo = []
    failed_earnings = []
    
    total_length = len(us_names)
    for ticker in us_names:
        
        try:
            curr_reuters = reuters_query(ticker)
            update_reuters(curr_reuters, ticker, reuters_table_dict)
        except:
            print("Reuters Failed on {}".format(ticker))
            failed_reuters.append(ticker)
        
        try:
            curr_yahoo = yahoo_query(ticker, start_date = dt.datetime(2016,1,1))
            curr_yahoo.full_info_query()
            update_yahoo(ticker, curr_yahoo, yahoo_table_dict)
        except:
            print("Yahoo Failed on {}".format(ticker))
            failed_yahoo.append(ticker)
            
        try:
            curr_alphaquery = alphaquery(ticker)
            alphaquery_update(curr_alphaquery)
        except:
            print("AQ Failed on {}".format(ticker))
        
        try:
            curr_earnings_history = earnings_report(ticker).dropna()
            curr_earnings_history = curr_earnings_history[~curr_earnings_history.index.duplicated(keep='first')]
            earnHistory_update(curr_earnings_history, ticker)
        except:
            print("Failed on {}".format(ticker))
            failed_earnings.append(ticker)
            
        i += 1
        print('{0:.2f}% Completed'.format(i/total_length*100))
    
    pd.DataFrame(index = failed_reuters).to_csv('failed_reuters.csv')
    pd.DataFrame(index = failed_yahoo).to_csv('failed_yahoo.csv')
    pd.DataFrame(index = failed_earnings).to_csv('failed_earnings.csv')
    
    print("Completed in %s seconds" % (time.time() - start_time))
    
