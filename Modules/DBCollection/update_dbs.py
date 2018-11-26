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

#tsx = pd.read_csv('TSX.csv')['Symbol']#.tolist()
nyse = pd.read_csv('NYSE.csv')['Symbol']#.tolist()
nasdaq = pd.read_csv('NASDAQ.csv')['Symbol']#.tolist()
amex = pd.read_csv('AMEX.csv')['Symbol']#.tolist()

us_names = pd.concat([nyse,nasdaq,amex]).drop_duplicates().values.tolist()

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


def update_reuters(curr_reuters):
    def datefromstring(datestring):
        
        curr_date = dt.datetime.strptime(datestring.replace("'",''), '%b %y')
        date_range = calendar.monthrange(curr_date.year,curr_date.month)
        date = dt.datetime(curr_date.year,curr_date.month,date_range[1])
        
        return date
    
    reuters_engine = create_engine('sqlite:///reuters.db', echo=False)
    
    
    try:
        overall_df = curr_reuters.overall_df
        overall_df['PullDate'] = dt.datetime.now().date()
        overall_df['PullDate'] = pd.to_datetime(overall_df['PullDate'])
        overall_df.index.names = ['Underlying']
        table_name = 'overviews'
        index_column = 'Underlying'
        update_table(reuters_engine, overall_df, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        revenues_eps_df = curr_reuters.revenues_eps_df.copy()
        revenues_eps_df['Quarter'] = revenues_eps_df['Quarter'].apply(lambda x: datefromstring(x))
        revenues_eps_df = revenues_eps_df.set_index('Underlying')
        revenues_eps_df.columns = revenues_eps_df.columns.tolist()[:-1] + ['Fiscal Year']
        table_name = 'epsAndRevenues'
        index_column = 'Underlying'
        update_table(reuters_engine, revenues_eps_df, table_name, index_column, True, False, Quarter = 'Quarter')
    except:
        None
    
    try:
        sales_ests = curr_reuters.sales_ests
        sales_ests = sales_ests.set_index('Underlying')
        sales_ests['PullDate'] = dt.datetime.now().date()
        sales_ests['PullDate'] = pd.to_datetime(sales_ests['PullDate'])
        table_name = 'salesEstimates'
        index_column = 'Underlying'
        update_table(reuters_engine, sales_ests, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        earnings_ests = curr_reuters.earnings_ests
        earnings_ests = earnings_ests.set_index('Underlying')
        earnings_ests['PullDate'] = dt.datetime.now().date()
        earnings_ests['PullDate'] = pd.to_datetime(earnings_ests['PullDate'])
        table_name = 'earningsEstimates'
        index_column = 'Underlying'
        update_table(reuters_engine, earnings_ests, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        LTgrowth_ests = curr_reuters.LTgrowth_ests
        LTgrowth_ests['PullDate'] = dt.datetime.now().date()
        LTgrowth_ests['PullDate'] = pd.to_datetime(LTgrowth_ests['PullDate'])
        LTgrowth_ests.index.names = ['Underlying']
        table_name = 'longTermGrowthEstimates'
        index_column = 'Underlying'
        update_table(reuters_engine, LTgrowth_ests, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
        
    try:    
        valuations = curr_reuters.valuations
        valuations['PullDate'] = dt.datetime.now().date()
        valuations['PullDate'] = pd.to_datetime(valuations['PullDate'])
        valuations = valuations.set_index('Underlying')
        table_name = 'valuations'
        index_column = 'Underlying'
        update_table(reuters_engine, valuations, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
        
    try:
        dividends = curr_reuters.dividends
        dividends['PullDate'] = dt.datetime.now().date()
        dividends['PullDate'] = pd.to_datetime(dividends['PullDate'])
        dividends = dividends.set_index('Underlying')
        table_name = 'dividends'
        index_column = 'Underlying'
        update_table(reuters_engine, dividends, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        growthrate = curr_reuters.growthrate
        growthrate['PullDate'] = dt.datetime.now().date()
        growthrate['PullDate'] = pd.to_datetime(growthrate['PullDate'])
        growthrate = growthrate.set_index('Underlying')
        table_name = 'growthRates'
        index_column = 'Underlying'
        update_table(reuters_engine, growthrate, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        finstrength = curr_reuters.finstrength
        finstrength['PullDate'] = dt.datetime.now().date()
        finstrength['PullDate'] = pd.to_datetime(finstrength['PullDate'])
        finstrength = finstrength.set_index('Underlying')
        table_name = 'financialStrength'
        index_column = 'Underlying'
        update_table(reuters_engine, finstrength, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        profitability = curr_reuters.profitability
        profitability['PullDate'] = dt.datetime.now().date()
        profitability['PullDate'] = pd.to_datetime(profitability['PullDate'])
        profitability = profitability.set_index('Underlying')
        table_name = 'profitability'
        index_column = 'Underlying'
        update_table(reuters_engine, profitability, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        efficiency = curr_reuters.efficiency
        efficiency['PullDate'] = dt.datetime.now().date()
        efficiency['PullDate'] = pd.to_datetime(efficiency['PullDate'])
        efficiency = efficiency.set_index('Underlying')
        table_name = 'efficiency'
        index_column = 'Underlying'
        update_table(reuters_engine, efficiency, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        management = curr_reuters.management
        management['PullDate'] = dt.datetime.now().date()
        management['PullDate'] = pd.to_datetime(management['PullDate'])
        management = management.set_index('Underlying')
        table_name = 'managementAbilities'
        index_column = 'Underlying'
        update_table(reuters_engine, management, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        growth_summary = curr_reuters.growth_summary
        growth_summary['PullDate'] = dt.datetime.now().date()
        growth_summary['PullDate'] = pd.to_datetime(growth_summary['PullDate'])
        growth_summary = growth_summary.set_index('Underlying')
        table_name = 'growthSummary'
        index_column = 'Underlying'
        update_table(reuters_engine, growth_summary, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        performance_summary = curr_reuters.performance_summary
        performance_summary['PullDate'] = dt.datetime.now().date()
        performance_summary['PullDate'] = pd.to_datetime(performance_summary['PullDate'])
        performance_summary = performance_summary.set_index('Underlying')
        table_name = 'performanceSummary'
        index_column = 'Underlying'
        update_table(reuters_engine, performance_summary, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        institution_holdings = curr_reuters.institution_holdings
        institution_holdings['PullDate'] = dt.datetime.now().date()
        institution_holdings['PullDate'] = pd.to_datetime(institution_holdings['PullDate'])
        institution_holdings.index.names = ['Underlying']
        table_name = 'institutionHoldings'
        index_column = 'Underlying'
        update_table(reuters_engine, institution_holdings, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        recommendations = curr_reuters.recommendations
        recommendations['PullDate'] = dt.datetime.now().date()
        recommendations['PullDate'] = pd.to_datetime(recommendations['PullDate'])
        recommendations.index.names = ['Underlying']
        table_name = 'recommendations'
        index_column = 'Underlying'
        update_table(reuters_engine, recommendations, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        analyst_recs = curr_reuters.analyst_recs
        analyst_recs['PullDate'] = dt.datetime.now().date()
        analyst_recs['PullDate'] = pd.to_datetime(analyst_recs['PullDate'])
        analyst_recs = analyst_recs.set_index('Underlying')
        table_name = 'analystRecommendations'
        index_column = 'Underlying'
        update_table(reuters_engine, analyst_recs, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        sales_analysis = curr_reuters.sales_analysis
        sales_analysis['PullDate'] = dt.datetime.now().date()
        sales_analysis['PullDate'] = pd.to_datetime(sales_analysis['PullDate'])
        sales_analysis = sales_analysis.set_index('Underlying')
        table_name = 'salesAnalysis'
        index_column = 'Underlying'
        update_table(reuters_engine, sales_analysis, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        earnings_analysis = curr_reuters.earnings_analysis
        earnings_analysis['PullDate'] = dt.datetime.now().date()
        earnings_analysis['PullDate'] = pd.to_datetime(earnings_analysis['PullDate'])
        earnings_analysis = earnings_analysis.set_index('Underlying')
        table_name = 'earningsAnalysis'
        index_column = 'Underlying'
        update_table(reuters_engine, earnings_analysis, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        LTgrowth_analysis = curr_reuters.LTgrowth_analysis
        LTgrowth_analysis['PullDate'] = dt.datetime.now().date()
        LTgrowth_analysis['PullDate'] = pd.to_datetime(LTgrowth_analysis['PullDate'])
        LTgrowth_analysis.index.names = ['Underlying']
        table_name = 'longTermGrowthAnalysis'
        index_column = 'Underlying'
        update_table(reuters_engine, LTgrowth_analysis, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        sales_surprises = curr_reuters.sales_surprises
        sales_surprises['PullDate'] = dt.datetime.now().date()
        sales_surprises['PullDate'] = pd.to_datetime(sales_surprises['PullDate'])
        sales_surprises = sales_surprises.set_index('Underlying')
        table_name = 'salesSurprises'
        index_column = 'Underlying'
        update_table(reuters_engine, sales_surprises, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        earnings_surprises = curr_reuters.earnings_surprises
        earnings_surprises['PullDate'] = dt.datetime.now().date()
        earnings_surprises['PullDate'] = pd.to_datetime(earnings_surprises['PullDate'])
        earnings_surprises = earnings_surprises.set_index('Underlying')
        table_name = 'earningsSurprises'
        index_column = 'Underlying'
        update_table(reuters_engine, earnings_surprises, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        sales_trend = curr_reuters.sales_trend
        sales_trend['PullDate'] = dt.datetime.now().date()
        sales_trend['PullDate'] = pd.to_datetime(sales_trend['PullDate'])
        sales_trend = sales_trend.set_index('Underlying')
        table_name = 'salesTrend'
        index_column = 'Underlying'
        update_table(reuters_engine, sales_trend, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        earnings_trend = curr_reuters.earnings_trend
        earnings_trend['PullDate'] = dt.datetime.now().date()
        earnings_trend['PullDate'] = pd.to_datetime(earnings_trend['PullDate'])
        earnings_trend = earnings_trend.set_index('Underlying')
        table_name = 'earningsTrend'
        index_column = 'Underlying'
        update_table(reuters_engine, earnings_trend, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        revenue_revisions = curr_reuters.revenue_revisions
        revenue_revisions['PullDate'] = dt.datetime.now().date()
        revenue_revisions['PullDate'] = pd.to_datetime(revenue_revisions['PullDate'])
        revenue_revisions = revenue_revisions.set_index('Underlying')
        table_name = 'revenueRevisions'
        index_column = 'Underlying'
        update_table(reuters_engine, revenue_revisions, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        earnings_revisions = curr_reuters.earnings_revisions
        earnings_revisions['PullDate'] = dt.datetime.now().date()
        earnings_revisions['PullDate'] = pd.to_datetime(earnings_revisions['PullDate'])
        earnings_revisions = earnings_revisions.set_index('Underlying')
        table_name = 'earningsRevisions'
        index_column = 'Underlying'
        update_table(reuters_engine, earnings_revisions, table_name, index_column, False, False, PullDate = 'PullDate')
    except:
        None
    
    try:
        insiders_txns = curr_reuters.insiders_txns
        insiders_txns['Trading Date'] = pd.to_datetime(insiders_txns['Trading Date'])
        insiders_txns = insiders_txns.set_index('Underlying')
        insiders_txns.columns = [x.replace(' ','') for x in insiders_txns.columns.tolist()]
        insiders_txns = insiders_txns.groupby(['Name','Title','TradingDate','Type']).agg({'Price':'mean','SharesTraded':'sum'}).reset_index()
        insiders_txns['Underlying'] = ticker
        insiders_txns = insiders_txns.set_index('Underlying')
        table_name = 'insiderTxns'
        index_column = 'Underlying'
        update_table(reuters_engine, insiders_txns, table_name, index_column, False, False, TradingDate = 'TradingDate')
    except:
        None

#%% Collecting Data

if __name__ == '__main__':
    
    start_time = time.time()
    i = 0
    total_length = len(us_names)
    for ticker in us_names:
        
        try:
            curr_reuters = reuters_query(ticker)
            update_reuters(curr_reuters)
        except:
            print("Reuters Failed on {}".format(ticker))
        
        try:
            curr_yahoo = yahoo_query(ticker, start_date = dt.datetime(2016,1,1))
            curr_yahoo.full_info_query()
            update_yahoo(ticker, curr_yahoo)
        except:
            print("Yahoo Failed on {}".format(ticker))
            
        try:
            curr_alphaquery = alphaquery(ticker)
            alphaquery_update(curr_alphaquery)
        except:
            print("AQ Failed on {}".format(ticker))
        
        try:
            curr_earnings_history = earnings_report(ticker).dropna()
            earnHistory_update(curr_earnings_history, ticker)
        except:
            print("Failed on {}".format(ticker))
        i += 1
        print('{0:.2f}% Completed'.format(i/total_length*100))

    print("Completed in %s seconds" % (time.time() - start_time))
    

