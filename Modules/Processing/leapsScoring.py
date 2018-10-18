# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 23:40:55 2018

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

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)
from yahoo_query import *

# Initializing Fin Statement Data
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Historical Queries\\US')

file_date = '2018-10-18'
us_annual = pd.read_csv('us_annual_{}.csv'.format(file_date), index_col = 0)
us_keystats = pd.read_csv('us_keystats_{}.csv'.format(file_date), index_col = 0)
us_quarterly = pd.read_csv('us_quarterly_{}.csv'.format(file_date), index_col = 0)


os.chdir('..\\Whales')

file_date = '2018-10-14'

whale_df = pd.read_csv('whales {}.csv'.format(file_date), index_col = 0)

hedgeFunds = whale_df[whale_df.index == 'hedgeFunds'].dropna()
hedgeFunds.index = hedgeFunds.ticker

allFunds = whale_df[whale_df.index == 'allFirms'].dropna()
allFunds.index = allFunds.ticker

os.chdir(main_dir)

def leaps_scores(data_df, keystats, min_score):
    try:
        df = data_df[['Underlying', 'earnings', 'netIncome','totalRevenue','totalStockholderEquity']]
    except:
        df = data_df[['Underlying', 'epsActual', 'netIncome','totalRevenue','totalStockholderEquity']]
    ks = keystats[['52WeekChange','currentPrice','sector','forwardPE','forwardEps','pegRatio']]
    ks['52WeekChange'] = pd.to_numeric(ks['52WeekChange'].replace('Infinity',0))

    df['profitMargin'] = df.netIncome/df.totalRevenue
    df['roe'] = df.netIncome/df.totalStockholderEquity

    # us_quarterly[['Underlying', 'retainedEarnings', 'netIncome','totalRevenue','totalStockholderEquity']]

    leap_scores = pd.DataFrame(columns = ['earningsGrowth','profitMargins','profitMarginChange',
                                          'roe','roeChange'],
                               index = df.Underlying.drop_duplicates().tolist()).fillna(0)

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

    leap_scores['score'] = leap_scores.sum(axis = 1)
    leap_scores = leap_scores[leap_scores.score >= min_score].join(ks).sort_values('52WeekChange', ascending = False)
    return leap_scores

def main():
    leaps_annual = leaps_scores(us_annual, us_keystats, 3)#.replace('Infinity', 0)
    leaps_quarterly = leaps_scores(us_quarterly, us_keystats, 3)#.replace('Infinity', 0)
    
    
    spdr_lst = ['SPY','XLU','XLRE','XLY','XLV',
                'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
                'XLP', 'XLE']
    
    sector_lst = []
    for ticker in spdr_lst:
        curr_etf = yahoo_query(ticker, dt.datetime(1980,1,1))
        curr_etf.hist_prices_query()
        sector_lst.append(curr_etf.hist_prices[['{}_close'.format(ticker)]])
        
    sector_prices = pd.concat(sector_lst, axis = 1)
    sector_prices.columns = ['SPY','Utilities','Real Estate','Consumer Cyclical',
                             'Healthcare', 'Basic Materials', 'Industrials',
                             'Financial Services', 'Technology', 'Communication Services',
                             'Consumer Defensive', 'Energy']
    
    leaps_annual_picks = leaps_annual[(leaps_annual.profitMargins > 0) &
                                      (leaps_annual.earningsGrowth > 0) &
                                      (leaps_annual.roe > 0) &
                                      (leaps_annual.profitMarginChange > 0)].join(hedgeFunds[['numOfFirmsHoldingInTop10','numOfHolders',
                                                                                                'fundNumPercentChange',
                                                                                                 'fundsCreatingNewPos',
                                                                                                 'fundsAddingPos',
                                                                                                 'fundsClosingPos',
                                                                                                 'fundsReducingPos']]).sort_values('fundNumPercentChange', 
                                                                                                                                   ascending = False)
    leaps_annual_picks['industryBeta'] = np.nan
    leaps_annual_picks['52WeekSectorChange'] = np.nan
    # leaps_annual_picks
    
    leaps_hist_prices = {}
    
    for idx, row in leaps_annual_picks.iterrows():
        curr_prices = yahoo_query(idx, dt.datetime(1980,1,1))
        curr_prices.hist_prices_query()
        leaps_hist_prices[idx] = curr_prices.hist_prices[['{}_close'.format(idx)]].join(sector_prices[[row.sector]])
        
    for ticker in leaps_hist_prices.keys():
        curr_prices = leaps_hist_prices[ticker].pct_change().dropna()
        curr_sector_ret = leaps_hist_prices[ticker].pct_change(252).dropna()[curr_prices.columns.tolist()[1]]
        try:
            curr_corr = curr_prices['{}_close'.format(ticker)].rolling(252).corr(curr_prices.iloc[:,1]).dropna()[-1]
            leaps_annual_picks.loc[ticker, 'industryBeta'] = curr_corr
            leaps_annual_picks.loc[ticker, '52WeekSectorChange'] = curr_sector_ret[-1]
        except:
            continue
            
    annual_neg_sectors = leaps_annual_picks[(leaps_annual_picks['52WeekSectorChange'] < 0) &
                                            (leaps_annual_picks['industryBeta'] < 0.5)]
    
    annual_pos_sectors = leaps_annual_picks[(leaps_annual_picks['52WeekSectorChange'] > 0) &
                                            (leaps_annual_picks['industryBeta'] < 0.5) &
                                            (leaps_annual_picks['52WeekChange'] > leaps_annual_picks['52WeekSectorChange'])]
    
    annual_leaps = pd.concat([annual_neg_sectors, annual_pos_sectors], axis = 0)
    
    leaps_quarterly_picks = leaps_quarterly.join(hedgeFunds[['numOfFirmsHoldingInTop10',
                                     'numOfHolders',
                                     'fundNumPercentChange',
                                     'fundsCreatingNewPos',
                                     'fundsAddingPos',
                                     'fundsClosingPos',
                                     'fundsReducingPos']]).sort_values('fundNumPercentChange', ascending = False)
    leaps_quarterly_picks['industryBeta'] = np.nan
    leaps_quarterly_picks['52WeekSectorChange'] = np.nan
    
    
    leaps_hist_quarterly_prices = {}
    
    for idx, row in leaps_quarterly_picks.iterrows():
        curr_prices = yahoo_query(idx, dt.datetime(1980,1,1))
        curr_prices.hist_prices_query()
        leaps_hist_quarterly_prices[idx] = curr_prices.hist_prices[['{}_close'.format(idx)]].join(sector_prices[[row.sector]])
        
    for ticker in leaps_hist_quarterly_prices.keys():
        curr_prices = leaps_hist_quarterly_prices[ticker].pct_change().dropna()
        curr_sector_ret = leaps_hist_quarterly_prices[ticker].pct_change(252).dropna()[curr_prices.columns.tolist()[1]]
        try:
            curr_corr = curr_prices['{}_close'.format(ticker)].rolling(252).corr(curr_prices.iloc[:,1]).dropna()[-1]
            leaps_quarterly_picks.loc[ticker, 'industryBeta'] = curr_corr
            leaps_quarterly_picks.loc[ticker, '52WeekSectorChange'] = curr_sector_ret[-1]
        except:
            continue
        
    quarterly_neg_sectors = leaps_quarterly_picks[(leaps_quarterly_picks['52WeekSectorChange'] < 0) &
                                               (leaps_quarterly_picks['industryBeta'] < 0.5)]
    
    quarterly_pos_sectors = leaps_quarterly_picks[(leaps_quarterly_picks['52WeekSectorChange'] > 0) &
                                            (leaps_quarterly_picks['industryBeta'] < 0.5) &
                                            (leaps_quarterly_picks['52WeekChange'] > leaps_quarterly_picks['52WeekSectorChange'])]
    
    quarterly_leaps = pd.concat([quarterly_neg_sectors, quarterly_pos_sectors], axis = 0)
    
    return leaps_annual, leaps_quarterly, annual_leaps, quarterly_leaps
    

os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Single Name Pulls')
#%%

if __name__ == "__main__":
    
    leaps_annual, leaps_quarterly, annual_leaps, quarterly_leaps = main()
    datenow = dt.datetime.today().strftime('%Y-%m-%d')
    filename = 'us_nameList {}.xlsx'.format(datenow)
    
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    leaps_annual.to_excel(writer, sheet_name='leaps_a')
    leaps_quarterly.to_excel(writer, sheet_name = 'leaps_q')
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    
    picknames = 'us_namePicks {}.xlsx'.format(datenow)
    
    writer = pd.ExcelWriter(picknames, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    annual_leaps.to_excel(writer, sheet_name='leaps_a')
    quarterly_leaps.to_excel(writer, sheet_name = 'leaps_q')
    
    
    writer.save()
    os.chdir(main_dir)