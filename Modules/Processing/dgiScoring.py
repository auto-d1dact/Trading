# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 23:53:44 2018

@author: Fang
"""

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


os.chdir('..\\CAD')

file_date = '2018-10-14'
cad_annual = pd.read_csv('cad_annual_{}.csv'.format(file_date), index_col = 0)
cad_keystats = pd.read_csv('cad_keystats_{}.csv'.format(file_date), index_col = 0)
cad_quarterly = pd.read_csv('cad_quarterly_{}.csv'.format(file_date), index_col = 0)

os.chdir('..\\Whales')

file_date = '2018-10-14'

whale_df = pd.read_csv('whales {}.csv'.format(file_date), index_col = 0)

hedgeFunds = whale_df[whale_df.index == 'hedgeFunds'].dropna()
hedgeFunds.index = hedgeFunds.ticker

allFunds = whale_df[whale_df.index == 'allFirms'].dropna()
allFunds.index = allFunds.ticker

os.chdir(main_dir)
#%%

def create_dgi(annual_data):
    dgi_data = annual_data[['totalLiab',
                             'totalStockholderEquity',
                             'longTermInvestments',
                             'shortTermInvestments',
                             'netIncome','operatingIncome',
                             'totalRevenue','dividendsPaid',
                             'investments','Underlying']]
    dgi_data['dividendsPaid'] = -dgi_data['dividendsPaid']

    dgi_data['payoutRatio'] = dgi_data.dividendsPaid/dgi_data.netIncome
    dgi_data['debtEquityRatio'] = dgi_data.totalLiab/dgi_data.totalStockholderEquity
    dgi_data['netMargin'] = dgi_data.netIncome/dgi_data.totalRevenue
    dgi_data['roic'] = dgi_data.operatingIncome/(dgi_data.longTermInvestments + dgi_data.shortTermInvestments)

    dgi_data = dgi_data[['Underlying','payoutRatio','debtEquityRatio','netMargin','roic','dividendsPaid']]
    return dgi_data

# DGI Portion

def dgi_scores(annual_df,keystats, min_dgi_score):
    dgi_df = create_dgi(annual_df).fillna(0).replace(-np.inf,0).replace(np.inf,0)

    dgi_df_scores = pd.DataFrame(columns = ['divGrowth','payoutChange','payoutRatio',
                                             'debtEquityRatio','netMargin','roic'],
                                  index = dgi_df.Underlying.drop_duplicates().tolist()).fillna(0)

    for ticker in dgi_df.Underlying.drop_duplicates():
        curr_dgi = dgi_df[dgi_df.Underlying == ticker].sort_index()
        if len(curr_dgi) - 1:
            if sum(curr_dgi['dividendsPaid'].pct_change() >= 0.02) == len(curr_dgi) -1:
                dgi_df_scores.loc[ticker, 'divGrowth'] = 1
            if sum(curr_dgi.payoutRatio.pct_change() <= 1) == len(curr_dgi) - 1:
                dgi_df_scores.loc[ticker, 'payoutChange'] = 1
            if curr_dgi.payoutRatio.tail(3).mean() <= 0.6:
                dgi_df_scores.loc[ticker, 'payoutRatio'] = 1
            if sum(curr_dgi.debtEquityRatio.tail(3) <= 1.5) == len(curr_dgi) - 1:
                dgi_df_scores.loc[ticker, 'debtEquityRatio'] = 1
            if sum(curr_dgi.netMargin.tail(3) >= 0.07) == len(curr_dgi) - 1:
                dgi_df_scores.loc[ticker, 'netMargin'] = 1
            if sum(curr_dgi.roic.tail(3) >= 0.2) == len(curr_dgi) - 1:
                dgi_df_scores.loc[ticker, 'roic'] = 1

    dgi_df_scores['score'] = dgi_df_scores.sum(axis = 1)
    dgi_df_scores = dgi_df_scores[dgi_df_scores.score >= min_dgi_score].join(keystats[['currentPrice','returnOnAssets',
                                                                                       'returnOnEquity','sector']])

    dgi_df_scores = dgi_df_scores.sort_values('returnOnAssets', ascending = False)
    dgi_df_scores['roaRank'] = range(1,len(dgi_df_scores) + 1)
    dgi_df_scores = dgi_df_scores.sort_values('returnOnEquity', ascending = False)
    dgi_df_scores['roeRank'] = range(1,len(dgi_df_scores) + 1)
    dgi_df_scores['roRank'] = dgi_df_scores.roaRank + dgi_df_scores.roeRank
    dgi_df_scores = dgi_df_scores.sort_values('roRank')
    return dgi_df_scores[['currentPrice','score','roRank','sector']]

os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Single Name Pulls')

#%%
if __name__ == "__main__":
    cad_dgi_scores = dgi_scores(cad_annual, cad_keystats, 4)
    
    us_dgi_score = dgi_scores(us_annual, us_keystats, 5)
    us_dgi_score.head(20).join(us_keystats[['forwardPE','forwardEps','pegRatio']]).sort_values('roRank')
    
    us_dgi_picks = us_dgi_score.head(20).join(us_keystats[['forwardPE',
                                            'forwardEps',
                                            'pegRatio']]).sort_values('roRank').join(hedgeFunds[['numOfFirmsHoldingInTop10',
                                                                                                 'numOfHolders',
                                                                                                 'fundNumPercentChange',
                                                                                                 'fundsCreatingNewPos',
                                                                                                 'fundsAddingPos',
                                                                                                 'fundsClosingPos',
                                                                                                 'fundsReducingPos']]).sort_values('fundNumPercentChange', 
                                                                                                                                   ascending = False)
    
    
    
    datenow = dt.datetime.today().strftime('%Y-%m-%d')
    filename = 'dgi_names {}.xlsx'.format(datenow)
    
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    cad_dgi_scores.to_excel(writer, sheet_name='ca_dgi_scores')
    us_dgi_score.to_excel(writer, sheet_name='us_dgi_scores')
    us_dgi_picks.to_excel(writer, sheet_name='us_dgi_picks')
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    os.chdir(main_dir)