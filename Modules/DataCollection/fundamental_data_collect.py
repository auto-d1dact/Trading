# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 21:22:30 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt

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
def pull_data(ticker):
    yahoo_data = yahoo_query(ticker,dt.datetime(2018,1,1))
    yahoo_data.full_info_query()
    earnings_info_quarter = yahoo_data.earnings_quarterly.join(yahoo_data.cashFlowStatementQuarter).join(yahoo_data.incomeStatementQuarter.drop(['netIncome','maxAge'],
                                                                                                                                      axis = 1),
                                                                                               rsuffix='_income').join(yahoo_data.balanceSheetQuarter,
                                                                                                                       rsuffix = '_balance')
    
    annual_info = yahoo_data.cashFlowStatementAnnual.join(yahoo_data.incomeStatementAnnual.drop(['netIncome','maxAge'],
                                                                                                  axis = 1),
                                                           rsuffix='_income').join(yahoo_data.balanceSheetAnnual,
                                                                                   rsuffix = '_balance')
    annual_info['earnings'] = yahoo_data.earnings_annual.sort_index(ascending = False)['earnings'].tolist()
#    earnings_info['earnBeatsBefore'] = 0
#    earnings_info['earnMissBefore'] = 0
#    
#    for idx, row in earnings_info.iterrows():
#        earnings_info.loc[idx,'earnBeatsBefore'] = len(earnings_info[(earnings_info.index <= idx) & (earnings_info.epsDifference > 0)])
#        earnings_info.loc[idx,'earnMissBefore'] = len(earnings_info[(earnings_info.index <= idx) & (earnings_info.epsDifference <= 0)])
#        earnings_info = earnings_info.shift(1)
#        
#    earnings_moves = past_earnings(ticker).sort_index()
#    earnings_moves = earnings_moves[(earnings_moves.index > min(yahoo_data.earnings_history.index) - dt.timedelta(days = 92)) &
#                                    (earnings_moves.index <= max(yahoo_data.earnings_history.index))].sort_index()
#    
#    earnings_df = pd.concat([earnings_info.reset_index(), 
#                             earnings_moves.reset_index()], axis = 1)
    earnings_info_quarter.columns = ['quarter' if col == 'index' else col for col in earnings_info_quarter.columns.tolist()]
    earnings_info_quarter['Underlying'] = ticker
    annual_info.columns = ['year' if col == 'index' else col for col in annual_info.columns.tolist()]
    annual_info['Underlying'] = ticker
    
    #separate df for current key measures
    keyMetrics = yahoo_data.profile.join(yahoo_data.keyStats).join(yahoo_data.finData, rsuffix = '_finData')

    return (earnings_info_quarter, annual_info, keyMetrics)

def download_yahoo_data(ticker_list, retries = 10):

    earnings_lst = []
    annual_lst = []
    keyStats_lst = []

    item_counter = 0
    total_length = len(ticker_list)
    failed_list = []

    for ticker in ticker_list:
        try:
            curr_earnings, curr_annual_info, curr_keyStats = pull_data(ticker)
            earnings_lst.append(curr_earnings)
            annual_lst.append(curr_annual_info)
            keyStats_lst.append(curr_keyStats)
            print('Accepted: {}'.format(ticker))
        except:
            for i in range(retries):
                try:
                    curr_earnings, curr_annual_info, curr_keyStats = pull_data(ticker)
                    earnings_lst.append(curr_earnings)
                    annual_lst.append(curr_annual_info)
                    keyStats_lst.append(curr_keyStats)
                    print('Accepted: {}'.format(ticker))
                except:
                    continue
            print('Failed: {}'.format(ticker))
            failed_list.append(ticker)

        item_counter += 1
        print('{0:.2f}% Completed'.format(item_counter/total_length*100))
        print('{} total failures'.format(len(failed_list)))

    earnings_df = pd.concat(earnings_lst, axis = 0)
    annual_df = pd.concat(annual_lst, axis = 0)
    earnings_df = earnings_df[earnings_df.columns]
    keyStats_df = pd.concat(keyStats_lst, axis = 0)

    return earnings_df, annual_df, keyStats_df, failed_list


#%%
if __name__ == '__main__':
       
    earnings_df, annual_df, keyStats_df, failed_list = download_yahoo_data(us_names, retries = 3)
    os.chdir('..\\')
    os.chdir('..\\')
    os.chdir('..\\Data\\Historical Queries\\US')

    datenow = dt.datetime.today().strftime('%Y-%m-%d')
    earnings_df.to_csv('us_quarterly_{}.csv'.format(datenow))
    annual_df.to_csv('us_annual_{}.csv'.format(datenow))
    keyStats_df.to_csv('us_keystats_{}.csv'.format(datenow))
    
    earnings_df, annual_df, keyStats_df, failed_list = download_yahoo_data(cad_names, retries = 3)
    os.chdir('..\\')
    os.chdir('..\\')
    os.chdir('..\\Data\\Historical Queries\\CAD')
    
    datenow = dt.datetime.today().strftime('%Y-%m-%d')
    earnings_df.to_csv('cad_quarterly_{}.csv'.format(datenow))
    annual_df.to_csv('cad_annual_{}.csv'.format(datenow))
    keyStats_df.to_csv('cad_keystats_{}.csv'.format(datenow))
    os.chdir(main_dir)
    
    del earnings_df, annual_df, keyStats_df, failed_list, datenow