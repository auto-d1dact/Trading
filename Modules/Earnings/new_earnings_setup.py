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

e_main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\Earnings'

os.chdir(e_main_dir)

from earnings_prediction import *

# Initializing Fin Statement Data
os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection')

from fundamental_data_collect import download_yahoo_data
from yahoo_query import *

os.chdir(e_main_dir)

from yahoo_earnings import *
from finstatement_cleaning import *

#%%

def predict_earnings(date_lookup, file_date, return_bounds, test_size):
    latest_earnings = date_earnings(date_lookup)
    
    latest_earnings_names = latest_earnings.index.tolist()
    
    earnings_df, annual_df, keyStats_df, failed_list = download_yahoo_data(latest_earnings_names, retries = 3)
    
    annual_df['index'] = pd.to_datetime(annual_df.index)
    
    earnings_df['index'] = pd.to_datetime(earnings_df.index)
    
    annual_df = annual_df.replace(0, np.nan)
    earnings_df = earnings_df.replace(0, np.nan)
    
    
    fin_statements = create_finstatements(annual_df, earnings_df, keyStats_df, tree_input = False)
    earnings_rets = earnings_stock_returns(fin_statements)
    
    
    fin_statements = fin_statements[fin_statements['index'] == max(fin_statements['index']).to_pydatetime()]
    fin_statements['index'] = dt.datetime.strptime(date_lookup, '%Y-%m-%d')
    fin_statements['index'] = pd.to_datetime(fin_statements['index'])
    
    earnings_rets = earnings_rets[earnings_rets.index == dt.datetime.strptime(date_lookup, '%Y-%m-%d')]
    
    earnings_rets['index'] = pd.to_datetime(earnings_rets.index)
    
    cleaned_data = pd.merge(fin_statements, earnings_rets,  how='inner', 
                                left_on=['index','Underlying'], right_on = ['index','Underlying'])
        
    cleaned_data = cleaned_data.replace([np.inf, -np.inf], np.nan)
    
    cleaned_data['alpha52WeekVsIndustry'] = cleaned_data['Stock52WeekReturn'] - cleaned_data['Industry52WeekReturn']
    cleaned_data['alpha52WeekVsMarket'] = cleaned_data['Stock52WeekReturn'] - cleaned_data['SPY52WeekReturn']
   
    sector_columns = ['sector_Basic Materials',
                      'sector_Consumer Cyclical', 'sector_Consumer Defensive',
                      'sector_Energy', 'sector_Financial Services', 'sector_Healthcare',
                      'sector_Industrials', 'sector_Real Estate', 'sector_Technology',
                      'sector_Utilities']
    
    inputCols = ['current_ratio_quarterly',
                     'total_debt_equity_ratio_quarterly',
                     'day_payable_outstanding_quarterly',
                     'total_liabilities_total_assets_quarterly', 'gross_margin_quarterly',
                     'operating_margin_quarterly', 'net_profit_margin_quarterly',
                     'changeInCash_quarterly', 'changeToLiabilities_quarterly',
                     'changeToNetincome_quarterly', 'changeToOperatingActivities_quarterly',
                     'current_ratio_annual', 'total_debt_equity_ratio_annual',
                     'day_payable_outstanding_annual',
                     'total_liabilities_total_assets_annual', 'gross_margin_annual',
                     'operating_margin_annual', 'net_profit_margin_annual',
                     'changeInCash_annual', 'changeToLiabilities_annual',
                     'changeToNetincome_annual', 'changeToOperatingActivities_annual',
                     'current_ratio_change', 'total_debt_equity_ratio_change',
                     'day_payable_outstanding_change',
                     'total_liabilities_total_assets_change', 'gross_margin_change',
                     'operating_margin_change', 'net_profit_margin_change',
                     'changeInCash_change', 'changeToLiabilities_change',
                     'changeToNetincome_change', 'changeToOperatingActivities_change','IndustryBeta',
                     'MarketBeta', 'alpha52WeekVsIndustry',
                     'alpha52WeekVsMarket']
        
    X_df = pd.concat([cleaned_data[inputCols], pd.get_dummies(cleaned_data[['sector']])], axis = 1).fillna(0)
    
    if len(X_df.columns) < 47:
        for missing_sector in list(filter(lambda x: x not in X_df.columns.tolist(), sector_columns)):
            X_df[missing_sector] = 0
            
    
    os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\Historical Queries\\Stock Prices')
        
    clf = clf_predict_earnings(file_date, return_bounds, testsize = test_size)
    
    predictions = cleaned_data[['Underlying','sector','CallTime','IndustryBeta',
                                'MarketBeta', 'Stock52WeekReturn', 'SPY52WeekReturn',
                                'Industry52WeekReturn']]
    
    predictions['Expected Return'] = return_bounds*clf.predict(X_df)
    
    os.chdir(e_main_dir)
    
    return predictions, earnings_rets