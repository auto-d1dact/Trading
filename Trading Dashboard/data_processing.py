# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 11:25:37 2018

@author: fchen
"""

import pandas as pd
import helper_functions
import data_collection
import datetime as dt
import numpy as np
import statsmodels.api as sm

from sqlalchemy import *
from sqlalchemy import create_engine

import sys

#%%

# Initial Data Retreival and Subsetting

def process_data(update = 'full', echo = True):
    engine = create_engine('sqlite:///marketData.db', echo = echo)
    
    data_collection.retrieve_data(update, echo = echo)
    
    try:
        # Try to retrieve data from database if tables exist
        vixcentral_data = pd.read_sql_query('SELECT Date, F1, F2, F3, F4, F5, F6, F7, F8, F9 FROM vixCentral', 
                                            con=engine, index_col = 'Date')
        vixcentral_data.index = pd.to_datetime(vixcentral_data.index)
        
        cboe_data = pd.read_sql_query('SELECT * FROM cboe', 
                                      con=engine)[['RetrieveTime','index','Expiration','Last']]
        cboe_data.RetrieveTime = pd.to_datetime(cboe_data.RetrieveTime)
        cboe_data.Expiration = pd.to_datetime(cboe_data.Expiration)
        
        daily_data = pd.read_sql_query('SELECT * FROM yahooDaily', con=engine, index_col = 'Date')
        daily_data.index = pd.to_datetime(daily_data.index)
        daily_data = daily_data[list(filter(lambda x: 'close' in x, daily_data.columns.tolist()))]
        daily_data.columns = [x.replace('_close','') for x in daily_data.columns]
        
        minute_data = pd.read_sql_query('SELECT * FROM yahooMinute', con=engine, index_col = 'Date')
        minute_data.index = pd.to_datetime(minute_data.index)
        minute_data = minute_data[list(filter(lambda x: 'close' in x, minute_data.columns.tolist()))]
        minute_data.columns = [x.replace('_close','') for x in minute_data.columns]
        
    except:
        # If nothing found, initial remote data collection must be run first
        print('Database has not been updated, please execute remote first')
        return
    
    else:
        # Otherwise, continue to process data 
        
        ####### Creating Dataframes for plotting - 5 total plots
        
        # Industry Correlation Dataframe for Plotting
        market_correlations = helper_functions.check_mkt_corr(daily_data, 60, 100)[0]
        
        # Intraday SPX Dollar Vol and SMA Plotting
        spx_intraday = helper_functions.stats(minute_data[['GSPC']])[['Last','Dollar Std Move','SMA 5','SMA 20']]
        
        # Daily SPX Plotting
        spx_daily = daily_data[['GSPC']]
        spx_daily['SMA 200'] = spx_daily.rolling(200).mean()
        spx_daily['SMA 20'] = spx_daily['GSPC'].rolling(20).mean()
        spx_daily = spx_daily.dropna()
        
        contango_weights = helper_functions.maturities(dt.datetime.now().date())
        
        # VIX Term Structure for Plotting
        previous_vix_term = daily_data[['VIX']].join(vixcentral_data.tail(1),
                                      how = 'inner').drop_duplicates().T
                                      
        if previous_vix_term.shape[1] == 0:
            previous_vix_term = daily_data[['VIX']].dropna().tail(1)
            previous_vix_term.index = vixcentral_data.tail(1).index
            previous_vix_term = previous_vix_term.join(vixcentral_data.tail(1),
                                      how = 'inner').drop_duplicates().T
        
        current_vix_spot = minute_data['VIX'].dropna().tail(1).values[0]
        
        if contango_weights[0] == 1:
            spot_date = dt.datetime.now().date() - dt.timedelta(days = 1)
        else:
            spot_date = dt.datetime.now().date()
        
        current_vix_spot = pd.DataFrame({'index':'VIX',
                                         'Expiration': spot_date,
                                         'Last':current_vix_spot},index = [0])
        current_vix_term = cboe_data[cboe_data.RetrieveTime == max(cboe_data.RetrieveTime)][['index','Expiration','Last']]
        current_vix_term = current_vix_spot.append(current_vix_term).set_index('index')
        
        vix_term = previous_vix_term.join(current_vix_term).dropna()
        vix_term.Expiration = pd.to_datetime(vix_term.Expiration)
        vix_term = vix_term.set_index('Expiration')
        vix_term.columns = ['PreviousStructure','CurrentStructure']
        vix_term = vix_term.apply(pd.to_numeric)
        
        ############ Creating Summary Tables for Display
        
        # Table beside vix term structure
        hv_data = pd.read_sql_query('SELECT Date, GSPC_open, GSPC_close FROM yahooDaily', con=engine, index_col = 'Date')
        hv_data = hv_data.tail(50)
        hv_data['CloseReturn'] = np.log(hv_data.GSPC_close) - np.log(hv_data.GSPC_close.shift(1))
        hv_data['OpenCloseReturn'] = np.log(hv_data.GSPC_close) - np.log(hv_data.GSPC_open)
        hv_data['HV'] = hv_data['CloseReturn'].rolling(30).std()*np.sqrt(252)
        hv_data['Intraday HV'] = hv_data['OpenCloseReturn'].rolling(30).std()*np.sqrt(252)
        hv_data = hv_data.tail(1)[['HV','Intraday HV']].reset_index(drop = True)
        
        term_ratios = vix_term.pct_change(-1).dropna().head(2) + 1
        term_ratios = pd.DataFrame(term_ratios.multiply(contango_weights, axis = 'rows').sum()).T
        term_ratios.columns = ['Contango Prior','Contango Now']
        
        latest_IV = minute_data[['VIX','VVIX']].tail(1).reset_index(drop = True)
        
        vol_summary_table = pd.concat([latest_IV, term_ratios, hv_data], axis = 1).T
        vol_summary_table.columns = ['Values']
        
        # Table beside SPX Intraday
        spx_trend = helper_functions.spx_trend(minute_data)
        
        spx_latest_prices = hv_data = pd.read_sql_query('SELECT Date, GSPC_open, GSPC_close, GSPC_high, GSPC_low FROM yahooMinute', 
                                                        con=engine, index_col = 'Date')
        spx_open = spx_latest_prices['GSPC_open'].head(1).values[0]
        spx_last = spx_latest_prices['GSPC_close'].tail(1).values[0]
        spx_high = max(spx_latest_prices['GSPC_close'])
        spx_low = min(spx_latest_prices['GSPC_close'])
        
        spx_price_action = pd.DataFrame({'SPX Intraday Summaries': [spx_open,
                                                                    spx_high,
                                                                    spx_low,
                                                                    spx_last]},
                                         index = ['SPX Open','SPX High','SPX Low','SPX Last'])
        spx_trend_table = pd.concat([spx_price_action,spx_trend],axis = 0)
        
        return spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations


#%%

def main(*argv):
    # main can take up to two arguments
    # one argument for -source=local or -source=remote
    # second argument for retrieve only minutely or full data retrieval
    
    # For debugging echo is on
    echo = False
    
    v = []
    
    for i, arg in enumerate(argv):
        if i == 0:
            v.append(arg.split('=')[1])
        else:
            v.append(arg)
        
    if len(v) > 0:
        if v[0] == 'remote':
            data_collection.retrieve_data('full', echo)
            print('Database Updated Remotely')
        elif v[0] == 'local':
            print('Using Local Data if Exists')
        elif len(v) > 1:
            data_collection.retrieve_data(v[1], echo)
            print('Database Updated Remotely')
    else:
        print('Using Local Data if Exists')
    
    try:
        spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations = process_data(echo)
    except:
        print('Database has not been updated, please execute remote first')
    else:
        print(spx_trend_table)
        print(vol_summary_table)
        
        return spx_trend_table, vol_summary_table

#%%

if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            main(sys.argv[1])
        else:
            main(sys.argv[1], sys.argv[2])
    except:
        main()