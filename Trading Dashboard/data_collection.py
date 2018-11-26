# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 10:16:19 2018

@author: fchen
"""

import pandas as pd
from cboe_fetch import cboe
from vixcentral_fetch import vixcentral
import helper_functions

from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

#%% Scraping Data from different sources

# data retrieval types:
# "full"
# "intraday"
def retrieve_data(retrieve_type, echo = True):
    
    yahoo_tickers = ['^GSPC','^VIX','^VVIX','XLU','XLRE','XLY','XLV',
                     'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
                     'XLP', 'XLE']
    
    intraday_tickers = ['^GSPC','^VIX','^VVIX']
    
    engine = create_engine('sqlite:///marketData.db', echo = echo)
    
    if retrieve_type == "full":
        yahoo_daily_data, yahoo_minute_data = helper_functions.yahoo_batch_quotes(yahoo_tickers,
                                                                                  fetch_daily = True,
                                                                                  fetch_intraday = True,
                                                                                  daysback = 750)
        cboe_data = cboe()
        cboe_data = cboe_data.latest.reset_index().set_index('RetrieveTime')
        vixcentral_data = vixcentral()
        yahoo_daily_data.columns = [x.replace('^','') for x in yahoo_daily_data.columns]
        yahoo_minute_data.columns = [x.replace('^','') for x in yahoo_minute_data.columns]
        
        yahoo_daily_data.to_sql('yahooDaily', con=engine, if_exists='replace',index_label = 'Date')
        yahoo_minute_data.to_sql('yahooMinute', con=engine, if_exists='replace',index_label = 'Date')
        cboe_data.to_sql('cboe', con=engine, if_exists='replace')
        vixcentral_data.daily.to_sql('vixCentral', con=engine, if_exists='replace',index_label = 'Date')
    
    elif retrieve_type == 'intraday':
        yahoo_minute_data = helper_functions.yahoo_batch_quotes(intraday_tickers, 
                                                                fetch_daily = False, 
                                                                fetch_intraday = True)
        yahoo_minute_data.columns = [x.replace('^','') for x in yahoo_minute_data.columns]
        cboe_data = cboe()
        cboe_data = cboe_data.latest.reset_index().set_index('RetrieveTime')
        
        yahoo_minute_data.to_sql('yahooMinute', con=engine, if_exists='replace',index_label = 'Date')
        cboe_data.to_sql('cboe', con=engine, if_exists='replace')
        
    return

#%%
if __name__ == "__main__":
    retrieve_data('full')
