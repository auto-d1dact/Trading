# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 19:55:47 2018

@author: fchen
"""

import datetime as dt
import pandas as pd
import json
import urllib.request as urlreq

# Function for pulling historical prices from yahoo
def prices(yahoo_url):
    
    try:
        with urlreq.urlopen(yahoo_url) as url:
            data = json.loads(url.read().decode())
            hist_prices = pd.DataFrame(data['chart']['result'][0]['indicators']['quote'][0],
                                       index = [dt.datetime.utcfromtimestamp(int(x)) for x in 
                                                data['chart']['result'][0]['timestamp']])
        return hist_prices
    
    except:
        return 'NA'

# Class for yahoo historical stock data
class yahoo:
    
    # Initializing yahoo class with ticker
    # and creating relevant URL api calls to 
    # query relevant yahoo data
    
    def __init__(self, ticker, fetch_daily = True, fetch_intraday = True, daysback = 365, minback = 120):
        
        unix_startdate = int((dt.datetime.today() - dt.timedelta(days = daysback)).timestamp())
        unix_enddate = int(dt.datetime.today().timestamp())
        
        self.ticker = ticker 
        
        minute_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1m'.format(self.ticker)
        hist_price_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&period1={1}&period2={2}&interval=1d'.format(self.ticker,unix_startdate,unix_enddate)
        
        # Storing daily prices
        if fetch_daily:
            self.daily = prices(hist_price_url)
            self.daily.columns = ["{0}_{1}".format(ticker, x) for x in self.daily.columns]
        
        # Storing minute prices
        if fetch_intraday:
            self.minute = prices(minute_url)
            self.minute.columns = ["{0}_{1}".format(ticker, x) for x in self.minute.columns]
        return
    
    def __repr__(self):
        repr_df = self.daily.pct_change().tail(1)
        repr_df.index = ['Last Day']
        return str(repr_df[["{}_close".format(self.ticker)]])
    
    def __str__(self):
        str_df = self.daily.pct_change().tail(1)
        str_df.index = ['Last Day']
        return str(str_df[["{}_close".format(self.ticker)]])