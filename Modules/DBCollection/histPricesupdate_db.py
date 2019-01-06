# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 15:11:48 2018

@author: Fang
"""

import pandas as pd
import numpy as np
from sqlalchemy import *
from sqlalchemy import create_engine
import os
import datetime as dt
import time

module_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(module_dir)

from yahoo_query import *

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

#tsx = pd.read_csv('TSX.csv')['Symbol']#.tolist()
nyse = pd.read_csv('NYSE.csv')['Symbol']#.tolist()
nasdaq = pd.read_csv('NASDAQ.csv')['Symbol']#.tolist()
amex = pd.read_csv('AMEX.csv')['Symbol']#.tolist()

us_names = pd.concat([nyse,nasdaq,amex]).drop_duplicates().values.tolist()


price_db = 'D:\\Price Data'
os.chdir(price_db)

hisprices_engine = create_engine('sqlite:///histprices.db', echo=False)

start_time = time.time()
i = 0
total_length = len(us_names)
for ticker in us_names:
    start_date = dt.datetime(1999,1,1)
    
    try:
        curr_yahoo = yahoo_query(ticker, start_date)
        curr_yahoo.hist_prices_query()

        curr_hist_prices = curr_yahoo.hist_prices.sort_index(ascending = True)
        curr_hist_prices.columns = [x.replace('{}_'.format(ticker),'') for x in curr_hist_prices.columns.tolist()]
        curr_hist_prices['Underlying'] = ticker
    except:
        print('Failed prices for {}'.format(ticker))
        print('{0:.2f}% Completed'.format(i/total_length*100))
        continue

    table_in_db = True

    try:
        query = 'SELECT Date, Underlying FROM historicalPrices WHERE Underlying = "{}"'.format(ticker)
        curr_hist_db = pd.read_sql_query(query, con=hisprices_engine, index_col = 'Date').tail(1)
        curr_hist_db.index = pd.to_datetime(curr_hist_db.index)
    except:
        table_in_db = False

    if table_in_db and len(curr_hist_db) != 0:
        latest_date_in_db = curr_hist_db.index.tolist()[0]
        curr_hist_prices = curr_hist_prices[curr_hist_prices.index > latest_date_in_db]

    curr_hist_prices.to_sql('historicalPrices', con=hisprices_engine, 
                            if_exists='append', index_label = 'Date')
    print('{0:.2f}% Completed'.format(i/total_length*100))
    
print("Completed in %s seconds" % (time.time() - start_time))