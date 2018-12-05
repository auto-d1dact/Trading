# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 01:23:17 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
import requests
import numpy as np

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

tsx = pd.read_csv('TSX.csv')['Symbol'].tolist()
nyse = pd.read_csv('NYSE.csv')['Symbol'].tolist()
nasdaq = pd.read_csv('NASDAQ.csv')['Symbol'].tolist()
amex = pd.read_csv('AMEX.csv')['Symbol'].tolist()

os.chdir(main_dir)

#%% Functions for Parsing Reuters html
def to_float(string):
            val = string.strip().replace(',','').replace('$','').replace('--','')
            if val != '':
                return float(val)
            else:
                return np.nan

#%%

def create_vol_df(curr_vol, day_lookback):
    try:
        curr_vol_table = curr_vol.find('div', {'id':'indicators-container'}).find('table')
    except:
        curr_vol_table = None
    
    if curr_vol_table != None:
        
        curr_title = '{}-Day'.format(day_lookback)
        curr_vol_dict = {'Field': [], curr_title : []}
        
        for row in curr_vol_table.find_all('tr'):
            
            cols = [col.text.strip() for col in row.find_all('td')]
            if len(cols) > 1:
                curr_vol_dict['Field'].append(cols[0])
                curr_vol_dict[curr_title].append(to_float(cols[1]))
        curr_vol_df = pd.DataFrame(curr_vol_dict).set_index('Field')
        
        return curr_vol_df
        

#%%
class alphaquery:
    
    def __init__(self, ticker):
        
        vol_df_list = []
        
        for day_lookback in [10, 20, 30, 60, 90, 120, 150, 180]:
            vol_url = 'https://www.alphaquery.com/stock/{0}/volatility-option-statistics/{1}-day/historical-volatility'.format(ticker,day_lookback)
            s = requests.session()
        
            curr_vol = bs(s.get(vol_url).text, 'lxml')
        
            s.cookies.clear()
            
            vol_df_list.append(create_vol_df(curr_vol, day_lookback))
            
            
        self.vol_df = pd.concat(vol_df_list, axis = 1)
        self.vol_df['Underlying'] = ticker
        
        