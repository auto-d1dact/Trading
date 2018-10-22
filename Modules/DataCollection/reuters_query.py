# -*- coding: utf-8 -*-
"""
Created on Mon Oct 22 14:15:38 2018

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

#%%

class reuters_query:
    
    def __init__(self, ticker):
        
        overview_url = 'https://www.reuters.com/finance/stocks/overview/' + ticker
        financials_url = 'https://www.reuters.com/finance/stocks/financial-highlights/' + ticker
        analysts_url = 'https://www.reuters.com/finance/stocks/analyst/' + ticker
        
        self.overview = bs(requests.get(overview_url).text, 'lxml')
        self.financials = bs(requests.get(financials_url).text, 'lxml')
        self.analysts = bs(requests.get(analysts_url).text, 'lxml')
        
#%%
ticker = 'AAPL'

stock = reuters_query(ticker)

#%%
overview_raw = stock.overview.find('div', 
                                   {'id': 'content'}).select('div[class*="sectionContent"]')

financials_raw = stock.financials.find('div', 
                                       {'id': 'content'}).select('div[class*="sectionContent"]')

analysts_raw = stock.analysts.find('div', 
                                   {'id': 'content'}).select('div[class*="sectionContent"]')

#%% Overview Tables

def to_float(string):
    val = string.strip().replace(',','').replace('$','').replace('-','')
    if val != '':
        return float(val)
    else:
        return np.nan


if len(overview_raw) != 0:
    
    overall_dict = {}
    
    #### Latest Prices
    header_table = overview_raw[1].select_one('div[id*="headerQuoteContainer"]').select('div[class*="sectionQuote"]')
    
    lastprice = header_table[0].select_one('span[style*="font-size"]')
    lastprice = to_float(lastprice.contents[0])
    
    high52week = header_table[-2].find('span', {'class': "sectionQuoteDetailHigh"})
    high52week = to_float(high52week.contents[0])
    
    low52week = header_table[-1].find('span', {'class': "sectionQuoteDetailLow"})
    low52week = to_float(low52week.contents[0])
    
    overall_dict['LastPrice'] = lastprice
    overall_dict['High52Week'] = high52week
    overall_dict['Low52Week'] = low52week
    
    #### Latest Overview
    overall_table = overview_raw[1].select_one('div[id*="overallRatios"]').find('table')
    
    for row in overall_table.find_all('tr'):
        
        row_cols = row.find_all('td')
        overall_dict[row_cols[0].contents[0].replace(':','').strip()] = to_float(row_cols[-1].find('strong').contents[0])
    
    #### Latest Financials
    overall_fins = overview_raw[1].select_one('div[id*="companyVsIndustry"]').find('table')
    
    for row in overall_fins.find_all('tr'):
        
        row_cols = row.find_all('td')
        
        if len(row_cols) != 0:
            
            fin_field = row_cols[0].contents[0].replace(':','').strip()
            
            overall_dict['{} Stock'.format(fin_field)] = to_float(row_cols[1].contents[0])
            overall_dict['{} Industry'.format(fin_field)] = to_float(row_cols[2].contents[0])
            overall_dict['{} Sector'.format(fin_field)] = to_float(row_cols[3].contents[0])
            
        
    overall_df = pd.DataFrame(overall_dict, index = [ticker])


#%% Financials Tables

if len(financials_raw) != 0:
    
    fins_details = financials_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
    fins_sums = financials_raw[1].select_one('div[class*="column2 gridPanel"]').select('div[class="module"]')
    
    fins_details_dict = {}
    
    for module in fins_details:
        
        detail_table = module.find('table')
        
        if len(detail_table) != 0:
            
            table_title = module.select_one('h3').contents[0].strip()
            
            curr_table_dict = {}
            
            for row in detail_table.find_all('tr'):
                
                if len(row.find_all('th')) != 0:
                    headers = [x.contents[0].strip() for x in row.find_all('th')]
                    
                    if 'revenue' in table_title.lower():
                        headers[0] = 'Year'
                        headers[1] = 'Quarter'
                    continue
                else:
                    curr_cols = row.find_all('td')
                    
                    
            
            #fins_details_dict[table_title]
            break

    

#%%
'''
dt.datetime.strptime(date.strip(), '%b. %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])


dt.datetime.strptime(date.strip(), '%B %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])
'''

