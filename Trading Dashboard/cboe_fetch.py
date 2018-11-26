# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 21:07:08 2018

@author: fchen
"""

import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import numpy as np

#%%

def cboe_vix():
    delayed_cboe_url = 'http://www.cboe.com/delayedquote/futures-quotes'
    soup = bs(requests.get(delayed_cboe_url).text, "lxml")
    # tables = soup.find_all('table')
    
    divs = soup.select('div[class*="wrap-columns-inner"]')
    vix_div = None
    for d in divs:
        header = d.find('div', {'class': 'left'})
        if header != None and 'VIX' in header.text:
            vix_div = d
    
    if vix_div != None:
        vix_table = vix_div.find('table')
    
    intraday_vix_dict = {}
    headers = []
    
    for row in vix_table.find_all('tr'):
        
        if row.find_all('th') != None:
            for col in row.find_all('th'):
                header = col.text.strip()
                headers.append(header)
                # Initializing dictionary
                intraday_vix_dict[header] = []
        
        if row.find_all('td') != None:
            for i, col in enumerate(row.find_all('td')):
                attribute = col.text.strip()
                if headers[i] == 'Expiration':
                    intraday_vix_dict[headers[i]].append(dt.datetime.strptime(attribute, '%m/%d/%Y'))
                elif headers[i] == 'Symbol':
                    intraday_vix_dict[headers[i]].append(attribute)
                else:
                    try:
                        intraday_vix_dict[headers[i]].append(float(attribute))
                    except:
                        intraday_vix_dict[headers[i]].append(np.nan)
    
    cboe_df = pd.DataFrame(intraday_vix_dict)
    cboe_df['RetrieveTime'] = dt.datetime.utcnow().replace(second=0, microsecond=0)
    return cboe_df.set_index('Symbol')

#%%

def filter_monthlies(cboe_df):
    filtered_df = cboe_df[~cboe_df.index.str.contains('VX')].sort_values('Expiration', 
                          ascending = True)
    return filtered_df

class cboe:
    def __init__(self, daysback = 365, minback = 120):
        self.latest = filter_monthlies(cboe_vix())
        self.latest.index = ['F{}'.format(x) for x in range(1,len(self.latest) + 1)]
        
    def __repr__(self):
        return str(self.latest)
    
    def __str__(self):
        return str(self.latest)

#%%

# Old intraday Scrape
#def intraday_vix_data():
#
#    delayed_cboe_url = 'http://www.cboe.com/delayedquote/futures-quotes'
#    soup = bs(requests.get(delayed_cboe_url).text, "lxml")
#
#    tables = soup.find_all('table')
#    
#    for table in tables:
#        if 'VX' in str(table):
#            table = table
#            break
#        else:
#            table = []
#    
#    if table == []:
#        return "NA"
#
#    intraday_vx_dict = {}
#
#    intraday_vx_dict['Symbol'] = []
#    intraday_vx_dict['Expiration'] = []
#    intraday_vx_dict['Last'] = []
#    intraday_vx_dict['Change'] = []
#    intraday_vx_dict['High'] = []
#    intraday_vx_dict['Low'] = []
#    intraday_vx_dict['Settlement'] = []
#    intraday_vx_dict['Volume'] = []
#    intraday_vx_dict['Int'] = []
#
#    i = 1
#    for row in table.find_all('tr'):
#        # Individual row stores current row item and delimits on '\n'
#        individual_row = str(row).split('\n')
#        curr_items = list(map(lambda x: x.replace('\r',''),
#                              list(map(lambda x: x.replace(' ', ''), 
#                                       list(filter(lambda x: '<' not in x, 
#                                                   individual_row))))))
#        if i == 1:
#            i += 1
#            continue
#        intraday_vx_dict['Symbol'].append(curr_items[0])
#        intraday_vx_dict['Expiration'].append(dt.datetime.strptime(curr_items[1], '%m/%d/%Y'))
#        intraday_vx_dict['Last'].append(float(curr_items[2]))
#        intraday_vx_dict['Change'].append(float(curr_items[3]))
#        intraday_vx_dict['High'].append(float(curr_items[4]))
#        intraday_vx_dict['Low'].append(float(curr_items[5]))
#        intraday_vx_dict['Settlement'].append(float(curr_items[6]))
#        intraday_vx_dict['Volume'].append(float(curr_items[7]))
#        intraday_vx_dict['Int'].append(int(curr_items[8]))
#
#    intraday_vx = pd.DataFrame(intraday_vx_dict)[['Symbol', 'Expiration', 'Last', 
#                                                  'Settlement', 'Change', 'High', 
#                                                  'Low', 'Int', 'Volume']]
#
#    intraday_vx = intraday_vx[~intraday_vx["Symbol"].str.contains('VX')].reset_index()[intraday_vx.columns]
#    
#    return intraday_vx