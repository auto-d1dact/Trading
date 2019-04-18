# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 10:06:18 2018

@author: Fang
"""

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import datetime as dt
import requests

def yahoo_earnings_table(table):
    tickers = []
    company_names = []
    call_times = []
    eps_estimates = []
        
    for row in table.find_all('tr'):
        
        i = 1
        for col in row.find_all('td')[:4]:
            
            field = str(col).split('</td>')[-2]
            if i == 1:
                ticker = field.replace('</a>','').split('>')[-1]
                tickers.append(ticker)
                
            elif i == 2:
                name = field.split('>')[-1]
                company_names.append(name)
            elif i == 3:
                call_time = field.replace('</span>','').split('>')[-1]
                call_times.append(call_time)
            else:
                estimate = field.split('>')[-1]
                eps_estimates.append(estimate)
            i += 1
    return pd.DataFrame({'Name': company_names,
                         'Call Time': call_times,
                         'EPS Estimate': eps_estimates}, index = tickers)
    
def date_earnings(earnings_date_string):
    #earnings_date_string = '2018-10-25'
    earnings_url = 'https://finance.yahoo.com/calendar/earnings?day={}'.format(earnings_date_string)
    
    site = requests.get(earnings_url)
    
    soup = bs(site.text, 'lxml')
    
    table_div = soup.find('div', {'id':'fin-cal-table'})
    total_names = str(list(filter(lambda x: 'results' in str(x), 
                                  table_div.find('h3').findAll('span')))[-1]).replace('</span>','').split('results')[-2]
    total_names = int(total_names.strip().split(' ')[-1])
    
    table = table_div.find('table', {'class': 'W(100%)'}).find('tbody')
    
    initial_table = yahoo_earnings_table(table)
    
    if total_names > 100:
        
        all_earnings = []
        for offset in range(1, total_names//100 + 1):
            offset_link = earnings_url + "&offset={}&size=100".format(offset*100)
            site = requests.get(offset_link)
            soup = bs(site.text, 'lxml')
            table_div = soup.find('div', {'id':'fin-cal-table'})
            table = table_div.find('table', {'class': 'W(100%)'}).find('tbody')
            
            all_earnings.append(yahoo_earnings_table(table))
        
        earnings = pd.concat([initial_table] + all_earnings, axis = 0)
    
    else:
        earnings = initial_table
    
    return earnings