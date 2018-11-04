# -*- coding: utf-8 -*-
"""
Created on Mon Oct 22 14:15:38 2018

@author: Fang

Update Reuters data every Saturday Morning
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

def overview_tables(overview_raw, ticker):
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
            
        
    return pd.DataFrame(overall_dict, index = [ticker])

##### Revenue table parsing
def revenues_eps_table(revenues_eps_module, ticker):

    detail_table = revenues_eps_module.find('table')
            
    if len(detail_table) != 0:
        
        table_title = revenues_eps_module.select_one('h3').contents[0].strip()
        
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.contents[0].strip().replace('*','') for x in row.find_all('th')]
                
                if 'revenue' in table_title.lower():
                    headers[0] = 'Year'
                    headers[1] = 'Quarter'
                    
                for header in headers:
                    curr_table_dict[header] = []
                continue
            else:
                curr_cols = row.find_all('td')
                
                if len(curr_cols) == 4:
                    curr_year = curr_cols[0].text.split('\xa0')[-1]
                    curr_quarter = curr_cols[1].text
                    curr_rev = curr_cols[2].text
                    curr_eps = curr_cols[3].text
                else:
                    curr_quarter = curr_cols[0].text
                    curr_rev = curr_cols[1].text
                    curr_eps = curr_cols[2].text
                
                if len(curr_quarter.split(' ')) < 2:
                    curr_quarter = curr_quarter + " '" + curr_year[-2:]
                
                curr_table_dict[headers[0]].append(curr_year)
                curr_table_dict[headers[1]].append(curr_quarter)
                curr_table_dict[headers[2]].append(to_float(curr_rev))
                curr_table_dict[headers[3]].append(to_float(curr_eps))
                
        revenue_eps_df = pd.DataFrame(curr_table_dict)
        revenue_eps_df['Underlying'] = ticker
        
        return revenue_eps_df

######### Consensus Table Parsing
def consensus_ests(consensus_est_module, ticker):
    detail_table = consensus_est_module.find('table')
    
    if len(detail_table) != 0:
        curr_table_dict_sales = {}
        curr_table_dict_earnings = {}
        curr_table_dict_lt = {}
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.contents[0].strip().replace('*','').replace('#','Num').replace('\xa0',' ') for x in row.find_all('th')]
                headers[0] = 'ReportDate'
                for header in headers:
                    curr_table_dict_sales[header] = []
                    curr_table_dict_earnings[header] = []
                    curr_table_dict_lt[header] = []
                    
                continue
            else:
                curr_cols = row.find_all('td')
                
                if len(curr_cols) == 1:
                    curr_table_title = curr_cols[0].text
                    continue
                
                curr_cols = [x.text.replace('\xa0',' ') for x in curr_cols]
                
                if 'sales' in curr_table_title.lower():
                    curr_table_dict_sales[headers[0]].append(curr_cols[0])
                    curr_table_dict_sales[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict_sales[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict_sales[headers[3]].append(to_float(curr_cols[3]))
                    curr_table_dict_sales[headers[4]].append(to_float(curr_cols[4]))
                    curr_table_dict_sales[headers[5]].append(to_float(curr_cols[5]))
                elif 'LT' not in curr_cols[0]:
                    curr_table_dict_earnings[headers[0]].append(curr_cols[0])
                    curr_table_dict_earnings[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict_earnings[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict_earnings[headers[3]].append(to_float(curr_cols[3]))
                    curr_table_dict_earnings[headers[4]].append(to_float(curr_cols[4]))
                    curr_table_dict_earnings[headers[5]].append(to_float(curr_cols[5]))
                else:
                    curr_table_dict_lt[headers[0]].append(curr_cols[0])
                    curr_table_dict_lt[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict_lt[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict_lt[headers[3]].append(to_float(curr_cols[3]))
                    curr_table_dict_lt[headers[4]].append(to_float(curr_cols[4]))
                    curr_table_dict_lt[headers[5]].append(to_float(curr_cols[5]))
        sales_ests = pd.DataFrame(curr_table_dict_sales)
        sales_ests['Underlying'] = ticker
        earnings_ests = pd.DataFrame(curr_table_dict_earnings)
        earnings_ests['Underlying'] = ticker
        LTgrowth_ests = pd.DataFrame(curr_table_dict_lt)
        LTgrowth_ests['Underlying'] = ticker
        
        return sales_ests, earnings_ests, LTgrowth_ests
    
########## Standard Table Parsing
def standard_fin_table(module, ticker):
    detail_table = module.find('table')
    
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.text.replace('\xa0',' ') for x in row.find_all('th')]
                headers[0] = 'Field'
                for header in headers:
                    curr_table_dict[header] = []
                    
                continue
            else:
                curr_cols = [x.text.strip() for x in row.find_all('td')]
                
                if len(curr_cols) != 1:
                    curr_table_dict[headers[0]].append(curr_cols[0])
                    curr_table_dict[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict[headers[3]].append(to_float(curr_cols[3]))
        df = pd.DataFrame(curr_table_dict)
        df['Underlying'] = ticker
        return df

######### Performance Table Parsing
def performance_table(performance_module, ticker):
    
    detail_table = performance_module.find('table')
        
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.text.replace('\xa0',' ').replace('%','Percent').replace(' ','') for x in row.find_all('th')]
                for header in headers:
                    curr_table_dict[header] = []
                    
                continue
            else:
                curr_cols = [x.text.strip() for x in row.find_all('td')]
                
                if len(curr_cols) != 1:
                    curr_table_dict[headers[0]].append(curr_cols[0])
                    curr_table_dict[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict[headers[3]].append(to_float(curr_cols[3]))
                    curr_table_dict[headers[4]].append(to_float(curr_cols[4]))
    
        performance_df = pd.DataFrame(curr_table_dict)
        performance_df['Underlying'] = ticker
        return performance_df
    
    
########### Institutional Holdings Parse
def institution_holding_table(institution_holdings_module, ticker):
    detail_table = institution_holdings_module.find('table')
            
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            curr_cols = [x.text.strip().replace('%','Percent').replace('#','Num').replace(':','') for x in row.find_all('td')]
            curr_table_dict[curr_cols[0]] = [to_float(curr_cols[1].replace('Percent',''))]
    
        institution_df = pd.DataFrame(curr_table_dict)
        institution_df.index = [ticker]
        return institution_df
    
########## Recommendation Table Parse
def recommendation_table(recommendation_module, ticker):
    detail_table = recommendation_module.find('table')
                
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.text.replace('\xa0',' ').replace('%','Percent').replace(' ','') for x in row.find_all('th')]
                for header in headers:
                    curr_table_dict[header] = []
                        
                continue
            else:
                curr_cols = [x.text.strip() for x in row.find_all('td')]
                if len(curr_cols) != 1:
                    curr_table_dict[headers[0]].append(curr_cols[0])
                    curr_table_dict[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict[headers[2]].append(curr_cols[2])
                    curr_table_dict[headers[3]].append(curr_cols[3])
    
        recommendation_df = pd.DataFrame(curr_table_dict)
        recommendation_df.index = [ticker]
        return recommendation_df

########### Analyst Recommendations Table Parse
def analyst_rec_table(analyst_recommendation_module, ticker):
    detail_table = analyst_recommendation_module.find('table')
                    
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.text.replace('\xa0',' ').replace('%','Percent').replace(' ','') for x in row.find_all('th')]
                for header in headers:
                    curr_table_dict[header] = []
                        
                continue
            else:
                curr_cols = [x.text.strip() for x in row.find_all('td')]
                if len(curr_cols) != 1:
                    curr_table_dict[headers[0]].append(curr_cols[0])
                    curr_table_dict[headers[1]].append(to_float(curr_cols[1]))
                    curr_table_dict[headers[2]].append(to_float(curr_cols[2]))
                    curr_table_dict[headers[3]].append(to_float(curr_cols[3]))
                    curr_table_dict[headers[4]].append(to_float(curr_cols[4]))
    
        analyst_recommendation_df = pd.DataFrame(curr_table_dict)
        analyst_recommendation_df.index = [ticker]
        return analyst_recommendation_df

#%%
class reuters_query:
    
    def __init__(self, ticker):
        
        overview_url = 'https://www.reuters.com/finance/stocks/overview/' + ticker
        financials_url = 'https://www.reuters.com/finance/stocks/financial-highlights/' + ticker
        analysts_url = 'https://www.reuters.com/finance/stocks/analyst/' + ticker
        
        self.overview = bs(requests.get(overview_url).text, 'lxml')
        self.financials = bs(requests.get(financials_url).text, 'lxml')
        self.analysts = bs(requests.get(analysts_url).text, 'lxml')
        
        overview_raw = self.overview.find('div', 
                                   {'id': 'content'}).select('div[class*="sectionContent"]')

        financials_raw = self.financials.find('div', 
                                               {'id': 'content'}).select('div[class*="sectionContent"]')
        
        analysts_raw = self.analysts.find('div', 
                                           {'id': 'content'}).select('div[class*="sectionContent"]')
        
        
        # Overview Tables
        
        if len(overview_raw) != 0:                
            self.overall_df = overview_tables(overview_raw, ticker)
        
        # Financial Ratio Tables
        if len(financials_raw) != 0:
            fins_details = financials_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
            fins_sums = financials_raw[1].select_one('div[class*="column2 gridPanel"]').select('div[class="module"]')
            
            self.revenues_eps_df = revenues_eps_table(fins_details[0], ticker)
            
            self.sales_ests, self.earnings_ests, self.LTgrowth_ests = consensus_ests(fins_details[1], ticker)
            
            self.valuations = standard_fin_table(fins_details[2], ticker)
            self.dividends = standard_fin_table(fins_details[3], ticker)
            self.growthrate = standard_fin_table(fins_details[4], ticker)
            self.finstrength = standard_fin_table(fins_details[5], ticker)
            self.profitability = standard_fin_table(fins_details[6], ticker)
            self.efficiency = standard_fin_table(fins_details[7], ticker)
            self.management = standard_fin_table(fins_details[8], ticker)
            
            self.growth_summary = standard_fin_table(fins_sums[0], ticker)
            self.performance_summary = performance_table(fins_sums[1], ticker)
            self.institution_holdings = institution_holding_table(fins_sums[3], ticker)
            
        # Analyst Tables
        if len(analysts_raw) != 0:
            analyst_details = analysts_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
            
            self.recommendations = recommendation_table(analyst_details[0], ticker)
            self.analyst_recs = analyst_rec_table(analyst_details[1], ticker)
        
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

#%% Financials Tables

if len(analysts_raw) != 0:
    
    analyst_details = analysts_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
    

#%%

analyst_recommendation_module = analyst_details[1]




        
#%%
'''
dt.datetime.strptime(date.strip(), '%b. %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])


dt.datetime.strptime(date.strip(), '%B %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])

'''