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
                    try:
                        curr_table_dict_lt[headers[0]].append(curr_cols[0])
                        curr_table_dict_lt[headers[1]].append(to_float(curr_cols[1]))
                        curr_table_dict_lt[headers[2]].append(to_float(curr_cols[2]))
                        curr_table_dict_lt[headers[3]].append(to_float(curr_cols[3]))
                        curr_table_dict_lt[headers[4]].append(to_float(curr_cols[4]))
                        curr_table_dict_lt[headers[5]].append(to_float(curr_cols[5]))
                    except:
                        None
        sales_ests = pd.DataFrame(curr_table_dict_sales)
        sales_ests['Underlying'] = ticker
        earnings_ests = pd.DataFrame(curr_table_dict_earnings)
        earnings_ests['Underlying'] = ticker
        
        try:
            LTgrowth_ests = pd.DataFrame(curr_table_dict_lt)
            LTgrowth_ests.index = [ticker]
            return sales_ests, earnings_ests, LTgrowth_ests
        except:        
            return sales_ests, earnings_ests, pd.DataFrame({})
    
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
        analyst_recommendation_df['Underlying'] = ticker
        return analyst_recommendation_df
    
########## Standard Analyst Table Parsing
def standard_analyst_table(module, ticker, ltgrowth = False):
    detail_table = module.find('table')
        
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
                    
                    for i, header in enumerate(headers[1:]):
                        curr_table_dict_sales[header].append(to_float(curr_cols[i + 1]))
                        
                elif 'LT' not in curr_cols[0]:
                    curr_table_dict_earnings[headers[0]].append(curr_cols[0])
                    
                    for i, header in enumerate(headers[1:]):
                        curr_table_dict_earnings[header].append(to_float(curr_cols[i + 1]))
                else:
                    try:
                        curr_table_dict_lt[headers[0]].append(curr_cols[0])
                        
                        for i, header in enumerate(headers[1:]):
                            curr_table_dict_lt[header].append(to_float(curr_cols[i + 1]))
                    except:
                        None
                        
        sales_ests = pd.DataFrame(curr_table_dict_sales)
        sales_ests['Underlying'] = ticker
        earnings_ests = pd.DataFrame(curr_table_dict_earnings)
        earnings_ests['Underlying'] = ticker
        
        if ltgrowth:
            try: 
                LTgrowth_ests = pd.DataFrame(curr_table_dict_lt)
                LTgrowth_ests.index = [ticker]
                
                return sales_ests, earnings_ests, LTgrowth_ests
            except:
                return sales_ests, earnings_ests, pd.DataFrame({})
        else:
            return sales_ests, earnings_ests

########## Revisions Table Parsing
def revisions_table(revisions_module, ticker):
    detail_table = revisions_module.find('table')
                        
    if len(detail_table) != 0:
        curr_table_dict_rev = {}
        curr_table_dict_earnings = {}
        
        for row in detail_table.find_all('tr'):
            if len(row.find_all('th')) != 0:
                headers = [x.text.replace('\xa0',' ').replace('%','Percent').replace(' ','').replace(':','') for x in row.find_all('th')]
                for i, header in enumerate(headers):
                    if i == 1 or i == 2:
                        headers[i] = header + ' LastWeek'
                    elif i == 3 or i == 4:
                        headers[i] = header + ' Last4Weeks'
                    else:
                        None
                
                if len(headers) > 4:
                    for h in headers:
                        curr_table_dict_rev[h] = []
                        curr_table_dict_earnings[h] = []
                        
                continue
            else:
                curr_cols = [x.text.strip().replace('\xa0',' ') for x in row.find_all('td')]
                
                if len(curr_cols) == 1:
                    curr_table_title = curr_cols[0]
                    continue
                
                curr_cols = [x.replace('\xa0',' ') for x in curr_cols]
                
                if 'rev' in curr_table_title.lower():
                    curr_table_dict_rev[headers[0]].append(curr_cols[0])
                    
                    for i, header in enumerate(headers[1:]):
                        curr_table_dict_rev[header].append(to_float(curr_cols[i + 1]))
                        
                else:
                    curr_table_dict_earnings[headers[0]].append(curr_cols[0])
                    
                    for i, header in enumerate(headers[1:]):
                        curr_table_dict_earnings[header].append(to_float(curr_cols[i + 1]))
    
        rev_revisions_df = pd.DataFrame(curr_table_dict_rev)
        rev_revisions_df['Underlying'] = ticker
        earnings_revisions_df = pd.DataFrame(curr_table_dict_earnings)
        earnings_revisions_df['Underlying'] = ticker
        return rev_revisions_df, earnings_revisions_df

########### Insider Tables Parse
def insiders_table(insider_details, ticker):
    
    detail_table = insider_details.find('table')
                
    if len(detail_table) != 0:
        curr_table_dict = {}
        
        i = 1
        for row in detail_table.find_all('tr'):
            if i == 1:
                headers = [x.text.replace('\n','') for x in row.find_all('th')]
                for header in headers:
                    curr_table_dict[header] = []
                i = 0
                continue
            else:
                curr_cols = [x.text.strip() for x in row.find_all('td')]
                if len(curr_cols) != 1:
                    curr_table_dict[headers[0]].append(dt.datetime.strptime(curr_cols[0], '%d %b %Y').date())
                    curr_table_dict[headers[1]].append(curr_cols[1])
                    curr_table_dict[headers[2]].append(curr_cols[2])
                    curr_table_dict[headers[3]].append(curr_cols[3])
                    curr_table_dict[headers[4]].append(to_float(curr_cols[4]))
                    curr_table_dict[headers[5]].append(to_float(curr_cols[5]))
    insiders_df = pd.DataFrame(curr_table_dict)
    insiders_df['Underlying'] = ticker
    return insiders_df

############ Full insider table parse
def reuters_insiders(ticker):
    
    ########### Insider Tables Parse
    def insiders_table(detail_table, ticker):

        def to_float(string):
            val = string.strip().replace(',','').replace('$','').replace('--','')
            if val != '':
                return float(val)
            else:
                return np.nan

        if len(detail_table) != 0:
            curr_table_dict = {}

            i = 1
            for row in detail_table.find_all('tr'):
                if i == 1:
                    headers = [x.text.replace('\n','') for x in row.find_all('th')]
                    for header in headers:
                        curr_table_dict[header] = []
                    i = 0
                    continue
                else:
                    curr_cols = [x.text.strip() for x in row.find_all('td')]
                    if len(curr_cols) != 1:
                        curr_table_dict[headers[0]].append(dt.datetime.strptime(curr_cols[0], '%d %b %Y').date())
                        curr_table_dict[headers[1]].append(curr_cols[1])
                        curr_table_dict[headers[2]].append(curr_cols[2])
                        curr_table_dict[headers[3]].append(curr_cols[3])
                        curr_table_dict[headers[4]].append(to_float(curr_cols[4]))
                        curr_table_dict[headers[5]].append(to_float(curr_cols[5]))
        insiders_df = pd.DataFrame(curr_table_dict)
        insiders_df['Underlying'] = ticker
        return insiders_df
    
    insiders_base_url = 'https://www.reuters.com/finance/stocks/insider-trading/'
    insiders_url = insiders_base_url + ticker
    insiders = bs(requests.get(insiders_url).text, 'lxml')
    new_ticker = insiders.find('h1').text.split(' ')[-1].replace('(','').replace(')','')
    
    insiderTxns_lst = []
    
    try:
        total_pages = int(insiders.select_one('span[class="pageStatus"]').text.strip().split(' ')[-1])
        current_page = int(insiders.select_one('span[class="pageStatus"]').text.strip().split(' ')[1])

        for i in range(1,total_pages + 1):
            page_url = insiders_base_url + new_ticker + '?symbol=&name=&pn={}&sortDir=&sortBy='.format(i)
            insiders = bs(requests.get(page_url).text, 'lxml')
            #print(page_url)
            total_pages = int(insiders.select_one('span[class="pageStatus"]').text.strip().split(' ')[-1])
            current_page = int(insiders.select_one('span[class="pageStatus"]').text.strip().split(' ')[1])

            if insiders.select_one('div[class*="insiderTradingHeader"]') != None:
                insider_details = insiders.select_one('div[class*="insiderTradingHeader"]').findNext('table')

                if current_page == i:
                    insiderTxns_lst.append(insiders_table(insider_details, ticker))
    except:
        None
    
    if len(insiderTxns_lst) > 0:
        return pd.concat(insiderTxns_lst, axis = 0).reset_index(drop = True)
    else:
        return []


#%%
class reuters_query:
    
    def __init__(self, ticker):
        
        overview_url = 'https://www.reuters.com/finance/stocks/overview/' + ticker
        overview = bs(requests.get(overview_url).text, 'lxml')
        
        new_ticker = overview.find('h1').text.split(' ')[-1].replace('(','').replace(')','')
        
        if '.' in new_ticker:
            check = new_ticker.split('.')[0]
            if check == ticker:
                ticker = new_ticker
        
        financials_url = 'https://www.reuters.com/finance/stocks/financial-highlights/' + ticker
        analysts_url = 'https://www.reuters.com/finance/stocks/analyst/' + ticker
#        insiders_url = 'https://www.reuters.com/finance/stocks/insider-trading/' + ticker
        
        
        financials = bs(requests.get(financials_url).text, 'lxml')
        analysts = bs(requests.get(analysts_url).text, 'lxml')
#        insiders = bs(requests.get(insiders_url).text, 'lxml')
#
#        insiders_raw = insiders.find('div', 
#                                     {'id': 'content'}).select('div[class*="sectionContent"]')
#        
        overview_raw = overview.find('div', 
                                     {'id': 'content'}).select('div[class*="sectionContent"]')

        financials_raw = financials.find('div', 
                                         {'id': 'content'}).select('div[class*="sectionContent"]')
        
        analysts_raw = analysts.find('div', 
                                     {'id': 'content'}).select('div[class*="sectionContent"]')
        
        
        # Overview Tables
        
        if len(overview_raw) != 0:                
            self.overall_df = overview_tables(overview_raw, ticker)
        
        # Financial Ratio Tables
        if len(financials_raw) != 0:
            fins_details = financials_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
            fins_sums = financials_raw[1].select_one('div[class*="column2 gridPanel"]').select('div[class="module"]')
            
            try:
                self.revenues_eps_df = revenues_eps_table(fins_details[0], ticker)
            except:
                print('No revenues for {}'.format(ticker))
            
            try:
                self.sales_ests, self.earnings_ests, self.LTgrowth_ests = consensus_ests(fins_details[1], ticker)
            except:
                print('No estimates for {}'.format(ticker))
            
            try:
                self.valuations = standard_fin_table(fins_details[2], ticker)
            except:
                print('No valuations for {}'.format(ticker))
            
            try:
                self.dividends = standard_fin_table(fins_details[3], ticker)
            except:
                print('No dividends for {}'.format(ticker))
                
            try:
                self.growthrate = standard_fin_table(fins_details[4], ticker)
            except:
                print('No growthrate for {}'.format(ticker))
             
            try:
                self.finstrength = standard_fin_table(fins_details[5], ticker)
            except:
                print('No finstrength for {}'.format(ticker))
                
            try:
                self.profitability = standard_fin_table(fins_details[6], ticker)
            except:
                print('No profitability for {}'.format(ticker))
            
            try:
                self.efficiency = standard_fin_table(fins_details[7], ticker)
            except:
                print('No efficiency for {}'.format(ticker))
            
            try:
                self.management = standard_fin_table(fins_details[8], ticker)
            except:
                print('No management for {}'.format(ticker))
            
            try:
                self.growth_summary = standard_fin_table(fins_sums[0], ticker)
            except:
                print('No growth_summary for {}'.format(ticker))
            
            try:
                self.performance_summary = performance_table(fins_sums[1], ticker)
            except:
                print('No performance_summary for {}'.format(ticker))
                
            try:
                self.institution_holdings = institution_holding_table(fins_sums[3], ticker)
            except:
                print('No institution_holdings for {}'.format(ticker))
            
        # Analyst Tables
        if len(analysts_raw) != 0:
            analyst_details = analysts_raw[1].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')
            
            try:
                self.recommendations = recommendation_table(analyst_details[0], ticker)
            except:
                print('No recommendations for {}'.format(ticker))
                
            try:
                self.analyst_recs = analyst_rec_table(analyst_details[1], ticker)
            except:
                print('No analyst_recs for {}'.format(ticker))
                
            try:
                self.sales_analysis, self.earnings_analysis, self.LTgrowth_analysis = standard_analyst_table(analyst_details[2], ticker, ltgrowth = True)
            except:
                print('No sales_analysis for {}'.format(ticker))
                
            try:
                self.sales_surprises, self.earnings_surprises = standard_analyst_table(analyst_details[3], ticker, ltgrowth = False)
                self.earnings_surprises['Surprise %'] = (self.earnings_surprises['Actual'] - self.earnings_surprises['Estimate'])/abs(self.earnings_surprises['Estimate'])
                self.sales_surprises['Surprise %'] = (self.sales_surprises['Actual'] - self.sales_surprises['Estimate'])/abs(self.sales_surprises['Estimate'])
            except:
                print('No sales_surprises for {}'.format(ticker))
                
            try:
                self.sales_trend, self.earnings_trend = standard_analyst_table(analyst_details[4], ticker, ltgrowth = False)
            except:
                print('No sales_trend for {}'.format(ticker))
                
            try:
                self.revenue_revisions, self.earnings_revisions = revisions_table(analyst_details[5], ticker)
            except:
                print('No revenue_revisions for {}'.format(ticker))
                
        # Insiders Table
#        if len(insiders_raw) > 2:
            
        try:
#                insider_details = insiders_raw[2].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')[0]
#                
#                insider_pages_numbers = insider_details.find('span', {'class':'pageStatus'})
#                
#                insiders_dflist = [insiders_table(insider_details, ticker)]
#                if insider_pages_numbers != None:
#                    insider_pages_numbers = int(insider_pages_numbers.text.strip().split(' ')[-1])
#                    
#                    for i in range(2, insider_pages_numbers + 1):
#                        page_url = insiders_url + "?symbol=&name=&pn={}&sortDir=&sortBy=".format(i)
#                        next_insider_page = bs(requests.get(page_url).text, 'lxml')
#                        next_insider_raw = next_insider_page.find('div', 
#                                                                  {'id': 'content'}).select('div[class*="sectionContent"]')
#                        next_insider_details = next_insider_raw[2].select_one('div[class*="column1 gridPanel"]').select('div[class="module"]')[0]
#                        next_insiders_df = insiders_table(next_insider_details, ticker)
#                        insiders_dflist.append(next_insiders_df)
                        
            self.insiders_txns = reuters_insiders(ticker)
        except:
            print('No insiders_txns for {}'.format(ticker))
            
    
#%%
'''
ticker = 'AAPL'

stock = reuters_query(ticker)


dt.datetime.strptime(date.strip(), '%b. %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])


dt.datetime.strptime(date.strip(), '%B %d, %Y')
earnings_dates.append(date)
earnings_times.append(earnings_row[2].strip().splitlines()[0])

'''