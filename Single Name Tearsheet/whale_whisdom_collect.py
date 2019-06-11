# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 21:53:25 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
import requests
from bs4 import BeautifulSoup as bs

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

us_names = pd.read_csv('us_names.csv')['Symbol'].tolist()

os.chdir(main_dir)

#%%
ticker = 'AAPL'
site = 'https://whalewisdom.com/stock/' + ticker
soup = bs(requests.get(site).text, 'lxml')

#%%
table = soup.select('table[class="table"]')[0]

#%%


#%%
def whale_scrape(ticker):
    site = 'https://whalewisdom.com/stock/' + ticker
    soup = bs(requests.get(site).text, 'lxml')
    tables = soup.find_all('table')
    for table in tables:
        if ticker in str(table):
            whaletable = table

    def html_filter(string):
        searchfor = ['tr', 'div']
        for search in searchfor:
            if search in string:
                return False
        return True

    headers = ['Quarter','Field','All Institutions', 'Hedge Funds']
    headers = ['ticker','numOfFirmsHoldingInTop10','numOfHolders','priorQDate',
               'recQDate','priorQShares','recQShares','fundNumPercentChange', 'fundsCreatingNewPos',
               'fundsAddingPos', 'fundsClosingPos', 'fundsReducingPos']
    whale_df = pd.DataFrame(columns = headers, index = ['allFirms','hedgeFunds'])
    whale_df['ticker'] = ticker
    
    i = 0
    j = 0

    for row in whaletable.find_all('tr'):


        individual_row = list(filter(lambda x: html_filter(x), str(row).split('\\n')))
        individual_row = list(filter(lambda x: x.strip() != '<td>', individual_row))
        individual_row = list(filter(lambda x: x.strip() != '</td>', individual_row))
        individual_row = list(filter(lambda x: '<td class=' not in x, individual_row))
        individual_row = [x.replace('<td>','').replace('</td>','').strip().replace(':', '') for x in individual_row]
        individual_row = list(filter(lambda x: len(x) > 0, individual_row))

        if i == 0 or len(individual_row) < 3 or 'Heat Map' in individual_row[0]:
            i += 1
            continue
        else:
            j += 1

        if j == 1:
            # top 10 
            field = 'numOfFirmsHoldingInTop10'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
        elif j == 2:
            # number of filers holding stock
            field = 'numOfHolders'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
        elif j == 3:
            # recent quarter share holdings
            quarter = dt.datetime.strptime(individual_row[0].split(' ')[-1], '%m/%d/%Y').date()
            whale_df['recQDate'] = quarter

            holdings_lst = []
            for holdings in individual_row[1:]:
                amount = holdings.split(' ')
                if amount[1] == 'Billion':
                    holdings_lst.append(float(amount[0].replace(',', ''))*10**9)
                elif amount[1] == 'Million':
                    holdings_lst.append(float(amount[0].replace(',', ''))*10**6)

            field = 'recQShares'
            whale_df.loc['allFirms', field] = float(holdings_lst[0])
            whale_df.loc['hedgeFunds', field] = float(holdings_lst[1])
        elif j == 4:
            # prior quarter share holdings
            quarter = dt.datetime.strptime(individual_row[0].split(' ')[-1], '%m/%d/%Y').date()
            whale_df['priorQDate'] = quarter

            holdings_lst = []
            for holdings in individual_row[1:]:
                amount = holdings.split(' ')
                if amount[1] == 'Billion':
                    holdings_lst.append(float(amount[0].replace(',', ''))*10**9)
                elif amount[1] == 'Million':
                    holdings_lst.append(float(amount[0].replace(',', ''))*10**6)

            field = 'priorQShares'
            whale_df.loc['allFirms', field] = float(holdings_lst[0])
            whale_df.loc['hedgeFunds', field] = float(holdings_lst[1])
        elif j == 5:
            # percent change in firms
            field = 'fundNumPercentChange'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace('%', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace('%', ''))
        elif j == 6:
            # funds creating new positions
            field = 'fundsCreatingNewPos'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
        elif j == 7:
            # funds adding to exisiting
            field = 'fundsAddingPos'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
        elif j == 8:
            # funds closing out
            field = 'fundsClosingPos'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
        else:
            # funds reducing
            field = 'fundsReducingPos'
            whale_df.loc['allFirms', field] = float(individual_row[1].replace(',', ''))
            whale_df.loc['hedgeFunds', field] = float(individual_row[2].replace(',', ''))
    
    return whale_df

#%%
if __name__ == '__main__':
    item_counter = 0
    total_length = len(us_names)
    failed_list = []
    whaledf_list = []
    retries = 1
    
    for ticker in us_names:
        try:
            curr_df = whale_scrape(ticker)
            whaledf_list.append(curr_df)
            print('Accepted: {}'.format(ticker))
        except:
            for i in range(retries):
                try:
                    curr_df = whale_scrape(ticker)
                    whaledf_list.append(curr_df)
                    print('Accepted: {}'.format(ticker))
                except:
                    continue
            print('Failed: {}'.format(ticker))
            failed_list.append(ticker)
    
        item_counter += 1
        print('{0:.2f}% Completed'.format(item_counter/total_length*100))
        print('{} total failures'.format(len(failed_list)))
    
    whales = pd.concat(whaledf_list, axis = 0)

    currdate = dt.datetime.today().strftime('%Y-%m-%d')
    
    os.chdir('..\\')
    os.chdir('..\\')
    os.chdir('..\\Data\\Historical Queries\\Whales')
    
    whales.to_csv('whales {}.csv'.format(currdate))
    
    os.chdir(main_dir)
    
    del whales, currdate, whaledf_list#, failed_ist

