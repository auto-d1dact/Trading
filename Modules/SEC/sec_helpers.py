# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 13:08:52 2018

@author: Fang
"""

import pandas as pd
import numpy as np
import time
from bs4 import BeautifulSoup as bs
import requests

import sys
from IPython.display import clear_output

def update_progress(progress, date_str):
    bar_length = 60
    
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
    if progress < 0:
        progress = 0
    if progress >= 1:
        progress = 1

    block = int(round(bar_length * progress))

    clear_output(wait = True)
    text = "Progress: [{0}] {1:.1f}% Elapsed Run Time: {2}".format( "#" * block + "-" * (bar_length - block), 
                                                      progress * 100,
                                                      date_str)
    print(text)

#%% Functions for insider txns scraping
def create_insidertxns(currF4):
    
    ###
    '''
    Format is standardized on SEC from 2003 Quarter 3
    '''
    
    ###
    start_time = time.time()
    
    column = 'Filename'
    sec_parent_url = 'https://www.sec.gov/Archives/'
    
    f4_df = pd.DataFrame(columns = ['cik','ticker','industry','txnDate','txnCode','txnAmount',
                                    'txnPrice','insiderName','isDirector',
                                    'isOfficer','tenPctOwner','officerTitle',
                                    'txnAcqDispCode','postTxnSharesOwned'])
    total_length = len(currF4)
    i = 0
    for idx in currF4.index:
        curr_f4 = sec_parent_url + currF4.loc[idx, column]
        curr_f4_text = requests.get(curr_f4).text
        soup_f4 = bs(curr_f4_text, 'lxml')

        try:
            cik = soup_f4.find('issuercik').text
            ticker = soup_f4.find('issuertradingsymbol').text.strip()
        except:
            continue
        
        try:
            header_text = soup_f4.find('sec-header').text
            industry = header_text[header_text.find('STANDARD INDUSTRIAL CLASSIFICATION:') + len('STANDARD INDUSTRIAL CLASSIFICATION:'):].split('\n')[0].strip().split('[')[0].strip()
        except:
            industry = ''
        
        try:
            insidername = soup_f4.find('rptownername').text.strip()
        except:
            insidername = ''
        
        try:
            isdirector = int(soup_f4.find('isdirector').text.strip())
        except:
            isdirector = ''
        
        try:
            isofficer = int(soup_f4.find('isofficer').text.strip())
        except:
            isofficer = ''
        
        try:
            istenpercentowner = int(soup_f4.find('istenpercentowner').text.strip())
        except:
            istenpercentowner = ''
        
        try:
            officertitle = soup_f4.find('officertitle').text.strip()
        except:
            officertitle = ''
        
        try:
            transactiondate = soup_f4.find('transactiondate').text.strip()
            if len(transactiondate) > 10:
                transactiondate = transactiondate.split('>')[-1]
        except:
            transactiondate = ''
            
        try:
            transactioncode = soup_f4.find('transactioncode').text.strip()
        except:
            transactioncode = ''
            
        try:
            transactionammount = float(soup_f4.find('transactionamounts').find('transactionshares').find('value').text.strip())
        except:
            transactionammount = np.nan
            
        try:
            transactionprice = float(soup_f4.find('transactionamounts').find('transactionpricepershare').find('value').text.strip())
        except:
            transactionprice = np.nan
            
        try:
            txnAcqDispCode = soup_f4.find('transactionamounts').find('transactionacquireddisposedcode').find('value').text.strip()
        except:
            txnAcqDispCode = ''
        
        try:
            sharesownedaftertxn = float(soup_f4.find('sharesownedfollowingtransaction').find('value').text.strip())
        except:
            sharesownedaftertxn = np.nan
        
        f4_df.loc[idx, :] = [cik, ticker,industry, transactiondate, transactioncode, transactionammount, transactionprice,
                             insidername, isdirector, isofficer,
                             istenpercentowner, officertitle, txnAcqDispCode, sharesownedaftertxn]
        i += 1
        run_time = round(time.time() - start_time, 2)
        
        update_progress((i + 1)/total_length, '{0} seconds for {1} on {2} '.format(run_time, ticker, transactiondate))
        #print('Completed %.2f Percent' % float(100*i/total_length))

    print('Completed in {} seconds'.format(time.time() - start_time))
    return f4_df