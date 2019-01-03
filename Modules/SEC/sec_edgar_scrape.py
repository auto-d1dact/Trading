# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 13:32:23 2018

@author: Fang
"""

import pandas as pd
import numpy as np
import datetime as dt
import time
from bs4 import BeautifulSoup as bs
import requests
import os
from sqlalchemy import *
from sqlalchemy import create_engine
import calendar
import sqlite3 as sql

module_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\SEC'

os.chdir(module_dir)
import sec_helpers

dbs_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Data\\DBs'
os.chdir(dbs_dir)
#'''
#Transaction Code Transaction
#A | Grant, award, or other acquisition of securities from the company (such as an option)
#K | Equity swaps and similar hedging transactions
#P | Purchase of securities on an exchange or from another person
#S | Sale of securities on an exchange or to another person
#D | Sale or transfer of securities back to the company
#F | Payment of exercise price or tax liability using portion of securities received from the company
#M | Exercise or conversion of derivative security received from the company (such as an option)
#G | Gift of securities by or to the insider
#V | A transaction voluntarily reported on Form 4
#J | Other (accompanied by a footnote describing the transaction)
#'''

#%% Function for getting file names from EDGAR
def sec_master_table(year,quarter):
    sec_url = 'https://www.sec.gov/Archives/edgar/full-index/{0}/QTR{1}/master.idx'.format(year,quarter)
    try:
        indexfile = pd.read_table(sec_url, skiprows = 9, sep="|").loc[1:,:].reset_index(drop = True)
    except:
        try:
            indexfile = pd.read_table(sec_url, skiprows = 9, sep="|", encoding = 'iso-8859-1').loc[1:,:].reset_index(drop = True)
        except:
            try:
                indexfile = pd.read_table(sec_url, skiprows = 9, sep="|", encoding = 'utf-8').loc[1:,:].reset_index(drop = True)
            except:
                try:
                    indexfile = pd.read_table(sec_url, skiprows = 9, sep="|", encoding = 'cp1252').loc[1:,:].reset_index(drop = True)
                except:
                    indexfile = pd.read_table(sec_url, skiprows = 9, sep="|", encoding = 'latin1').loc[1:,:].reset_index(drop = True)
    
    indexfile.columns = [x.replace(' ','') for x in indexfile.columns.tolist()]
    
    indexfile['DateFiled'] = pd.to_datetime(indexfile['DateFiled'])
    
    form4_insidertxns = indexfile[indexfile['FormType'] == '4'].reset_index(drop = True)
    form4_insidertxns['Year'] = year
    form4_insidertxns['Quarter'] = quarter
    
    #curr_table[curr_table['Form Type'].str.contains('10-Q')]
    
    return form4_insidertxns

def sec_crawler_table(year,quarter):
    sec_url = 'https://www.sec.gov/Archives/edgar/full-index/{0}/QTR{1}/crawler.idx'.format(year,quarter)
    crawlertext = requests.get(sec_url).text.split('\n')[7:]
    
    for i, line in enumerate(crawlertext):
        if i == 0:
            headers = [x.strip().replace(' ','') for x in list(filter(lambda x: x != '', line.split('  ')))]
            crawler_df = pd.DataFrame(columns = headers)
        elif i == 1:
            continue
        else:
            if '10-Q' in line or '10-K' in line:  
                url = line[line.find('http'):].strip()
                filingdate = line[:line.find('http')].strip().split(' ')[-1]
                cik = line[:line.find(filingdate)].strip().split(' ')[-1]
                formtype = line[:line.find(cik)].strip().split('   ')[-1].strip()
                companyname = line[:line.find(formtype)].strip()
                
                crawler_df.loc[i - 2, 'URL'] = url
                crawler_df.loc[i - 2, 'DateFiled'] = filingdate
                crawler_df.loc[i - 2, 'CIK'] = cik
                crawler_df.loc[i - 2, 'FormType'] = formtype
                crawler_df.loc[i - 2, 'CompanyName'] = companyname
    
    crawler_df['DateFiled'] = pd.to_datetime(crawler_df['DateFiled'])

    tenQs = crawler_df[crawler_df['FormType'] == '10-Q'].reset_index(drop = True)
    tenQs['Year'] = year
    tenQs['Quarter'] = quarter
    
    tenKs = crawler_df[crawler_df['FormType'] == '10-K'].reset_index(drop = True)
    tenKs['Year'] = year
    tenKs['Quarter'] = quarter
    
    return tenQs, tenKs

#%% Function for querying latest links from local database
def latest_sec_db_links(formtype, year, qtr, engine):
    
    if formtype == 'F4':
        table_name = 'F4Links'
    elif formtype == '10Q':
        table_name = 'Links10Q'
    elif formtype == '10K':
        table_name = 'Links10K'
    else:
        print('formtypes are F4 10Q and 10K')
        return
    
    query = 'SELECT * FROM {0} WHERE Year = {1} AND Quarter = {2}'.format(table_name, year, qtr)
    
    db_table = pd.read_sql_query(query, con=engine, index_col = 'idx')
    
    db_table['DateFiled'] = pd.to_datetime(db_table['DateFiled'])
    
    return db_table.reset_index(drop = True)

#%%

start_time = time.time()
sec_engine = create_engine('sqlite:///SEC.db', echo=False)

for year in range(2008, 2019):
    for qtr in range(1, 5):
        if year == 2008 and qtr < 4:
            continue
        else:
            latest_db_f4 = latest_sec_db_links('F4', year, qtr, sec_engine)
            
            for df in np.array_split(latest_db_f4, 100):
                curr_txns = sec_helpers.create_insidertxns(df)
                curr_txns.to_sql('InsiderTxns', con=sec_engine, if_exists='append', index_label = 'idx')
                
            print('{0}-Q{1}'.format(year,qtr))

print('Completed in {} seconds'.format(time.time() - start_time))

#%%

if __name__ == '__main__':
    
    start_time = time.time()
    
    sec_engine = create_engine('sqlite:///SEC.db', echo=False)
    
    curr_date = dt.datetime.now()
    q1 = dt.datetime(curr_date.year, 4, 1)
    q2 = dt.datetime(curr_date.year, 7, 1)
    q3 = dt.datetime(curr_date.year, 10, 1)
    q4 = dt.datetime(curr_date.year + 1, 1, 1)
    
    if curr_date < q1:
        qtr = 1
    elif curr_date < q2:
        qtr = 2
    elif curr_date < q3:
        qtr = 3
    else:
        qtr = 4
    
    year = curr_date.year
    
    latest_db_f4 = latest_sec_db_links('F4', year, qtr, sec_engine)
    latest_db_10q = latest_sec_db_links('10Q', year, qtr, sec_engine)
    latest_db_10k = latest_sec_db_links('10K', year, qtr, sec_engine)
    
    curr10q, curr10k = sec_crawler_table(year,qtr)
    currF4 = sec_master_table(year,qtr)
    
    db_files_f4 = latest_db_f4['Filename'].tolist()
    db_files_10q = latest_db_10q['URL'].tolist()
    db_files_10k = latest_db_10k['URL'].tolist()
    
    curr_files_f4 = currF4['Filename'].tolist()
    curr_files_10q = curr10q['URL'].tolist()
    curr_files_10k = curr10k['URL'].tolist()
    
    newf4s = list(filter(lambda x: x not in db_files_f4, curr_files_f4))
    new10qs = list(filter(lambda x: x not in db_files_10q, curr_files_10q))
    new10ks = list(filter(lambda x: x not in db_files_10k, curr_files_10k))
    
    to_add_f4 = currF4[currF4['Filename'].isin(newf4s)]
    to_add_10q = curr10q[curr10q['Filename'].isin(new10qs)]
    to_add_10k = curr10k[curr10k['Filename'].isin(new10ks)]
    
    to_add_f4.to_sql('F4Links', con=sec_engine, if_exists='append', index_label = 'idx')
    to_add_10q.to_sql('Links10Q', con=sec_engine, if_exists='append', index_label = 'idx')
    to_add_10k.to_sql('Links10K', con=sec_engine, if_exists='append', index_label = 'idx')
    
    print('Completed in {} seconds'.format(time.time() - start_time))
