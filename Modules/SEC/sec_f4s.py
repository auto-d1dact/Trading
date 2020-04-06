# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 13:32:23 2020

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

#%% Function for getting file names from EDGAR
def get_sec_f4s(year,quarter):
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