# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 22:06:45 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
import requests

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection'
os.chdir(main_dir)

# Initializing Stock Universe
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Stock Universe')

cad_names = pd.read_csv('cad_names.csv')['Symbol'].tolist()
us_names = pd.read_csv('us_names.csv')['Symbol'].tolist()

os.chdir(main_dir)

from yahoo_query import *

#%%