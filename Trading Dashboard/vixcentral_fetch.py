# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 21:09:26 2018

@author: fchen
"""

import pandas as pd

# Class for collecting VIX data specifically

class vixcentral:
    
    def __init__(self, daysback = 365, minback = 120):
        
        self.daily = pd.read_csv('http://173.212.203.121/noko.csv', index_col = 0)
        self.daily.index = pd.to_datetime(self.daily.index)
        self.daily = self.daily.sort_index()
        
    def __repr__(self):
        return str(self.daily.tail(1))
    
    def __str__(self):
        return str(self.daily.tail(1))


        