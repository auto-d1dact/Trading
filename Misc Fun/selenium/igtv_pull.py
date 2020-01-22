# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 22:28:11 2020

@author: Fang
"""

import pandas as pd
import igtv_helpers


#%%
if __name__ == '__main__':
    wrong_selection = True
    
    while wrong_selection:
        channel_selection = input("Enter 1 for single channel\nEnter 2 for list of channels\n")
        if channel_selection in ['1','2']:
            wrong_selection = False
    
    if channel_selection == '1':
        show = input("Enter channel name: ")
        igtv_helpers.scrape_igtv(show, return_df = False, code_sleep_time = 0.1)
        
    else:
        show_file = input("Enter file name: ")
        show_aliases = pd.read_csv('shows_list.csv')
        
        for show in show_aliases.iloc[:,0]:
            igtv_helpers.scrape_igtv(show, return_df = False, code_sleep_time = 0.1)
