# -*- coding: utf-8 -*-
"""
Created on Sun Aug 11 13:49:59 2019

@author: Fang
"""

#%%
import time
import sys
import os
import pandas as pd
import datetime as dt
import numpy as np
cwd = os.getcwd()

import igtv_helpers
import igtv_show

#%%

def igtv_main():
    ig_handles = pd.read_csv('shows_list.csv')['ig_handle']
    
    shows = [igtv_show.igtv_show(ig_handle) for ig_handle in ig_handles]
    
    log_df = pd.DataFrame(columns = ['PullStatus','NumberOfPosts'],
                          index = ig_handles)
    
    n = len(shows)
    
    print('\n')
    print('Pulling IGTV Show Summaries')
    print('\n')
    print('##################################################')
    print('\n')
    start_time_full = time.time()
    
    for i, show in enumerate(shows):
        
        try:
            show.get_show_info()
            show.get_show_posts()
            
            log_df.loc[show.ig_handle, 'PullStatus'] = 'Completed'
            log_df.loc[show.ig_handle, 'NumberOfPosts'] = len(show.posts)
        except:
            log_df.loc[show.ig_handle, 'PullStatus'] = 'Failed'
            
        sys.stdout.write('\r')
        j = (i + 1) / n
        sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j) + ' Getting Show Summaries')
        sys.stdout.flush()
        time.sleep(1.5)
    
    print('\n')
    print('Show Summaries Completed in {} seconds'.format(time.time() - start_time_full))
    
    show_engagement_summaries = pd.concat([show.summary_engagements for show in shows], axis = 0)
    all_show_posts = pd.concat([show.posts for show in shows], axis = 0).reset_index(drop = True)
    
    all_show_posts['views'] = np.nan
    all_show_posts['comment_count'] = np.nan
    all_show_posts['likes'] = np.nan
    all_show_posts['caption'] = np.nan
    all_show_posts['upload_date'] = np.nan
    
    print('\n')
    print('Pulling IGTV Post Details')
    print('\n')
    print('##################################################')
    print('\n')
    start_time_posts = time.time()
    
    n = len(all_show_posts)
    
    for idx, row in all_show_posts.iterrows():
        curr_post_details = igtv_helpers.get_post_details(row.url, row.ig_handle, row.igtv_code)
        
        all_show_posts.loc[idx, 'views'] = curr_post_details.loc[0, 'views']
        all_show_posts.loc[idx, 'comment_count'] = curr_post_details.loc[0, 'comment_count']
        all_show_posts.loc[idx, 'likes'] = curr_post_details.loc[0, 'likes']
        all_show_posts.loc[idx, 'caption'] = curr_post_details.loc[0, 'caption']
        all_show_posts.loc[idx, 'upload_date'] = curr_post_details.loc[0, 'upload_date']
        
        sys.stdout.write('\r')
        j = (idx + 1) / n
        sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j) + ' Getting Post Data')
        sys.stdout.flush()
        time.sleep(1.5)
    
    print('\n')
    print('Post Data Pull Completed in {} seconds'.format(time.time() - start_time_posts))
    
    
    currdate = dt.datetime.today().strftime('%Y-%m-%d')
    
    log_filename = cwd + '\\Logs\\igtv_extract_log_{}.csv'.format(currdate)
    show_sums_filename = cwd + '\\Outputs\\igtv_extract_show_summaries_{}.csv'.format(currdate)
    show_posts_filename = cwd + '\\Outputs\\igtv_extract_show_posts_{}.csv'.format(currdate)
    
    log_df.to_csv(log_filename)
    show_engagement_summaries.to_csv(show_sums_filename)
    all_show_posts.to_csv(show_posts_filename)
    
    print('\n')
    print('##################################################')
    print('\n')
    print('Full Data Extraction Completed in {} seconds'.format(time.time() - start_time_full))

if __name__ == '__main__':
    igtv_main()