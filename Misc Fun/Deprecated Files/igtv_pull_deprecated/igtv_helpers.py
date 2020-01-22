# -*- coding: utf-8 -*-
"""
Created on Sun Aug 11 14:16:55 2019

@author: Fang
"""

#%%
import pip
import time
import os
import pandas as pd
import datetime as dt
import requests
import json
import numpy as np

try:
    from bs4 import BeautifulSoup as bs
except ImportError:
    pip.main(['install', 'bs4'])
    from bs4 import BeautifulSoup as bs
    
try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
except ImportError:
    pip.main(['install', 'selenium'])
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException

#%%

def show_summary_from_source(curr_source, ig_handle):
    curr_soup = bs(curr_source, 'lxml')
    followers_count = curr_soup.find('script', attrs = {'type': "application/ld+json"})
    followers_count = json.loads(followers_count.text.strip())['mainEntityofPage']['interactionStatistic']['userInteractionCount']
    
    show_overview_info_metas = [str(meta) for meta in curr_soup.find_all('meta')]
    filtered_metas = list(filter(lambda meta: 'Follow' in meta, show_overview_info_metas))[0].split('"')[1].split(' - ')[0]
    show_overview_summary = filtered_metas.split(', ')
    following_count = show_overview_summary[1].split(' ')[0].replace(',','')
    total_posts_count = show_overview_summary[2].split(' ')[0].replace(',','')
        
    show_summary_dict = {'num_of_followers': followers_count, 
                         'num_of_following': following_count,
                         'total_posts': total_posts_count}
    
    return pd.DataFrame(show_summary_dict, index = [ig_handle])

#%%

def get_main_html_source(show_url, chrome_dir, ig_handle):
    
    browser = webdriver.Chrome(executable_path = chrome_dir + "\\chromedriver.exe")

    browser.get(show_url)

    source_list = []
    
    curr_source = browser.page_source
    
    summary_info_df = show_summary_from_source(curr_source, ig_handle)
    
    source_list.append(curr_source)    

    SCROLL_PAUSE_TIME = 1.5

    # Get scroll height
    last_height = browser.execute_script("return document.body.scrollHeight")

    while True:

        curr_page_source = browser.page_source
        source_list.append(curr_page_source)

        # Scroll down to bottom
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    browser.quit()
    
    return summary_info_df, source_list

#%%

def get_show_posts_df(ig_handle, source_text):
    
    source_soup = bs(source_text, 'lxml').find_all('a')
    
    source_soup = list(filter(lambda a: '/tv/' in a['href'],  source_soup))
    
    show_post_dict = {'igtv_code': [],
                      'ig_handle': [],
                      'title': [],
                      'video_length': [],
                      'url': []}
    
    for a in source_soup:
        
        try:
            show_post_dict['title'].append(a.find('div', attrs={'class': 'pu1E0'}).text)
        except:
            show_post_dict['title'].append(np.nan)
        
        try:
            show_post_dict['video_length'].append(a.find('div', attrs={'class': 'zncDM'}).text)
        except:
            show_post_dict['video_length'].append(np.nan)
            
        try:
            show_post_dict['url'].append('https://www.instagram.com{}'.format(a['href']))
        except:
            show_post_dict['url'].append(np.nan)
        
        try:
            show_post_dict['igtv_code'].append(a['href'].replace('/tv/','').strip('/'))
        except:
            show_post_dict['igtv_code'].append(np.nan)
            
        show_post_dict['ig_handle'].append(ig_handle)
    
    show_post_df = pd.DataFrame(show_post_dict)
    
    return show_post_df

#%%
def get_post_details(post_url, ig_handle, igtv_code):

    post_soup = bs(requests.get(post_url).text, 'lxml')
    
    post_json = post_soup.find('script', type = 'application/ld+json')
    
    if post_json != None:
        ## Main method to get post info
        
        post_json = json.loads(post_json.text.strip())
        
        try:
            commentCount = post_json['commentCount']
        except:
            try:
                commentCount = post_json['description'].split(' Likes, ')[1].split(' Comments')[0]
            except:
                commentCount = np.nan
            
        try:
            likes = post_json['description'].split(' Likes, ')[0].replace(',', '')
        except:
            likes = np.nan
        
        try:
            views = post_json['interactionStatistic']['userInteractionCount']
        except:
            views = np.nan
        
        try:
            uploadDate = post_json['uploadDate']#.split('T')[0]
        except:
            uploadDate = np.nan
            
        try:
            caption = post_json['caption']
        except:
            caption = np.nan
        
    else:
        ## Alternate method to get post info
        raw_dict_string = str(post_soup.find_all('body')[0].find('script')).split('_sharedData = ')[1].replace(';</script>','')
        raw_json = json.loads(raw_dict_string)
        post_json = raw_json['entry_data']['PostPage'][0]['graphql']['shortcode_media']
        
        try:
            commentCount = post_json['edge_media_preview_comment']['count']
        except:
            commentCount = np.nan
            
        try:
            likes = post_json['edge_media_preview_like']['count']
        except:
            likes = np.nan
        
        try:
            views = post_json['video_view_count']
        except:
            views = np.nan
        
        try:
            uploadDate = post_json['uploadDate'].split('T')[0]
        except:
            uploadDate = np.nan
            
        try:
            caption = post_json['edge_media_to_caption']['edges'][0]['node']['text']
        except:
            caption = np.nan
    
    post_dict = {'igtv_code': igtv_code,
                 'ig_handle': ig_handle,
                 'comment_count': commentCount,
                 'likes': likes,
                 'views': views,
                 'caption': caption,
                 'upload_date': uploadDate}
    
    return pd.DataFrame(post_dict, index = [0])

#%%
post_url = 'https://www.instagram.com/tv/B0_RD72Bkll/'

post_soup = bs(requests.get(post_url).text, 'lxml')
    
post_json = post_soup.find('script', type = 'application/ld+json')