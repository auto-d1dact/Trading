# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 22:16:27 2020

@author: Fang
"""

import pip
import time
import os
import numpy as np
import pandas as pd
from operator import itemgetter 
from selenium.webdriver import ActionChains
    
try:
    from selenium import webdriver
except ImportError:
    pip.main(['install', 'selenium'])
    from selenium import webdriver


def clear():
    os.system( 'cls' )

# Progress bar function unrelated to web scraping
def update_progress(progress, desc_string):
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

    clear()
    text = "Progress: [{0}] {1:.1f}% Last Post: {2}".format( "#" * block + "-" * (bar_length - block), 
                                                                   progress * 100,
                                                                   desc_string)
    print(text)


chrome_dir = os.getcwd()

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

#%%
# Function for taking list of post webElements and extracting data into dataframe
def get_post_df(browser, posts, code_sleep_time = 0.1):

    post_df = pd.DataFrame(columns = ['title','vid_length','approx_views','approx_comments','vid_url'])
    
    failed_posts = pd.DataFrame(columns = ['title','vid_url'])
    
    start_time = time.time()

    for i, post in enumerate(posts):
        try:
            actions = ActionChains(browser)
            title = post.find_element_by_class_name('pu1E0').text
            vid_length = post.find_element_by_class_name('zncDM').text

            actions.move_to_element(post).perform()
            browser.implicitly_wait(15)
            time.sleep(code_sleep_time)
            eng_elements = post.find_elements_by_tag_name('li')

            for element in eng_elements:
                el_val, el_class = element.find_elements_by_tag_name('span')

                el_val = el_val.text
                el_class = el_class.get_attribute('class')

                if 'Play' in el_class:
                    approx_views = el_val
                elif 'Speech' in el_class:
                    approx_comments = el_val
                else:
                    approx_views = np.nan
                    approx_comments = np.nan

            vid_url = post.get_attribute('href')

            curr_row = pd.DataFrame({'title': title,
                                     'vid_length': vid_length,
                                     'approx_views': approx_views,
                                     'approx_comments': approx_comments,
                                     'vid_url': vid_url},
                                    index = [i])[['title','vid_length','approx_views','approx_comments','vid_url']]

            post_df = post_df.append(curr_row)
            actions.move_by_offset(-post.size['width']/2 - 1,-post.size['height']/2 - 1).perform()
        except:
            title = post.find_element_by_class_name('pu1E0').text
            vid_url = post.get_attribute('href')
            curr_failed_row = pd.DataFrame({'title': title,
                                            'vid_url': vid_url},
                                           index = [i])
            failed_posts = failed_posts.append(curr_failed_row)
        
        browser.implicitly_wait(15)
        time.sleep(code_sleep_time)
        run_time = round(time.time() - start_time, 2)

        update_progress((i + 1)/len(posts), '{0} Seconds {1} '.format(run_time, '{0}'.format(title)))
    
    return post_df, failed_posts

#%%
# Function for scraping entire IGTV channel for post data
def scrape_igtv(show, return_df = False, code_sleep_time = 0.1):

    site = 'https://www.instagram.com/{show_alias}/channel/?hl=en'.format(show_alias = show)

    browser = webdriver.Chrome(executable_path = chrome_dir + "\\chromedriver.exe", options=options)

    browser.get(site)
    
    browser.delete_all_cookies()
    
    time.sleep(5)
    browser.implicitly_wait(30)
    
    posts = browser.find_elements_by_class_name('_bz0w')
    posts = list(filter(lambda x: x.get_attribute('href') != None, posts))
    post_df, failed_posts = get_post_df(browser, posts)

    while True:
        curr_posts = browser.find_elements_by_class_name('_bz0w')
        indices_check = pd.DataFrame(list(filter(lambda x: x != None, [p.get_attribute('href') for p in curr_posts])), 
                                     columns = ['url']).merge(post_df[['vid_url']],
                                                              how = 'left',
                                                              left_on = ['url'],
                                                              right_on = 'vid_url')
        indices_check = indices_check[indices_check.vid_url.isnull()].index.tolist()

        if indices_check != []:
            if len(indices_check) == 1:
                curr_posts = [itemgetter(*indices_check)(curr_posts)]
            else:
                curr_posts = itemgetter(*indices_check)(curr_posts)
                
            curr_df, curr_failed_posts = get_post_df(browser, curr_posts)
            post_df = post_df.append(curr_df)
            failed_posts = failed_posts.append(curr_failed_posts)
            browser.implicitly_wait(15)
            time.sleep(code_sleep_time)
        else:
            break

    os.chdir('scraped_data')

    post_df.reset_index(drop = True).to_csv('{show_alias}.csv'.format(show_alias = show))

    os.chdir(chrome_dir)

    browser.close()
    
    if return_df:
        return post_df
    else:
        return