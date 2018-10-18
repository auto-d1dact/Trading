# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 16:01:12 2018

@author: Fang

functions list:
    
    maturities(dt.datetime()) --> [float(front_wgt), float(back_wgt)]
    
    optionslam_scrape(str[ticker]) --> dict[earnings]
    
    yahoo_table_parse(str[raw_html_table]) --> DataFrame[ticker]
    
    yahoo_earnings(dt.datetime()) --> DataFrame[earnings_on_date]
    
    fundamentals(str[ticker]) --> DataFrame[stock_fundamentals]
    
    get_fundas(list[ticker_lst]) --> DataFrame[stock_fundamentals]
    
    historical_data(str[ticker], int[day_number], int[rolling_window], outsize[str]) --> DataFrame[daily_stock_data]
    
    current_volatility(list[ticker_lst], int[roll]) --> DataFrame[stock_volatilities]
    
    all_options(str[ticker], bool[greeks]) --> DataFrame[options_chains]
    
    earnings_condor(str[ticker], int[max_gap], int[dte_thresh], float[|money_thresh| <= 1]) -- DataFrame[condors], DataFrame[puts], DataFrame[calls]
    
    write_excel(str[filename], list[str[sheetnames]], list[dataframes]) --> void()
    
    curr_stock_data(str[ticker]) --> DataFrame[stock_info]
    
    curr_batch_quotes(list_of_string[tickers]) --> DataFrame[stock_info]

    past_earnings(str[ticker]) --> DataFrame[earnings_info]

    earnings_history(str[ticker]) --> [DataFrame[earnings_estimate], DataFrame[past_earnings], DataFrame[earnings_estimate_change]]
    
    av_data(str[ticker]) --> DataFrame[ticker_open, ticker_close]

    av_batch(list_of_str[tickers]) --> DataFrame[tickers_closes]

    check_mkt_corr(int[corr_rolling_window],int[plot_window]) --> DataFrame[rolling_corr]

    vvix_check() --> DataFrame[VVIX Data]

    earnings_longs(list_of_str[ticker], float[bid_ask_spread]) --> DataFrame[option_chains]

    all_options_v2(str[ticker], int[dte_ub], int[dte_lb], float[moneyness]) --> DataFrame[option_chains]

    yahoo_options_query(str[ticker], int[dte_ub], int[dte_lb]) --> DataFrame[option_chains]

    greek_calc(DataFrame[option_chain], str[prem_price_use], str[day_format], float[interest_rate], float[dividend_rate])

    price_sim(DataFrame[options_df], float[price_change], float[vol_change], int[days_change], str[output = 'All'],
              str[skew = 'flat'], str[day_format = 'trading'], float[interest_rate = 0.0193], float[dividend_rate = 0],
              float[prem_price_use = 'Mid'])


    position_sim(DataFrame[position_df], list_of_int[holdings], int[shares],
                 float[price_change], float[vol_change], int[dte_change], str[output = 'All'],
                 str[skew = 'flat'], str[prem_price_use = 'Mid'], str[day_format = 'trading'], 
                 float[interest_rate = 0.0193], float[dividend_rate = 0])
	
	yahoo_fundamentals(list_of_str[tickers]) --> DataFrame[fundamentals]
    
   spx_spreads(int[dte_ub], int[dte_lb],
               str[price = 'Market'], float[moneyness], 
               int[max_strike_distance], 
               float[max_exposure], 
               float[commission = 3]) --> DataFrame[spx spreads]
"""

# Note to import from .py files, must follow structure
# from <.py filename excluding '.py'> import <class name>
# Optionslam creds: aspringfastlaner Options2018

# Importing necessary models
import smtplib
import pandas as pd
import numpy as np
import datetime as dt
import pandas.stats.moments as st
from pandas import ExcelWriter
import matplotlib.pyplot as plt
import os
import seaborn as sns
import matplotlib.dates as dates
# import matplotlib.ticker as ticker
from lxml import html
import requests
import webbrowser
from bs4 import BeautifulSoup as bs
import json
from pandas.io.json import json_normalize
import csv
import sched, time
import pandas_datareader as datareader
from pandas_datareader.data import Options
from alpha_vantage.timeseries import TimeSeries
#ts = TimeSeries(key='5HZEUI5AFJB06BUK',output_format='pandas')
#key2 = '6ZAZOL7YF8VPXND7'
import matplotlib.pyplot as plt
import urllib.request as urlreq
from collections import OrderedDict
import statsmodels.formula.api as sm


import plotly.plotly as py
import plotly.graph_objs as go

from pandas_datareader.data import Options
import py_vollib
from py_vollib.black_scholes_merton.implied_volatility import *
from py_vollib.black_scholes_merton.greeks.analytical import *

import quandl as qd
qd.ApiConfig.api_key = 'dzmzEExntfap7SNx5p6t'


def maturities(date):
    
    # Calculate today, but note that since we are adjusting for lookback bias, we need to change the current date to one day prior
    today = date
    curr_month = today.month
    curr_year = today.year
    
    # Finding Prev Third Wed
    curr_eigth_day = dt.date(curr_year,curr_month,7)
    curr_second_day = dt.date(curr_year,curr_month,3).weekday()
    curr_third_fri = curr_eigth_day - dt.timedelta(curr_second_day) + dt.timedelta(14)
    last_third_wed = curr_third_fri - dt.timedelta(30)
    
    # Finding Next Third Wed
    if curr_month == 12:
        next_month = 2
        next_year = curr_year + 1
    elif curr_month == 11:
        next_month = 1
        next_year = curr_year + 1
    else:
        next_month = curr_month + 2
        next_year = curr_year
    next_eigth_day = dt.date(next_year,next_month,7)
    next_second_day = dt.date(next_year,next_month,3).weekday()
    next_third_fri = next_eigth_day - dt.timedelta(next_second_day) + dt.timedelta(14)
    next_third_wed = next_third_fri - dt.timedelta(30)
    
    # Finding Cur Third Wed
    if curr_month == 12:
        next_month = 1
        next_year = curr_year + 1
    else:
        next_month = curr_month + 1
        next_year = curr_year
    next_eigth_day = dt.date(next_year,next_month,7)
    next_second_day = dt.date(next_year,next_month,3).weekday()
    next_third_fri = next_eigth_day - dt.timedelta(next_second_day) + dt.timedelta(14)
    curr_third_wed = next_third_fri - dt.timedelta(30)
    
    # Finding Term: When current date is after expiry, should be 100% of spot/f1
    if today < curr_third_wed:
        dte = curr_third_wed - today
        term = curr_third_wed - last_third_wed
    else:
        dte = next_third_wed - today
        term = next_third_wed - curr_third_wed
    # print (float(dte.days)/term.days)
    front_weight = float(dte.days)/term.days
    back_weight = 1 - front_weight
    return [front_weight, back_weight]


'''
Function for pulling implied volatility from option slam for single ticker
'''

def optionslam_scrape(ticker):
    site = 'https://www.optionslam.com/earnings/stocks/' + ticker
    res = requests.get(site)
    soup = bs(requests.get(site).text, "lxml")
    soup = soup.prettify()
    earnings_dict = {'Ticker': ticker}
    
    # Check if there's weekly options
    curr7_implied = "Current 7 Day Implied Movement:"
    implied_move_weekly = "Implied Move Weekly:"
    nextearnings = "Next Earnings Date:"
    if curr7_implied not in soup:
        return 'No Weeklies'
    
    # Parsing if weekly options exist
    # Next earnings date and before or after
    earnings_start_string = "Next Earnings Date:"
    earnings_end_string = '</font>'
    raw_earnings_string = (soup.split(earnings_start_string))[1].split(earnings_end_string)[0].replace('\n','').strip()
    
    try:
        earnings_date = str((raw_earnings_string.split('<b>'))[1].split('<font size="-1">')).split("'")[1].strip()
    except:
        return 'Error Parsing'
    
    earnings_time = str(raw_earnings_string[-2:].strip()).strip()
    
    earnings_dict['Date'] = earnings_date
    earnings_dict['Earnings Time'] = earnings_time
    
    # Parsing 7 day implied move if weekly option exists
    ending_string = '<font size="-2">'
    curr_7 = (soup.split(curr7_implied))[1].split(ending_string)[0].replace('\n','').strip("").split("<td>")[-1].strip()
    earnings_dict['Current 7 Day Implied'] = curr_7
    
    # Parsing Weekly Implied move if weekly option exists
    if implied_move_weekly in soup:
        weekly_implied = (soup.split(implied_move_weekly))[1].split(ending_string)[0].replace('\n','').strip("").split("<td>")[-1].strip()
    else:
        weekly_implied = ''
    earnings_dict["Implied Move Weekly"] = weekly_implied
    
    return earnings_dict

def yahoo_table_parse(raw_html_table):
    tickers = []
    call_times = []
    implied_7_day = []
    implied_weekly = []
    eps = []
    i = 1
    end_row = 10
    for row in raw_html_table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
        individual_row = str(row).split('\n')
        row_items = individual_row[0].split('</span>')[:3]

        if i == 1:
            i += 1
            continue
        tick = row_items[0].split('data-symbol="')[1].split('"')[0]
        os_check = optionslam_scrape(tick)

        if type(os_check) == str:
            continue
        else:
            tickers.append(tick)
            call_times.append(row_items[0].split('data-symbol="')[1].split('"')[-1].replace('>',''))
            eps.append(row_items[1].split('</td>')[1].split('>')[1])
            
            try:
                implied_7 = float(os_check['Current 7 Day Implied'].replace('%',''))
            except:
                implied_7 = os_check['Current 7 Day Implied'].replace('%','')
                
            try:
                implied_week = float(os_check['Implied Move Weekly'].replace('%',''))
            except:
                implied_week = os_check['Implied Move Weekly'].replace('%','')
                
            implied_7_day.append(implied_7)
            implied_weekly.append(implied_week)


    return pd.DataFrame({'Tickers': tickers, 'Call Times': call_times, 'EPS': eps,
                         'Current 7 Day Implied': implied_7_day,
                         'Implied Move Weekly': implied_weekly})


def yahoo_earnings(date):
    # Yahoo Earnings Calendar Check

    today = date.strftime('%Y-%m-%d')
    tables = []
    no_tables = False
    for i in range(6):
        if no_tables:
            break
        yahoo_url = 'https://finance.yahoo.com/calendar/earnings?day=' + today + '&offset={}&size=100'.format(int(i*100))
        soup = bs(requests.get(yahoo_url).text, "lxml")
        
        try:
            table = soup.find_all('table')[0]
            tables.append(yahoo_table_parse(table))
        except:
            print('No Table')
            no_tables = True

    return pd.concat(tables,axis = 0, ignore_index = True)

'''
Function for getting all relevant earnings for a given starting week (Monday in dt.datetime(YYYY, m, d) format)
Returns a dataframe with the earnings names, implied move, price, and earnings times.
'''
def weekly_earnings_check(start_datetime, days_forward):

    start_date = start_datetime

    weekly_earnings = []
    while start_date.weekday() < days_forward:
        try:
            temp_earnings = yahoo_earnings(start_date)
            temp_earnings['Earnings Date'] = start_date
#             temp_earnings['Last Close'] = 0
#             for idx, row in temp_earnings.iterrows():
#                 try:
#                     temp_earnings.loc[idx, 'Last Close'] = ts.get_daily(row['Tickers'])[0].tail(1)['close'][0]
#                 except:
#                     temp_earnings.loc[idx, 'Last Close'] = np.nan
            weekly_earnings.append(temp_earnings)
            start_date = start_date + dt.timedelta(1)
        except:
            start_date = start_date + dt.timedelta(1)

    earnings_df = pd.concat(weekly_earnings, axis = 0, ignore_index = True)
#     earnings_df = earnings_df[earnings_df['Last Close'] >= 20]
#     earnings_df['Lower Bound'] = np.round(earnings_df['Last Close']*(1 - earnings_df['Implied Move Weekly']/100),2)
#     earnings_df['Upper Bound'] = np.round(earnings_df['Last Close']*(1 + earnings_df['Implied Move Weekly']/100),2)
    earnings_df = earnings_df.sort_values(['Earnings Date','Call Times'])
    
    return earnings_df
    
def fundamentals(ticker):
    
    site = 'https://finance.yahoo.com/quote/{0}?p={0}'.format(ticker)

    res = requests.get(site)
    soup = bs(res.text, 'lxml')
    table = soup.find_all('table')[1]
    sum_dict = {}

    # Looping through the soup lxml text table format
    # and splitting each row as a individual string
    # and parsing string to retrieve the date,
    # open, and close information.


    for row in table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
        individual_row = str(row).split('\n')[0]

        # row_items is parsed string for each current row where each
        # item in list is the date, open, high, low, close, and volume
        row_items = individual_row.split('<span data-reactid=')[1].split('"><!-- react-text: ')
        
        if len(row_items) > 1:
            sum_item = row_items[0].split('>')[1].split('<')[0]
            sum_value = row_items[1].split('-->')[1].split('<')[0]
        elif 'YIELD' in row_items[0]:
            try:
                temp_val = row_items[0].split('-value">')[1].split("</td>")[0]
                div_amount = float(temp_val.split(' ')[0])
                div_yield = float(temp_val.split(' ')[1].replace('(','').replace(')','').replace('%',''))

                sum_dict['Div'] = div_amount
                sum_dict['Yield'] = div_yield
            except:
                sum_dict['Div'] = np.nan
                sum_dict['Yield'] = np.nan
        elif 'Market Cap' in row_items[0]:
            sum_item = 'Market Cap (B)'
            mkt_cap = row_items[0].split('data-reactid="')[-1].split('>')[1].split('<')[0]
            mkt_cap_amount = float(mkt_cap[:-1])
            if mkt_cap[-1] == 'M':
                sum_value = mkt_cap_amount/1000
            else:
                sum_value = mkt_cap_amount
        else:
            sum_item = row_items[0].split('>')[1].split('<')[0]
            sum_value = row_items[0].split('data-reactid="')[-1].split('>')[1].split('<')[0]
        
        sum_dict[sum_item] = sum_value
    
    sum_dict['Days Since Last Earnings'] = (dt.datetime.today().date() - 
                                            stock_earnings(ticker)['Earnings Dates'][1].date()).days

    return pd.DataFrame(sum_dict, index = [ticker])

# Function to return fundametal data of a ticker list
def get_fundas(ticker_lst):
    fund_lst = []
    for tick in ticker_lst:
        try:
            fund_lst.append(fundamentals(tick))
        except:
            continue
    return pd.concat(fund_lst,axis = 0)

# Function historical data from alpha advantage
def historical_data(ticker, day_number = 252, rolling_window = 20, outsize = 'full'):
    alphavantage_link = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&apikey=6ZAZOL7YF8VPXND7&datatype=csv&outputsize={1}'.format(ticker, outsize)
    stockframe = pd.read_csv(alphavantage_link, index_col = 0).sort_index()[['open', 'close']]
    stockframe['daily_ret'] = np.log(stockframe['close']/stockframe['close'].shift(1))
    stockframe['intra_ret'] = np.log(stockframe['close']/stockframe['open'])
    stockframe['ovrnt_ret'] = np.log(stockframe['open']/stockframe['close'].shift(1))
    stockframe['daily_vol'] = stockframe.daily_ret.rolling(window=rolling_window,center=False).std()
    stockframe['intra_vol'] = stockframe.intra_ret.rolling(window=rolling_window,center=False).std()
    stockframe['ovrnt_vol'] = stockframe.ovrnt_ret.rolling(window=rolling_window,center=False).std()
    stockframe['daily_ann'] = stockframe.daily_vol*np.sqrt(252)
    stockframe['intra_ann'] = stockframe.intra_vol*np.sqrt((24/6.5)*252)
    stockframe['ovrnt_ann'] = stockframe.ovrnt_vol*np.sqrt((24/17.5)*252)
    stockframe['oc_diff'] = stockframe.close - stockframe.open
    stockframe['daily_dollar_vol'] = stockframe.daily_vol*stockframe.close.shift(1)
    stockframe['daily_dollar_std'] = np.abs(stockframe.oc_diff/stockframe.daily_dollar_vol)

    return stockframe.tail(day_number)

# Function for building a dataframe of volatilities
# Daily, Intraday, Overnight
def current_volatility(ticker_list, roll = 20):
    
    rows = []
    failed_tickers = []
    
    def failed_check(failed_lst,rows):
        if len(failed_lst) == 0:
            return failed_lst, rows
        else:
            new_lst = []
            new_rows = rows
            for tick in failed_lst:
                try: 
                    curr_vol = historical_data(tick, outsize = 'compact').tail(1)[['daily_ann','intra_ann','ovrnt_ann','close',
                                                                                   'daily_dollar_vol']]
                    curr_vol.index.name = 'Tickers'
                    curr_vol.index = [tick]
                    new_rows.append(curr_vol)
                except:
                    new_lst.append(tick)
            return failed_check(new_lst, rows)

    for tick in ticker_list:
        try: 
            curr_vol = historical_data(tick, outsize = 'compact').tail(1)[['daily_ann','intra_ann','ovrnt_ann','close',
                                                                           'daily_dollar_vol']]
            curr_vol.index.name = 'Tickers'
            curr_vol.index = [tick]
            rows.append(curr_vol)
        except:
            failed_tickers.append(tick)
            
    failed_lst, rows = failed_check(failed_tickers, rows)
        
    return pd.concat(rows, axis = 0)

def all_options_old(ticker, greeks = True):
    tape = Options(ticker, 'yahoo')
    data = tape.get_all_data().reset_index()
    
    data['Moneyness'] = np.abs(data['Strike'] - data['Underlying_Price'])/data['Underlying_Price']
    
    data['DTE'] = (data['Expiry'] - dt.datetime.today()).dt.days
    data = data[['Strike', 'DTE', 'Type', 'IV', 'Vol','Open_Int', 'Moneyness', 'Root', 'Underlying_Price',
                 'Last','Bid','Ask']]
    data['Mid'] = (data['Ask'] - data['Bid'])/2 + data['Bid']
    
    if greeks:
        year = 365
        strikes = data['Strike'].values
        time_to_expirations = data['DTE'].values
        #ivs = data['IV'].values
        underlying = data['Underlying_Price'].values[0]
        types = data['Type'].values
    
        # Make sure nothing thows up
        assert len(strikes) == len(time_to_expirations)
    
        sigmas = data['IV']
        deltas = []
        gammas = []
        thetas = []
        vegas = []
        for sigma, strike, time_to_expiration, flag in zip(sigmas, strikes, time_to_expirations, types):
    
            # Constants
            S = underlying
            K = strike
            t = time_to_expiration/float(year)
            r = 0.005 / 100
            q = 0 / 100
    
            try:
                delta = py_vollib.black_scholes_merton.greeks.analytical.delta(flag[0], S, K, t, r, sigma, q)
                deltas.append(delta)
            except:
                delta = 0.0
                deltas.append(delta)
    
            try:
                gamma = py_vollib.black_scholes_merton.greeks.analytical.gamma(flag[0], S, K, t, r, sigma, q)
                gammas.append(gamma)
            except:
                gamma = 0.0
                gammas.append(gamma)
    
            try:
                theta = py_vollib.black_scholes_merton.greeks.analytical.theta(flag[0], S, K, t, r, sigma, q)
                thetas.append(theta)
            except:
                theta = 0.0
                thetas.append(theta)
    
            try:
                vega = py_vollib.black_scholes_merton.greeks.analytical.vega(flag[0], S, K, t, r, sigma, q)
                vegas.append(vega)
            except:
                vega = 0.0
                vegas.append(vega)
    
        data['Delta'] = deltas
        data['Gamma'] = gammas
        data['Theta'] = thetas
        data['Vega'] = vegas

    return data.reset_index()[data.columns]#data.dropna().reset_index()[data.columns]


def earnings_condor(tick, max_gap, dte_thresh, money_thresh):
    chain = all_options_old(tick)
    chain = chain[chain['DTE'] <= dte_thresh]
    chain = chain.reset_index()[chain.columns]
    chain = chain[chain['Moneyness'] <= money_thresh]
    chain_puts = chain[(chain['Type'] == 'put') & (chain['Strike'] < chain['Underlying_Price'].values[0])]
    chain_calls = chain[(chain['Type'] == 'call') & (chain['Strike'] > chain['Underlying_Price'].values[0])]


    put_spread_prem = []
    put_spread_delta = []
    put_spread_gamma = []
    put_spread_vega = []
    put_spread_theta = []
    put_spread_short_strike = []
    put_spread_long_strike = []
    put_spread_max_loss = []
    for idx, row in chain_puts.sort_values('Strike', ascending = False).iterrows():
        curr_short_strike = row.Strike
        curr_short_prem = row.Bid
        curr_short_delta = row.Delta
        curr_short_gamma = row.Gamma
        curr_short_vega = row.Vega
        curr_short_theta = row.Theta

        temp_longs = chain_puts[(chain_puts['Strike'] < curr_short_strike) &
                                (chain_puts['Strike'] >= curr_short_strike - max_gap)]

        for temp_idx, temp_row in temp_longs.iterrows():
            curr_long_strike = temp_row.Strike
            curr_long_prem = temp_row.Ask
            curr_long_delta = temp_row.Delta
            curr_long_gamma = temp_row.Gamma
            curr_long_vega = temp_row.Vega
            curr_long_theta = temp_row.Theta

            curr_spread_prem = curr_short_prem - curr_long_prem
            curr_spread_delta = -curr_short_delta + curr_long_delta
            curr_spread_gamma = -curr_short_gamma + curr_long_gamma
            curr_spread_vega = -curr_short_vega + curr_long_vega
            curr_spread_theta = -curr_short_theta + curr_long_theta
            curr_spread_maxloss = (curr_short_strike - curr_long_strike - curr_spread_prem)*100

            put_spread_prem.append(curr_spread_prem)
            put_spread_delta.append(curr_spread_delta)
            put_spread_gamma.append(curr_spread_gamma)
            put_spread_vega.append(curr_spread_vega)
            put_spread_theta.append(curr_spread_theta)
            put_spread_short_strike.append(curr_short_strike)
            put_spread_long_strike.append(curr_long_strike)
            put_spread_max_loss.append(curr_spread_maxloss)

    put_spreads_df = pd.DataFrame(OrderedDict({'put Combo': range(len(put_spread_prem)),
                                               'Short Put Strike': put_spread_short_strike,
                                               'Long Put Strike': put_spread_long_strike,
                                               'put Spread Premium': put_spread_prem,
                                               'put Spread Maxloss': put_spread_max_loss,
                                               'put Spread Delta': put_spread_delta,
                                               'put Spread Gamma': put_spread_gamma,
                                               'put Spread Vega': put_spread_vega,
                                               'put Spread Theta': put_spread_theta}),
                                  index = range(len(put_spread_prem)))

    call_spread_prem = []
    call_spread_delta = []
    call_spread_gamma = []
    call_spread_vega = []
    call_spread_theta = []
    call_spread_short_strike = []
    call_spread_long_strike = []
    call_spread_max_loss = []
    for idx, row in chain_calls.sort_values('Strike', ascending = True).iterrows():
        curr_short_strike = row.Strike
        curr_short_prem = row.Bid
        curr_short_delta = row.Delta
        curr_short_gamma = row.Gamma
        curr_short_vega = row.Vega
        curr_short_theta = row.Theta

        temp_longs = chain_calls[(chain_calls['Strike'] > curr_short_strike) &
                                (chain_calls['Strike'] <= curr_short_strike + max_gap)]

        for temp_idx, temp_row in temp_longs.iterrows():
            curr_long_strike = temp_row.Strike
            curr_long_prem = temp_row.Ask
            curr_long_delta = temp_row.Delta
            curr_long_gamma = temp_row.Gamma
            curr_long_vega = temp_row.Vega
            curr_long_theta = temp_row.Theta

            curr_spread_prem = curr_short_prem - curr_long_prem
            curr_spread_delta = -curr_short_delta + curr_long_delta
            curr_spread_gamma = -curr_short_gamma + curr_long_gamma
            curr_spread_vega = -curr_short_vega + curr_long_vega
            curr_spread_theta = -curr_short_theta + curr_long_theta
            curr_spread_maxloss = -(curr_short_strike - curr_long_strike + curr_spread_prem)*100

            call_spread_prem.append(curr_spread_prem)
            call_spread_delta.append(curr_spread_delta)
            call_spread_gamma.append(curr_spread_gamma)
            call_spread_vega.append(curr_spread_vega)
            call_spread_theta.append(curr_spread_theta)
            call_spread_short_strike.append(curr_short_strike)
            call_spread_long_strike.append(curr_long_strike)
            call_spread_max_loss.append(curr_spread_maxloss)

    call_spreads_df = pd.DataFrame(OrderedDict({'call Combo': range(len(call_spread_prem)),
                                               'Short call Strike': call_spread_short_strike,
                                               'Long call Strike': call_spread_long_strike,
                                               'call Spread Premium': call_spread_prem,
                                               'call Spread Maxloss': call_spread_max_loss,
                                               'call Spread Delta': call_spread_delta,
                                               'call Spread Gamma': call_spread_gamma,
                                               'call Spread Vega': call_spread_vega,
                                               'call Spread Theta': call_spread_theta}),
                                  index = range(len(call_spread_prem)))

    #put_combos = []
    #call_combos = []
    condor_prems = []
    condor_maxloss = []
    condor_delta = []
    condor_gamma = []
    condor_vega = []
    condor_theta = []
    put_short = []
    put_long = []
    call_short = []
    call_long = []

    for idxc, rowc in call_spreads_df.iterrows():
        for idxp, rowp in put_spreads_df.iterrows():
            p_s = put_spreads_df[put_spreads_df['put Combo'] == rowp['put Combo']]['Short Put Strike'].values[0]
            p_l = put_spreads_df[put_spreads_df['put Combo'] == rowp['put Combo']]['Long Put Strike'].values[0]
            c_s = call_spreads_df[call_spreads_df['call Combo'] == rowc['call Combo']]['Short call Strike'].values[0]
            c_l = call_spreads_df[call_spreads_df['call Combo'] == rowc['call Combo']]['Long call Strike'].values[0]

            put_short.append(p_s)
            put_long.append(p_l)
            call_short.append(c_s)
            call_long.append(c_l)

            curr_prem = round(rowp['put Spread Premium'] + rowc['call Spread Premium'],2)

            condor_prems.append(curr_prem)
            condor_maxloss.append(100*(max(p_s - p_l, c_l - c_s) - curr_prem))
            condor_delta.append(rowp['put Spread Delta'] + rowc['call Spread Delta'])
            condor_gamma.append(rowp['put Spread Gamma'] + rowc['call Spread Gamma'])
            condor_vega.append(rowp['put Spread Vega'] + rowc['call Spread Vega'])
            condor_theta.append(rowp['put Spread Theta'] + rowc['call Spread Theta'])

    condors_df = pd.DataFrame(OrderedDict({'P Short Strike': put_short,
                                           'P Long Strike': put_long,
                                           'C Short Strike': call_short,
                                           'C Long Strike': call_long,
                                           'Premium': condor_prems,
                                           'Maxloss': condor_maxloss,
                                           'Delta': condor_delta,
                                           'Gamma': condor_gamma,
                                           'Vega': condor_vega,
                                           'Theta': condor_theta}),
                                  index = range(len(condor_prems)))
    condors_df['RiskRewardRatio'] = round((100*condors_df['Premium'])/condors_df['Maxloss'],2)
    put_spreads_df['RiskRewardRatio'] = round((100*put_spreads_df['put Spread Premium'])/put_spreads_df['put Spread Maxloss'],2)
    call_spreads_df['RiskRewardRatio'] = round((100*call_spreads_df['call Spread Premium'])/call_spreads_df['call Spread Maxloss'],2)
    condors_df['Underlying Price'] = chain['Underlying_Price'].values[0]
    
    return condors_df, put_spreads_df, call_spreads_df


def write_excel(filename, sheetnames, df_list):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    for i, df in enumerate(df_list):
        
        df.to_excel(writer, sheet_name = sheetnames[i])

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    return

def curr_stock_data(ticker):
    yahoo_url = 'https://finance.yahoo.com/quote/{0}/history?p={0}'.format(ticker)

    soup = bs(requests.get(yahoo_url).text, "lxml")

    table = soup.find_all('table')[0]

    i = 0

    for row in table.find_all('tr'):
        if i == 2:
            break
        else:
            individual_row = str(row).split('\n')
            i += 1

    curr_svxy = [float(x.split('>')[-1]) for x in individual_row[0].split('</span>')[1:-2]]
    
    return curr_svxy

def curr_batch_quotes(tickerlst):
    lst_string = str(tickerlst).replace('[','').replace(']','').replace(' ','').replace("'",'')
    av_link = 'https://www.alphavantage.co/query?function=BATCH_STOCK_QUOTES&symbols={0}&apikey=APIKEY=5HZEUI5AFJB06BUK&datatype=csv'.format(lst_string)
    df = pd.read_csv(av_link, index_col = 0)
    return df

def past_earnings(ticker):
    
    site = 'https://www.optionslam.com/earnings/stocks/' + ticker
    soup = bs(requests.get(site).text, "lxml")
    table = soup.find_all('table')[10]

    i = 1
    earnings_dates = []
    earnings_times = []
    
    for row in table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
    #     individual_row = 
    #     row_items = individual_row[0].split('</span>')[:3]
    
        if i >= 5:

            try:
                earnings_date = str(row).split('<td nowrap="">')[1].split('<font size="-1">')[0].replace('.','')
                earnings_dates.append(dt.datetime.strptime(earnings_date.strip(), '%b %d, %Y'))
                earnings_times.append(str(row).split('<font size="-1">')[1].split('</font>')[0].strip())
            except:
                try:
                    earnings_date = str(row).split('<td nowrap="">')[1].split('<font size="-1">')[0].replace('.','')
                    earnings_dates.append(dt.datetime.strptime(earnings_date.strip(), '%B %d, %Y'))
                    earnings_times.append(str(row).split('<font size="-1">')[1].split('</font>')[0].strip())
                except:
                    break

        i += 1

    alphavantage_link = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&apikey=6ZAZOL7YF8VPXND7&datatype=csv&outputsize={1}'.format(ticker, 'full')
    stockframe = pd.read_csv(alphavantage_link, index_col = 0).sort_index()[['open', 'close']]
    stockframe['PostEarningsReturn'] = np.log(stockframe['close']/stockframe['close'].shift(1))
    stockframe['1Year'] = stockframe.close.pct_change(252).shift(1)
    stockframe['6month'] = stockframe.close.pct_change(126).shift(1)
    stockframe['3month'] = stockframe.close.pct_change(63).shift(1)
    stockframe['1month'] = stockframe.close.pct_change(20).shift(1)
    stockframe.index = pd.to_datetime(stockframe.index)

    earnings_returns = []

    for earnings_date, earnings_time in zip(earnings_dates, earnings_times):
        if earnings_time == 'BO':
            earnings_returns.append(stockframe.loc[stockframe.index >= earnings_date].head(1))
        else:
            earnings_returns.append(stockframe.loc[stockframe.index > earnings_date].head(1))

    earnings_df = pd.concat(earnings_returns, axis = 0)
    
    return earnings_df

def earnings_history(ticker):
    yahoo_url = 'https://finance.yahoo.com/quote/{0}/analysts?p={0}'.format(ticker)
    soup = bs(requests.get(yahoo_url).text, "lxml")
    table = soup.find_all('table')
    
    # Earnings Estimates
    raw_html_table = table[0]

    i = 0
    headers = ['Current Qtr', 'Next Qtr', 'Current Year', 'Next Year']
    idx = ['No. of Analysts', 'Avg. Estimate', 'Low Estimate', 'High Estimate', 'Year Ago EPS']
    earnings_df = pd.DataFrame(columns = headers, index = idx)
    for row in raw_html_table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
        individual_row = str(row).split('\n')
        if i != 0:
            split_row = individual_row[0].split('</span></td><')
            col = 0
            for j in split_row[1:-1]:
                earnings_df.iloc[i-1,col] = float(j.split('>')[-1].replace('%','').replace('N/A','nan'))
                col += 1

        i += 1

    raw_html_table = table[2]

    i = 0
    idx = ['EPS Est.', 'EPS Actual', 'Difference', 'Surprise %']

    for row in raw_html_table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
        individual_row = str(row).split('\n')
        if i == 0:
            cols = [x.split('>')[-1] for x in individual_row[0].split('</span></th>')[1:-1]]
            earnings_history = pd.DataFrame(columns = cols, index = idx)
        if i != 0:
            split_row = individual_row[0].split('</span></td><')
            col = 0
            for j in split_row[1:-1]:
                earnings_history.iloc[i-1,col] = float(j.split('>')[-1].replace('%','').replace('N/A','nan'))
                col += 1

        i += 1
        
    # EPS Trend
    raw_html_table = table[3]

    i = 0
    idx = ['Current Estimate', '7 Days Ago', '30 Days Ago',
           '60 Days Ago', '90 Days Ago']
    eps_trend = pd.DataFrame(columns = headers, index = idx)
    for row in raw_html_table.find_all('tr'):
        # Individual row stores current row item and delimits on '\n'
        individual_row = str(row).split('\n')
        if i != 0:
            split_row = individual_row[0].split('</span></td><')
            col = 0
            for j in split_row[1:-1]:
                eps_trend.iloc[i-1,col] = float(j.split('>')[-1].replace('%','').replace('N/A','nan'))
                col += 1

        i += 1
        
    return [earnings_df, earnings_history, eps_trend]


def earnings_long_single(ticker, bid_ask_spread):
    chain = all_options_old(ticker)
    intra_vol = current_volatility([ticker])['intra_ann'][0]
    chain = chain[(abs(chain['Delta']) <= 0.5) &
                  (abs(chain['Delta']) >= 0.3) &
                  (chain['IV'] <= intra_vol)]
    potential_longs = chain[chain['DTE'] == chain['DTE'].sort_values().values[0]]
    potential_longs[potential_longs['Ask'] - potential_longs['Bid'] <= bid_ask_spread]
    potential_longs['HV'] = intra_vol
    return potential_longs

def earnings_longs(tickers, bid_ask_spread):
    all_chains = []
    for ticker in tickers:
        curr_long = earnings_long_single(ticker, bid_ask_spread)
        if len(curr_long) > 0:
            all_chains.append(curr_long)
    chains = pd.concat(all_chains, axis = 0)
    chains = chains.reset_index()[chains.columns]
    return chains

def av_data(ticker):
    outsize = 'full'
    alphavantage_link = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&apikey=5HZEUI5AFJB06BUK&datatype=csv&outputsize={1}'.format(ticker, outsize)
    stockframe = pd.read_csv(alphavantage_link, index_col = 0).sort_index()[['open', 'close']]
    return stockframe

def av_batch(ticker_lst):
    lst = ticker_lst
    df_lst = []

    while len(lst) > 0:

        for ticker in lst:
            try:
                df = av_data(ticker)[['close']]
                df.columns = [ticker]
                df_lst.append(df)
                lst.remove(ticker)
            except:
                continue

    df = pd.concat(df_lst, axis = 1)
    return df
#%% 

'''
Newer and optimized functions

'''
def check_mkt_corr(rolling_window, plot_window):
    spdr_lst = ['SPY','XLU','XLRE','XLY','XLV',
                'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
                'XLP', 'XLE']

    df = av_batch(spdr_lst)
    df = df.pct_change()
    df_corr = df.rolling(rolling_window).corr(df['SPY'])
    del df_corr['SPY'], df_corr['XLC']
    df_corr = df_corr.dropna().tail(plot_window)
    df_corr['Avg_Corr'] = df_corr.mean(axis = 1)
    df_corr['SPY_cum'] = (df[['SPY']].tail(plot_window) + 1).cumprod() - 1

    plt.figure(figsize=(20,10))
    plt.xlabel('Date')

    ax1 = df_corr.SPY_cum.plot(color='blue', grid=True, label='SPY Return')
    ax2 = df_corr.Avg_Corr.plot(color='red', grid=True, secondary_y=True, label='Correlation')

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()


    plt.legend(h1+h2, l1+l2, loc=2)
    plt.show()
    return df_corr

def vvix_check():

    spx = av_data('SPX')
    spx.index = pd.to_datetime(spx.index)
    spx.columns = ['spx_open','spx_close']

    vix = pd.read_csv('http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vixcurrent.csv', skiprows = 1,
                     index_col = 0)
    vix.index = pd.to_datetime(vix.index)
    vvix = pd.read_csv('http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vvixtimeseries.csv', skiprows = 1,
                      index_col = 0)
    vvix.index = pd.to_datetime(vvix.index)

    df = pd.concat([spx,vix,vvix], axis = 1).dropna()
    df['VIX_Return'] = df['VIX Close'].pct_change()
    df['VVIX_Return'] = df['VVIX'].pct_change()
    df = df.dropna()

    vix_spike_thresh = 0.05
    days_since_spike_count = 0
    days_since_spike = []
    vvix_stdev = []
    for idx,row in df.iterrows():
        if row.VIX_Return >= vix_spike_thresh:
            days_since_spike_count = 0
        else:
            days_since_spike_count += 1
        days_since_spike.append(days_since_spike_count)

        vvix_stdev.append(df[['VVIX_Return']].rolling(days_since_spike_count).std()[df.index == idx]['VVIX_Return'][0])

    df['Days_Since_Spike'] = days_since_spike
    df['VVIX_Stdev'] = vvix_stdev
    df['VVIX_Move'] = np.round(np.abs(df['VVIX_Return']/df['VVIX_Stdev']),2)
    df['VVIX_Move'] = df['VVIX_Move'].shift(1)

    out_df = df[['spx_close','VIX Close','VVIX','VIX_Return','VVIX_Move','Days_Since_Spike']]
    out_df['SPX 200 SMA'] = np.round(out_df['spx_close'].rolling(200).mean(), 2)
    out_df['SPX 20 SMA'] = np.round(out_df['spx_close'].rolling(20).mean(), 2)
    out_df['VIX_Return'] = np.round(out_df['VIX_Return'], 4)
    out_df['spx_close'] = np.round(out_df['spx_close'], 2)
    out_df.columns = ['SPX Close', 'VIX Close', 'VVIX', 'VIX Return', 
                      'VVIX Move', 'Days Since Spike', 'SPX 200 SMA', 'SPX 20 SMA']
    return out_df.tail(60)


#%% BSM Functions
from scipy.stats import norm as norm


def d1(options_df, interest_rate = 0.0193, q = 0, year = 252):
    numerator = np.log(options_df['Underlying_Price'] / 
                       options_df['Strike']) + ((interest_rate - q) + options_df['IV']**2 / 2.0) * options_df['DTE']/year
    denominator = options_df['IV'] * np.sqrt(options_df['DTE']/year)
    return numerator / denominator

def d2(options_df, interest_rate = 0.0193, q = 0, year = 252):
    return d1(options_df, interest_rate, q, year) - options_df['IV'] * np.sqrt(options_df['DTE']/year)

def bsm_call(options_df, interest_rate = 0.0193, q = 0, year = 252):

    D1 = d1(options_df, interest_rate, q, year)
    D2 = d2(options_df, interest_rate, q, year)
    call_prices = options_df['Underlying_Price'] * np.exp(-q * options_df['DTE']/year) * norm.cdf(D1) - options_df['Strike'] * np.exp(-interest_rate * options_df['DTE']/year) * norm.cdf(D2)
    
    return pd.DataFrame(call_prices)

def bsm_put(options_df, interest_rate = 0.0193, q = 0, year = 252):

    D1 = d1(options_df, interest_rate, q, year)
    D2 = d2(options_df, interest_rate, q, year)
    put_prices = options_df['Strike'] * np.exp(-interest_rate * options_df['DTE']/year) * norm.cdf(-D2) - options_df['Underlying_Price'] * np.exp(-q * options_df['DTE']/year) * norm.cdf(-D1)
    return pd.DataFrame(put_prices)

def black_scholes_merton(options_df, interest_rate = 0.0193, q = 0, year = 252):
    calls = options_df[options_df['Type'] == 'call']
    if len(calls) > 0:
        calls['Simulated Prices'] = bsm_call(calls, interest_rate, q, year)
    
    
    puts = options_df[options_df['Type'] == 'put']
    if len(puts) > 0:
        puts['Simulated Prices'] = bsm_put(puts, interest_rate, q, year)
    
    if len(puts) > 0 and len(calls) > 0:
        df = pd.concat([calls, puts], axis = 0)
    elif len(puts) > 0:
        df = puts
    else:
        df = calls
        
    return df.fillna(0).reset_index()[df.columns]

def delta(options_df, interest_rate = 0.0193, q = 0, year = 252):

    options_df['D1'] = d1(options_df, interest_rate, q, year)
    calls = options_df[options_df['Type'] == 'call']
    if len(calls) > 0:
        calls['Delta'] = np.exp(-q*calls['DTE']/year)*norm.cdf(calls['D1'])
    
    puts = options_df[options_df['Type'] == 'put']
    if len(puts) > 0:
        puts['Delta'] = -np.exp(-q*puts['DTE']/year)*norm.cdf(-puts['D1'])
    
    if len(puts) > 0 and len(calls) > 0:
        df = pd.concat([calls, puts], axis = 0)
    elif len(puts) > 0:
        df = puts
    else:
        df = calls
        
    del df['D1']
    return df.fillna(0).reset_index()[df.columns]

def theta(options_df, interest_rate = 0.0193, q = 0, year = 252):

    options_df['D1'] = d1(options_df, interest_rate, q, year)
    options_df['D2'] = d2(options_df, interest_rate, q, year)

    options_df['first_term'] = (options_df['Underlying_Price'] * np.exp(-q * options_df['DTE']/year) * 
                                norm.pdf(options_df['D1']) * options_df['IV']) / (2 * np.sqrt(options_df['DTE']/year))
    
    calls = options_df[options_df['Type'] == 'call']
    if len(calls) > 0:
        calls_second_term = -q * calls.Strike * np.exp(-q * calls.DTE/year)*norm.cdf(calls.D1)
        calls_third_term = interest_rate * calls.Strike * np.exp(-interest_rate * calls.DTE/year)*norm.cdf(calls.D2)
        calls['Theta'] = -(calls.first_term + calls_second_term + calls_third_term) / 365.0
    
    puts = options_df[options_df['Type'] == 'put']
    if len(puts) > 0:
        puts_second_term = -q * puts.Strike * np.exp(-q * puts.DTE/year) * norm.cdf(-puts.D1)
        puts_third_term = interest_rate * puts.Strike * np.exp(-interest_rate * puts.DTE/year) * norm.cdf(-puts.D2)
        puts['Theta'] = (-puts.first_term + puts_second_term + puts_third_term) / 365.0
    
    if len(puts) > 0 and len(calls) > 0:
        df = pd.concat([calls, puts], axis = 0)
    elif len(puts) > 0:
        df = puts
    else:
        df = calls
        
    del df['first_term'], df['D1'], df['D2']
    
    return df.fillna(0).reset_index()[df.columns]


def gamma(options_df, interest_rate = 0.0193, q = 0, year = 252):
    D1 = d1(options_df, interest_rate, q, year)
    numerator = np.exp(-q * options_df.DTE/year) * norm.pdf(D1)
    denominator = options_df.Underlying_Price * options_df.IV * np.sqrt(options_df.DTE/year)
    options_df['Gamma'] = numerator / denominator
    return options_df.fillna(0)


def vega(options_df, interest_rate = 0.0193, q = 0, year = 252):
    D1 = d1(options_df, interest_rate, q, year)
    options_df['Vega'] = options_df.Underlying_Price * np.exp(-q * options_df.DTE/year) * norm.pdf(D1) * np.sqrt(options_df.DTE/year) * 0.01
    return options_df.fillna(0)


def rho(options_df, interest_rate = 0.0193, q = 0, year = 252):
    options_df['D2'] = d2(options_df, interest_rate, q, year)
    calls = options_df[options_df['Type'] == 'call']
    if len(calls) > 0:
        calls['Rho'] = calls.DTE/year * calls.Strike * np.exp(-interest_rate * calls.DTE/year) * norm.cdf(calls.D2) * 0.01
    
    puts = options_df[options_df['Type'] == 'put']
    if len(puts) > 0:
        puts['Rho'] = -puts.DTE/year * puts.Strike * np.exp(-interest_rate * puts.DTE/year) * norm.cdf(-puts.D2) * 0.01
    
    if len(puts) > 0 and len(calls) > 0:
        df = pd.concat([calls, puts], axis = 0)
    elif len(puts) > 0:
        df = puts
    else:
        df = calls
        
    del df['D2']
    
    return df.fillna(0).reset_index()[df.columns]

def all_greeks(options_df, interest_rate = 0.0193, q = 0, year = 252):
    df = delta(theta(gamma(vega(rho(options_df, interest_rate, q ,year), 
                                interest_rate, q, year),interest_rate, q, year), interest_rate, q, year), interest_rate, q, year)
    # del df['D2']
    return df

#%%

def all_options(ticker, dte_ub, dte_lb, moneyness = 0.03):
    tape = Options(ticker, 'yahoo')
    data = tape.get_all_data().reset_index()
    
    data['Moneyness'] = np.abs(data['Strike'] - data['Underlying_Price'])/data['Underlying_Price']
    
    data['DTE'] = (data['Expiry'] - dt.datetime.today()).dt.days
    data = data[['Strike', 'Expiry','DTE', 'Type', 'IV', 'Underlying_Price',
                 'Last','Bid','Ask', 'Moneyness']]
    data['Mid'] = (data['Ask'] - data['Bid'])/2 + data['Bid']
    data = data.dropna()
    data = data[(abs(data['Moneyness']) <= moneyness) &
                (data['DTE'] <= dte_ub) &
                (data['DTE'] >= dte_lb)]
    return data.sort_values(['DTE','Type']).reset_index()[data.columns]#data.dropna().reset_index()[data.columns]


def price_sim(options_df, price_change, vol_change, days_change, output = 'All',
              skew = 'flat', day_format = 'trading', interest_rate = 0.0193, q = 0,
              prem_price_use = 'Mid'):
    '''
    output types can be: All, Price, Delta, Gamma, Vega, Theta
    skew types can be: flat, left, right, smile
    '''
    if prem_price_use != 'Mid':
        price_col = 'Last'
    else:
        price_col = 'Mid'
        
    if day_format != 'trading':
        year = 365
    else:
        year = 252
    
    df = options_df.copy()
    df['Underlying_Price'] = df['Underlying_Price']*(1 + price_change)
    df['DTE'] = df['DTE'] - days_change
    df[df['DTE'] < 0] = 0
    
    
    if skew == 'flat':
        df['IV'] = df['IV'] + vol_change
    elif skew == 'right':
        df['IV'] = df['IV'] + vol_change + vol_change*(df['Strike']/df['Underlying_Price'] - 1)
    elif skew == 'left':
        df['IV'] = df['IV'] + vol_change - vol_change*(df['Strike']/df['Underlying_Price'] - 1)
    else:
        df['IV'] = df['IV'] + vol_change + vol_change*abs(df['Strike']/df['Underlying_Price'] - 1)
            
    output_df = black_scholes_merton(delta(gamma(theta(vega(rho(df,interest_rate, q, year), 
                                                           interest_rate, q, year),
                                                      interest_rate, q, year), 
                                                interest_rate, q, year), 
                                          interest_rate, q, year),
                                     interest_rate, q, year)
    return output_df

def position_sim(position_df, holdings, shares,
                 price_change, vol_change, dte_change, output = 'All',
                 skew = 'flat', prem_price_use = 'Mid', day_format = 'trading', 
                 interest_rate = 0.0193, dividend_rate = 0, vol_spacing = 2, 
                 spacing = 20):
    if prem_price_use != 'Mid':
        price_col = 'Last'
    else:
        price_col = 'Mid'
        
        
    position = position_df.reset_index()[['Strike','Expiry','DTE','Type','IV','Underlying_Price',price_col]]
    position['Pos'] = holdings
    initial_cost = sum(position[price_col]*position['Pos'])*100 + shares*position['Underlying_Price'].values[0]
    
    price_changes = np.linspace(price_change[0], price_change[-1], spacing)
    dte_changes = np.linspace(dte_change[0], dte_change[-1], dte_change[-1] - dte_change[0] + 1)

    if vol_spacing <= 2:
        vol_changes = vol_change
    else:
        vol_changes = np.linspace(vol_change[0], vol_change[-1], vol_spacing)

    adj_dfs = []

    price_ax, dte_ax = np.meshgrid(price_changes,dte_changes)

    vol_adj_df = pd.DataFrame(np.array(np.meshgrid(price_changes,dte_changes)).reshape(2,-1).T)
    vol_adj_df.columns = ['ret_change', 'dte_change']

    for vol_change in vol_changes:
        # mesh_shape = np.meshgrid(price_changes,dte_changes)

        indi_sims = []
        for idx, row in position.iterrows():
            curr_sim = pd.DataFrame(index = range(len(vol_adj_df)))
            curr_sim['Strike'] = row.Strike
            curr_sim['DTE'] = row.DTE - vol_adj_df['dte_change']
            curr_sim[curr_sim['DTE'] < 0] = 0

            curr_sim['Type'] = row.Type
            curr_sim['IV'] = row.IV
            curr_sim['Underlying_Price'] = (1 + vol_adj_df[['ret_change']])*row.Underlying_Price
            curr_sim = price_sim(curr_sim, 0, vol_change, 0, output,
                                 skew, day_format, interest_rate, dividend_rate,
                                 prem_price_use)
            indi_sims.append(curr_sim)

        if len(holdings) < 2:
            try:
                adj_df = indi_sims[0]
            except:
                break
            adj_df['Delta'] = adj_df['Delta'] + shares/100
            adj_df['PnL'] = position.head(1)['Pos'][0]*(adj_df['Simulated Prices'] - 
                                                        position.head(1)[price_col][0])*100 + shares*(adj_df['Underlying_Price'] - 
                                                                                                      position.head(1)['Underlying_Price'][0])
            adj_df['Percent Return'] = adj_df['PnL']/initial_cost
        else:
            adj_df = curr_sim[['Underlying_Price']]
            adj_df['Delta'] = 0
            adj_df['Gamma'] = 0
            adj_df['Vega'] = 0
            adj_df['Theta'] = 0
            adj_df['Rho'] = 0
            adj_df['PnL'] = 0
            for i, val in enumerate(holdings):
                adj_df['Delta'] = adj_df['Delta'] + val*indi_sims[i]['Delta']
                adj_df['Gamma'] = adj_df['Gamma'] + val*indi_sims[i]['Gamma']
                adj_df['Vega'] = adj_df['Vega'] + val*indi_sims[i]['Vega']
                adj_df['Theta'] = adj_df['Theta'] + val*indi_sims[i]['Theta']
                adj_df['Rho'] = adj_df['Rho'] + val*indi_sims[i]['Rho']
                adj_df['PnL'] = adj_df['PnL'] + val*indi_sims[i]['Simulated Prices']

            adj_df['PnL'] = (adj_df['PnL'] - sum(position[price_col]*position['Pos']))*100 + shares*(adj_df['Underlying_Price'] -
                                                                                                     position.head(1)['Underlying_Price'][0])
            adj_df['Percent Return'] = adj_df['PnL']/initial_cost

        adj_df['Date'] = dt.datetime.today().date() + pd.to_timedelta(vol_adj_df['dte_change'] + 1, 'd')
        adj_dfs.append(adj_df)
    
    return (adj_dfs, price_ax, dte_ax)

#%% Older non-optimized functions
def yahoo_options_query(ticker, dte_ub, dte_lb):
    with urlreq.urlopen('https://query1.finance.yahoo.com/v7/finance/options/{}'.format(ticker)) as url:
        data = json.loads(url.read().decode())
        midprice = (data['optionChain']['result'][0]['quote']['ask'] + 
                    data['optionChain']['result'][0]['quote']['bid'])/2
        expiry_dates_unix = data['optionChain']['result'][0]['expirationDates']
        expiry_dates = [dt.datetime.fromtimestamp(int(utime)) for utime in expiry_dates_unix]
    
    query_dates = []
    
    for i, expiry in enumerate(expiry_dates):
        if (expiry - dt.datetime.today()).days <= dte_ub and (expiry - dt.datetime.today()).days >= dte_lb:
            query_dates.append(i)
            print(expiry)
    query_dte_ub = query_dates[-1]
    query_dte_lb = query_dates[0]
            
    with urlreq.urlopen('https://query1.finance.yahoo.com/v7/finance/options/{}?date={}'.format(ticker,
                                                                                            expiry_dates_unix[query_dte_ub])) as url:
        data = json.loads(url.read().decode())['optionChain']['result'][0]['options'][0]
        calls_ub = json_normalize(data['calls'])
        puts_ub = json_normalize(data['puts'])
        
    time.sleep(1)
        
    with urlreq.urlopen('https://query1.finance.yahoo.com/v7/finance/options/{}?date={}'.format(ticker,
                                                                                            expiry_dates_unix[query_dte_lb])) as url:
        data = json.loads(url.read().decode())['optionChain']['result'][0]['options'][0]
        calls_lb = json_normalize(data['calls'])
        puts_lb = json_normalize(data['puts'])
        
    def standardize_chain(chain, option_type, query_date):
        chain = chain.drop(['contractSize','currency','lastTradeDate','openInterest','percentChange',
                            'volume','expiration','change','contractSymbol'], axis = 1)
        chain['type'] = option_type
        #cols = ['ask', 'bid', 'impliedVolatility', 'lastPrice', 'strike']
        chain['expiry'] = expiry_dates[query_date].date()
        chain['DTE'] = (expiry_dates[query_date].date() - dt.datetime.today().date()).days
        chain['lastSpotPrice'] = midprice
        chain['midPrice'] = (chain['ask'] + chain['bid'])/2
        return chain
    
    calls_ub = standardize_chain(calls_ub, 'c',query_dte_ub)
    puts_ub = standardize_chain(puts_ub, 'p',query_dte_ub)
    calls_lb = standardize_chain(calls_lb, 'c',query_dte_ub)
    puts_lb = standardize_chain(puts_lb, 'p',query_dte_ub)
    
    display = ['strike','type','expiry','DTE','inTheMoney','ask','bid','midPrice',
               'lastPrice','impliedVolatility', 'lastSpotPrice']
    
    return pd.concat([calls_lb, puts_lb, calls_ub, puts_ub], axis = 0).reset_index()[display]


def greek_calc_old(df, prem_price_use = 'Mid', day_format = 'trading', interest_rate = 0.0193, dividend_rate = 0):
    if prem_price_use != 'Mid':
        price_col = 'Last'
    else:
        price_col = 'Mid'
        
    if day_format != 'trading':
        year = 365
    else:
        year = 252
    
    premiums = df[price_col].values
    strikes = df['Strike'].values
    time_to_expirations = df['DTE'].values
    ivs = df['IV'].values
    underlying = df['Underlying_Price'].values[0]
    types = df['Type'].values

    deltas = []
    gammas = []
    thetas = []
    rhos = []
    vegas = []
    for premium, strike, time_to_expiration, flag, iv in zip(premiums, strikes, time_to_expirations, types, ivs):

        # Constants
        # P = premium
        S = underlying
        K = strike
        t = time_to_expiration/float(year)
        r = interest_rate
        q = dividend_rate
        try:
            rho = py_vollib.black_scholes_merton.greeks.analytical.rho(flag[0], S, K, t, r, iv, q)
        except:
            rho = 0.0
        rhos.append(rho)

        try:
            delta = py_vollib.black_scholes_merton.greeks.analytical.delta(flag[0], S, K, t, r, iv, q)
        except:
            delta = 0.0
        deltas.append(delta)

        try:
            gamma = py_vollib.black_scholes_merton.greeks.analytical.gamma(flag[0], S, K, t, r, iv, q)
        except:
            gamma = 0.0
        gammas.append(gamma)

        try:
            theta = py_vollib.black_scholes_merton.greeks.analytical.theta(flag[0], S, K, t, r, iv, q)
        except:
            theta = 0.0
        thetas.append(theta)

        try:
            vega = py_vollib.black_scholes_merton.greeks.analytical.vega(flag[0], S, K, t, r, iv, q)
        except:
            vega = 0.0
        vegas.append(vega)

    df['Delta'] = deltas
    df['Gamma'] = gammas
    df['Theta'] = thetas
    df['Vega'] = vegas
    df['Rho'] = rhos
    # df = df.dropna()
    
    return df

def price_sim_old(options_df, price_change, vol_change, days_change, output = 'All',
              skew = 'flat', day_format = 'trading', interest_rate = 0.0193, dividend_rate = 0,
              prem_price_use = 'Mid'):
    '''
    output types can be: All, Price, Delta, Gamma, Vega, Theta
    skew types can be: flat, left, right, smile
    '''
    if prem_price_use != 'Mid':
        price_col = 'Last'
    else:
        price_col = 'Mid'
        
    if day_format != 'trading':
        year = 365
    else:
        year = 252
        
    
    #premiums = options_df[price_col].values
    strikes = options_df['Strike'].values
    time_to_expirations = options_df['DTE'].values
    ivs = options_df['IV'].values
    underlying = options_df['Underlying_Price'].values[0]
    types = options_df['Type'].values

    # Tweaking changes
    prices = []
    deltas = []
    gammas = []
    thetas = []
    vegas = []
    rhos = []
    for sigma, strike, time_to_expiration, flag in zip(ivs, strikes, time_to_expirations, types):

        # Constants
        S = underlying*(1 + price_change)
        t = max(time_to_expiration - days_change, 0)/float(year)
        K = strike
        r = interest_rate
        q = dividend_rate
        
        if skew == 'flat':
            sigma = sigma + vol_change
        elif skew == 'right':
            sigma = sigma + vol_change + vol_change*(K/S - 1)
        elif skew == 'left':
            sigma = sigma + vol_change - vol_change*(K/S - 1)
        else:
            sigma = sigma + vol_change + vol_change*abs(K/S - 1)
        
        if (output == 'All') or (output == 'Price'):
            if days_change == time_to_expiration:
                if flag == 'call':
                    price = max(S - K, 0.0)
                else:
                    price = max(K - S, 0.0)
                prices.append(price)
            else:
                try:
                    price = py_vollib.black_scholes_merton.black_scholes_merton(flag[0], S, K, t, r, sigma, q)
                except:
                    price = 0.0
                prices.append(price)
                    
        if (output == 'All') or (output == 'Delta'):
            try:
                delta = py_vollib.black_scholes_merton.greeks.analytical.delta(flag[0], S, K, t, r, sigma, q)
            except:
                delta = 0.0
            deltas.append(delta)
        
        if (output == 'All') or (output == 'Gamma'):
            try:
                gamma = py_vollib.black_scholes_merton.greeks.analytical.gamma(flag[0], S, K, t, r, sigma, q)
            except:
                gamma = 0.0
            gammas.append(gamma)
            
        if (output == 'All') or (output == 'Theta'):
            try:
                theta = py_vollib.black_scholes_merton.greeks.analytical.theta(flag[0], S, K, t, r, sigma, q)
            except:
                theta = 0.0
            thetas.append(theta)
        
        if (output == 'All') or (output == 'Vega'):
            try:
                vega = py_vollib.black_scholes_merton.greeks.analytical.vega(flag[0], S, K, t, r, sigma, q)
            except:
                vega = 0.0
            vegas.append(vega)
        if (output == 'All') or (output == 'Rho'):
            try:
                rho = py_vollib.black_scholes_merton.greeks.analytical.rho(flag[0], S, K, t, r, sigma, q)
            except:
                rho = 0.0
            rhos.append(rho)
            
    df = options_df[['Strike','DTE','Type',price_col,'Underlying_Price']]
    df['Simulated Price'] = prices
    df['Price Change'] = df['Simulated Price']/(df[price_col]) - 1
    if (output == 'All') or (output == 'Delta'):
        df['Delta'] = deltas
    if (output == 'All') or (output == 'Gamma'):
        df['Gamma'] = gammas
    if (output == 'All') or (output == 'Theta'):
        df['Theta'] = thetas
    if (output == 'All') or (output == 'Vega'):
        df['Vega'] = vegas
    if (output == 'All') or (output == 'Rho'):
        df['Rho'] = rhos
    df = df.dropna()
    return df

def position_sim_old(position_df, holdings, shares,
                 price_change, vol_change, dte_change, output = 'All',
                 skew = 'flat', prem_price_use = 'Mid', day_format = 'trading', 
                 interest_rate = 0.0193, dividend_rate = 0):
    
    if prem_price_use != 'Mid':
        price_col = 'Last'
    else:
        price_col = 'Mid'
                
    position = position_df
    position['Pos'] = holdings
    position_dict = {}
    position_dict['Total Cost'] = sum(position[price_col]*position['Pos'])*100 + shares*position['Underlying_Price'].values[0]
    
    simulation = price_sim_old(position, price_change, vol_change, dte_change, output,
                           skew, day_format, interest_rate, dividend_rate,
                           prem_price_use)
    
    if (output == 'All') or (output == 'PnL') or (output == 'Percent Return'):
        position_dict['Simulated Price'] = sum(simulation['Simulated Price']*position['Pos'])*100 + shares*position['Underlying_Price'].values[0]*(1 + price_change)
        position_dict['PnL'] = position_dict['Simulated Price'] - position_dict['Total Cost']
        if position_dict['Total Cost'] > 0:
            position_dict['Percent Return'] = position_dict['PnL']/position_dict['Total Cost']
        else:
            position_dict['Percent Return'] = -position_dict['PnL']/position_dict['Total Cost']
            
    if (output == 'All') or (output == 'Delta'):
        position_dict['Simulated Delta'] = sum(simulation['Delta']*position['Pos']) + shares/100
        
    if (output == 'All') or (output == 'Gamma'):
        position_dict['Simulated Gamma'] = sum(simulation['Gamma']*position['Pos'])
        
    if (output == 'All') or (output == 'Theta'):
        position_dict['Simulated Theta'] = sum(simulation['Theta']*position['Pos'])
        
    if (output == 'All') or (output == 'Vega'):
        position_dict['Simulated Vega'] = sum(simulation['Vega']*position['Pos'])
    
    if (output == 'All') or (output == 'Rho'):
        position_dict['Simulated Rho'] = sum(simulation['Rho']*position['Pos'])
    
    outframe = pd.DataFrame(position_dict, index = [vol_change])
    return outframe
#%%
	
def yahoo_fundamentals(ticker_lst):
    
    for i, ticker in enumerate(ticker_lst):
        with urlreq.urlopen('https://query1.finance.yahoo.com/v7/finance/options/{}'.format(ticker)) as url:
            data = json_normalize(json.loads(url.read().decode())['optionChain']['result'][0]['quote']).T
            data.columns = [ticker]
        if i == 0:
            out_df = data
        else:
            out_df = out_df.join(data)
            
    return out_df

#%%
def spx_spreads(dte_ub, dte_lb,price, moneyness, max_strike_distance, max_exposure, commission = 3):
    spx_chain = all_options('^SPX', dte_ub, dte_lb, moneyness)
    spx_chain = spx_chain[(spx_chain.Type == 'put') &
                          (spx_chain.Strike <= spx_chain.Underlying_Price)]
    spx_chain = all_greeks(spx_chain)

    unique_dtes = spx_chain.DTE.drop_duplicates().tolist()

    

    df_cols = ['Expiry','LongStrike','ShortStrike', 'LongPrice', 'ShortPrice',
               'Premium', 'MaxLoss', 'Vega', 'Gamma',
               'Delta','Theta']
    spread_df = pd.DataFrame(columns = df_cols)

    for dte in unique_dtes:
        curr_dtes = spx_chain[spx_chain.DTE == dte].sort_values('Strike')
        for i_long, r_long in curr_dtes.iterrows():
            curr_df_slice = pd.DataFrame(columns = df_cols)
            try:
                curr_shorts = curr_dtes[curr_dtes.Strike > r_long.Strike]
                curr_shorts = curr_shorts[(curr_shorts.Strike - r_long.Strike) <= max_strike_distance]

                curr_df_slice['ShortStrike'] = curr_shorts['Strike']
                curr_df_slice['LongStrike'] = r_long.Strike
                curr_df_slice['Expiry'] = r_long.Expiry
                curr_df_slice['Vega'] = r_long.Vega - curr_shorts['Vega']
                curr_df_slice['Gamma'] = r_long.Gamma - curr_shorts['Gamma']
                curr_df_slice['Delta'] = r_long.Delta - curr_shorts['Delta']
                curr_df_slice['Theta'] = r_long.Theta - curr_shorts['Theta']

                if price == 'Market':
                    curr_df_slice['LongPrice'] = r_long.Ask
                    curr_df_slice['ShortPrice'] = curr_shorts['Bid']
                    curr_df_slice['Premium'] = 100*(curr_shorts['Bid'] - r_long.Ask)
                else:
                    curr_df_slice['LongPrice'] = r_long.Mid
                    curr_df_slice['ShortPrice'] = curr_shorts['Mid']
                    curr_df_slice['Premium'] = 100*(curr_shorts['Mid'] - r_long.Mid)

                curr_df_slice['MaxLoss'] = 100*(curr_shorts.Strike - r_long.Strike) - curr_df_slice['Premium']

                spread_df = pd.concat([spread_df, curr_df_slice], axis = 0).reset_index(drop = True)
            except:
                continue

    spread_df['RiskReward'] = spread_df['Premium']/spread_df['MaxLoss']
    spread_df['Contracts'] = np.floor(max_exposure/spread_df['MaxLoss'])
    spread_df['Premium'] = spread_df['Contracts']*spread_df['Premium'] - commission*spread_df['Contracts']
    spread_df['MaxLoss'] = spread_df['Contracts']*spread_df['MaxLoss']
    spread_df['Vega'] = spread_df['Contracts']*spread_df['Vega']
    spread_df['Gamma'] = spread_df['Contracts']*spread_df['Gamma']
    spread_df['Delta'] = spread_df['Contracts']*spread_df['Delta']
    spread_df['Theta'] = spread_df['Contracts']*spread_df['Theta']
    spread_df['RiskReward'] = spread_df['Premium']/spread_df['MaxLoss']

    spread_df = spread_df.sort_values('RiskReward', ascending = False).reset_index(drop = True)
    spread_df['Score'] = spread_df.index
    spread_df = spread_df.sort_values('Theta', ascending = False).reset_index(drop = True)
    spread_df['Score'] = spread_df['Score'] + spread_df.index
    spread_df = spread_df.sort_values('Vega', ascending = False).reset_index(drop = True)
    spread_df['Score'] = spread_df['Score'] + spread_df.index
    spread_df = spread_df.sort_values('Gamma', ascending = False).reset_index(drop = True)
    spread_df['Score'] = spread_df['Score'] + spread_df.index
    spread_df = spread_df.sort_values('Delta', ascending = False).reset_index(drop = True)
    spread_df['Score'] = spread_df['Score'] + spread_df.index
    spread_df = spread_df.sort_values('Score').reset_index(drop = True)
    return spread_df