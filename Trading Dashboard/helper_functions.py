# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 21:10:04 2018

@author: fchen
"""
import datetime as dt
import numpy as np
import pandas as pd
import yahoo_fetch

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


def stats(intraday_df, sma_long = 20, sma_short = 5, rolling_window = 20):
    
    intraday_vol = intraday_df
    intraday_vol.columns = ['Last']
    intraday_vol['Log Return'] = np.log(intraday_vol['Last']/intraday_vol['Last'].shift(1))
    intraday_vol['Return Std'] = intraday_vol['Log Return'].rolling(window=rolling_window,center=False).std()
    intraday_vol['Dollar Std'] = intraday_vol['Return Std']*intraday_vol['Last']
    intraday_vol['Dollar Std Move'] = (intraday_vol['Last'] - intraday_vol['Last'].shift(1))/intraday_vol['Dollar Std'].shift(1)
    intraday_df['Dollar Std Move'] = intraday_vol['Dollar Std Move']
    
        
    intraday_df['SMA {}'.format(sma_short)] = intraday_df.Last.rolling(sma_short).mean()
    intraday_df['SMA {}'.format(sma_long)] = intraday_df.Last.rolling(sma_long).mean()
    
    return intraday_df.dropna()

def yahoo_batch_quotes(tickerlst, fetch_daily = True, fetch_intraday = True, daysback = 365):
    
    for i, ticker in enumerate(tickerlst):
        curr_stock = yahoo_fetch.yahoo(ticker, fetch_daily, fetch_intraday, daysback)
        
        if fetch_daily:
            curr_stock.daily.index = pd.to_datetime(curr_stock.daily.index)
            
        if fetch_intraday:
            curr_stock.minute.index = pd.to_datetime(curr_stock.minute.index)
    
        if i == 0:
            if fetch_daily:
                daily_data = curr_stock.daily
            if fetch_intraday:
                minute_data = curr_stock.minute
        else:
            if fetch_daily:
                daily_data = daily_data.join(curr_stock.daily)
            if fetch_intraday:
                minute_data = minute_data.join(curr_stock.minute)
    
    if fetch_daily:
        daily_data.index = daily_data.index.date
    
    if fetch_daily and fetch_intraday:
        return daily_data, minute_data
    
    elif fetch_daily:
        return daily_data
    elif fetch_intraday:
        return minute_data
    
def check_mkt_corr(data, rolling_window, plot_window):
    spdr_lst = ['GSPC','XLU','XLRE','XLY','XLV',
                'XLB', 'XLI', 'XLF', 'XLK', 'XLC',
                'XLP', 'XLE']
    df = data[spdr_lst]
    df = df.pct_change()
    df_corr = df.rolling(rolling_window).corr(df['GSPC'])
    del df_corr['GSPC'], df_corr['XLC']
    df_corr = df_corr.dropna().tail(plot_window)
    df_corr['Avg_Corr'] = df_corr.mean(axis = 1)
    df_corr['SPX_cum'] = (df[['GSPC']].tail(plot_window) + 1).cumprod() - 1

    return df_corr, df

def spx_trend(minute_data, lookback_minutes = 60):
    import statsmodels.api as sm
    
    try:
        spx_trend = minute_data[['GSPC']].dropna().tail(lookback_minutes)
    except:
        trend_table = pd.DataFrame({'SPX Intraday Summaries': [0,0]}, index = ['Trend Magnitude','Trend Strength'])
        return trend_table
    else:
        spx_trend = spx_trend/spx_trend['GSPC'].head(1).values[0] - 1
        spx_trend = spx_trend*100
        spx_trend = spx_trend.reset_index(drop = True)
        spx_trend = sm.OLS(spx_trend.index,spx_trend['GSPC']).fit().summary()
        trend_strength = pd.read_html(spx_trend.tables[0].as_html())[0].loc[0,2:].reset_index(drop = True)
        trend_magnitude = pd.read_html(spx_trend.tables[1].as_html())[0].loc[:,1].reset_index(drop = True)
        trend_table = pd.concat([trend_magnitude,trend_strength], axis = 1).T.set_index(0)
        trend_table.columns = ['SPX Intraday Summaries']
        trend_table.index = ['Trend Magnitude','Trend Strength']
        
        return trend_table
