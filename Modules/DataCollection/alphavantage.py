# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 09:41:13 2018

@author: Fang
"""
import pandas as pd
import matplotlib.pyplot as plt

def curr_batch_quotes(tickerlst):
    lst_string = str(tickerlst).replace('[','').replace(']','').replace(' ','').replace("'",'')
    av_link = 'https://www.alphavantage.co/query?function=BATCH_STOCK_QUOTES&symbols={0}&apikey=APIKEY=5HZEUI5AFJB06BUK&datatype=csv'.format(lst_string)
    df = pd.read_csv(av_link, index_col = 0)
    return df

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
    return df_corr, df