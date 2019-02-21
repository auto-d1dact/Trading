# -*- coding: utf-8 -*-
"""
Created on Sun Feb 17 12:17:16 2019

@author: Fang
"""

import warnings
warnings.filterwarnings('ignore')
import os
import math
from ib_insync import *
import numpy as np
import pandas as pd
import datetime as dt
from scipy.stats import norm as norm
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
import requests as req
from bs4 import BeautifulSoup as bs
#util.startLoop()

os.chdir('D:\Options Data\IB Intraday')

def create_dfrow(curr_option):
    df_row = pd.DataFrame({'Symbol': curr_option.contract.localSymbol, 
                           'Type':curr_option.contract.right, 
                           'Bid': curr_option.bid, 
                           'Ask': curr_option.ask, 
                           'Volume': curr_option.volume, 
                           'Strike': curr_option.contract.strike,
                           'Expiry': dt.datetime.strptime(curr_option.contract.lastTradeDateOrContractMonth, '%Y%m%d'),
                           'bidIV': curr_option.bidGreeks.impliedVol, 
                           'bidDelta': curr_option.bidGreeks.delta,
                           'bidGamma': curr_option.bidGreeks.gamma, 
                           'bidVega': curr_option.bidGreeks.vega,
                           'bidTheta': curr_option.bidGreeks.theta, 
                           'askIV': curr_option.askGreeks.impliedVol, 
                           'askDelta': curr_option.askGreeks.delta,
                           'askGamma': curr_option.askGreeks.gamma, 
                           'askVega': curr_option.askGreeks.vega,
                           'askTheta': curr_option.askGreeks.theta,
                           'modelIV': curr_option.modelGreeks.impliedVol, 
                           'modelDelta': curr_option.modelGreeks.delta,
                           'modelGamma': curr_option.modelGreeks.gamma, 
                           'modelVega': curr_option.modelGreeks.vega,
                           'modelTheta': curr_option.modelGreeks.theta}, index = [0])
    return df_row

def time_remaining(weekly_options, curr_spx):

    typ = weekly_options.reset_index().loc[0,'Type']

    try:
        interest_rate = bs(req.get('https://www.marketwatch.com/investing/bond/tmubmusd01m?countrycode=bx','lxml').text, 'lxml')
        interest_rate = float(interest_rate.select_one('h3[class*="intraday__price"]').select_one('bg-quote').text)/100
    except:
        interest_rate = 0.02422

    weekly_options['a'] = interest_rate - (weekly_options['modelIV']**2)/2

    if typ == 'P':
        weekly_options['b'] = weekly_options['modelIV']*norm.ppf(-weekly_options['modelDelta'])
    if typ == 'C':
        weekly_options['b'] = -weekly_options['modelIV']*norm.ppf(weekly_options['modelDelta'])
    weekly_options['c'] = np.log(curr_spx/weekly_options['Strike'])

    weekly_options_otm = weekly_options[abs(weekly_options.modelDelta) <= 0.5]
    weekly_options_itm = weekly_options[abs(weekly_options.modelDelta) > 0.5]

    if typ == 'P':
        weekly_options_otm['time_remaining'] = ((-weekly_options_otm.b - 
                                                 np.sqrt(weekly_options_otm.b**2 - 
                                                         4*weekly_options_otm.a*weekly_options_otm.c))/(2*weekly_options_otm.a))**2
        weekly_options_itm['time_remaining'] = ((-weekly_options_itm.b + 
                                                 np.sqrt(weekly_options_itm.b**2 - 
                                                         4*weekly_options_itm.a*weekly_options_itm.c))/(2*weekly_options_itm.a))**2
    if typ == 'C':
        weekly_options_otm['time_remaining'] = ((-weekly_options_otm.b + 
                                                 np.sqrt(weekly_options_otm.b**2 - 
                                                         4*weekly_options_otm.a*weekly_options_otm.c))/(2*weekly_options_otm.a))**2
        weekly_options_itm['time_remaining'] = ((-weekly_options_itm.b - 
                                                 np.sqrt(weekly_options_itm.b**2 - 
                                                         4*weekly_options_itm.a*weekly_options_itm.c))/(2*weekly_options_itm.a))**2
    weekly_times = pd.concat([weekly_options_otm, 
                              weekly_options_itm], axis = 0)[['Expiry','time_remaining']].dropna().groupby('Expiry').mean()

    del weekly_options['a'], weekly_options['b'], weekly_options['c'], weekly_options_itm, weekly_options_otm

    return weekly_options.merge(weekly_times.reset_index(), on = 'Expiry')

def get_spreads(weekly_options, curr_spx, curr_vix, curr_skew, expiry_index = 0, commissions = 0.0266):

    expirations_dates = weekly_options.Expiry.drop_duplicates().tolist()
    
    weekly_options = weekly_options[weekly_options.Expiry == expirations_dates[expiry_index]]

    typ = weekly_options.reset_index().loc[0,'Type']
    
    if typ == 'P':
        weekly_options = weekly_options.sort_values('Strike', ascending = False).reset_index(drop = True)
    if typ == 'C':
        weekly_options = weekly_options.sort_values('Strike', ascending = True).reset_index(drop = True)
        
    time_remaining = weekly_options.reset_index(drop = True).loc[0,'time_remaining']

    shorts = weekly_options[['Bid', 'Strike', 'bidDelta', 'bidGamma', 'bidTheta', 'bidVega']]
    shorts.columns = ['Price','Strike','Delta','Gamma','Theta','Vega']
    longs = weekly_options[['Ask','Strike', 'askDelta', 'askGamma','askTheta', 'askVega']]
    longs.columns = ['Price','Strike','Delta','Gamma','Theta','Vega']

    spreads = shorts - longs.shift(-1)
    spreads['Short_Strike'] = shorts.Strike
    spreads['Short_Delta'] = shorts.Delta
    spreads['Long_Strike'] = longs.Strike.shift(-1)
    spreads = spreads[['Short_Strike', 'Long_Strike', 'Short_Delta', 'Price',
                       'Strike', 'Delta', 'Gamma', 'Theta', 'Vega']]

    spreads['Credit'] = spreads.Price - commissions

    if typ == 'P':
        spreads['MaxLoss'] = (-spreads.Strike + spreads.Credit)*100
        spreads['BreakEven'] = spreads['Short_Strike'] - spreads.Credit
    if typ == 'C':
        spreads['MaxLoss'] = (spreads.Strike + spreads.Credit)*100
        spreads['BreakEven'] = spreads['Short_Strike'] + spreads.Credit

    del spreads['Price'], spreads['Strike']

    spreads = spreads[spreads.Credit > 0].dropna().reset_index(drop = True)

    stepsize = 0.01
    period_iv = np.sqrt(time_remaining)*curr_vix/100
    
    skew_implied_2std = 0.027*(curr_skew - 100)/10
    skew_implied_3std = 0.006*(curr_skew - 100)/10

    period_downside_iv_2std = (-2*period_iv)/norm.ppf(skew_implied_2std,0,1)
    period_downside_iv_3std = (-3*period_iv)/norm.ppf(skew_implied_3std,0,1)

    spreads['EV'] = np.nan
    spreads['Win Prob'] = np.nan

    ev_lsts = []

    for idx, row in spreads.iterrows():

        if typ == 'P':
            ev_df = pd.DataFrame({'SPX': np.arange(row.Long_Strike, row.Short_Strike + stepsize, stepsize)})
            ev_df = ev_df[(ev_df['SPX'] < row.Short_Strike + stepsize)]
            ev_df['PnL'] = ev_df['SPX'] - row.Short_Strike + row.Credit
        if typ == 'C':
            ev_df = pd.DataFrame({'SPX': np.arange(row.Short_Strike, row.Long_Strike + stepsize, stepsize)})
            ev_df = ev_df[(ev_df['SPX'] < row.Long_Strike + stepsize)]
            ev_df['PnL'] = row.Short_Strike - ev_df['SPX'] + row.Credit

        ev_df['Prob'] = norm.cdf(ev_df['SPX']/curr_spx - 1,0, period_iv)
        ev_df[ev_df.SPX < curr_spx*(1 - period_iv)]['Prob'] = norm.cdf(ev_df[ev_df.SPX < curr_spx*(1 - period_iv)]['SPX']/curr_spx - 1,
                                                                       0, period_downside_iv_2std)
        ev_df[ev_df.SPX < curr_spx*(1 - 2*period_iv)]['Prob'] = norm.cdf(ev_df[ev_df.SPX < curr_spx*(1 - 2*period_iv)]['SPX']/curr_spx - 1,
                                                                         0, period_downside_iv_3std)
        
        lb = ev_df.loc[0,'Prob']
        ub = ev_df.loc[len(ev_df) - 1,'Prob']
        ev_df['Prob'] = ev_df['Prob'].diff()
        ev_df.loc[0,'Prob'] = lb
        ev_df.loc[len(ev_df) - 1, 'Prob'] = 1 - ub
        ev_df['EV'] = ev_df.Prob*ev_df.PnL

        ev_lsts.append(ev_df)
        total_ev = sum(ev_df['EV'])
        win_prob = sum(ev_df[ev_df['PnL'] >= 0]['Prob'])

        spreads.loc[idx, 'EV'] = total_ev
        spreads.loc[idx, 'Win Prob'] = win_prob
    
    spreads['Expiry'] = expirations_dates[expiry_index]
    spreads.Expiry = pd.to_datetime(spreads['Expiry'])
    return spreads, ev_lsts


#%% Stats
def get_block_stats(curr_block, block_label):

    curr_spx_trend = curr_block.close_spx - curr_block.reset_index(drop = True).loc[0,'close_spx']
    curr_vix_trend = curr_block.close_vix - curr_block.reset_index(drop = True).loc[0,'close_vix']
    spx_X = curr_spx_trend.index ## X usually means our input variables (or independent variables)
    spx_Y = curr_spx_trend ## Y usually means our output/dependent variable

    vix_X = curr_vix_trend.index ## X usually means our input variables (or independent variables)
    vix_Y = curr_vix_trend ## Y usually means our output/dependent variable

    # # Note the difference in argument order
    spx_ols = sm.OLS(spx_Y, spx_X).fit().summary()
    spx_ols_r2 = float(pd.read_html(spx_ols.tables[0].as_html())[0].loc[1,3])
    spx_ols_coeff = float(pd.read_html(spx_ols.tables[1].as_html())[0].loc[1,1])

    vix_ols = sm.OLS(vix_Y, vix_X).fit().summary()
    vix_ols_r2 = float(pd.read_html(vix_ols.tables[0].as_html())[0].loc[1,3])
    vix_ols_coeff = float(pd.read_html(vix_ols.tables[1].as_html())[0].loc[1,1])

    spx_bar_std = np.std(curr_block.close_spx/curr_block.open_spx)
    vix_bar_std = np.std(curr_block.close_vix/curr_block.open_vix)

    spx_block_std = np.std((curr_block.close_spx.shift(1)/curr_block.close_spx).dropna())
    vix_block_std = np.std((curr_block.close_vix.shift(1)/curr_block.close_vix).dropna())

    spx_adf_sig = ts.adfuller(np.log(curr_block.close_spx))[1]
    vix_adf_sig = ts.adfuller(np.log(curr_block.close_vix))[1]

    spx_block_return = curr_block.loc[len(curr_block) - 1, 'close_spx']/curr_block.loc[0, 'close_spx'] - 1
    vix_block_return = curr_block.loc[len(curr_block) - 1, 'close_vix']/curr_block.loc[0, 'close_vix'] - 1
    
    spx_vix_corr = np.corrcoef(curr_block.close_spx,curr_block.close_vix)[0,1]
    
    block_stats = pd.DataFrame({'Block_Label': block_label,
                                'SPX_OLS_Coeff': spx_ols_coeff, 
                                'SPX_OLS_R2': spx_ols_r2,
                                'SPX_bar_std': spx_bar_std,
                                'SPX_block_std': spx_block_std,
                                'SPX_adf_sig': spx_adf_sig,
                                'SPX_block_return': spx_block_return,
                                'VIX_OLS_Coeff': vix_ols_coeff, 
                                'VIX_OLS_R2': vix_ols_r2,
                                'VIX_bar_std': vix_bar_std,
                                'VIX_block_std': vix_block_std,
                                'VIX_adf_sig': vix_adf_sig,
                                'VIX_block_return': vix_block_return,
                                'SPX_VIX_corr': spx_vix_corr,
                                'VIX_open': curr_block.loc[0,'close_vix']}, index = [0])
    return block_stats

def day_block_stats(curr_data):
    block_label = 1
    day_timeblock_stats = []
    for time_block in range(0,len(curr_data),30):
        if time_block < len(curr_data):
            curr_block = curr_data.loc[time_block:time_block + 30, :].reset_index(drop = True)
            day_timeblock_stats.append(get_block_stats(curr_block, "Time{}".format(block_label)))
        block_label += 1

    day_timeblock_stats = pd.concat(day_timeblock_stats, axis = 0).reset_index(drop = True)

    return day_timeblock_stats
#%%

class tws_data:
    
    def __init__(self, number_of_expiries, client_id):
        
        ib = IB()
        ib.connect('127.0.0.1', 7496, clientId=client_id)
        
        spx = Index('SPX', 'CBOE')
        vix = Index('VIX', 'CBOE')
        skew = Index('SKEW', 'CBOE')
        ib.qualifyContracts(skew)
        ib.qualifyContracts(spx)
        
        ib.reqHeadTimeStamp(spx, whatToShow='TRADES', useRTH=True)
        ib.reqHeadTimeStamp(vix, whatToShow='TRADES', useRTH=True)
        
        end_date = '' #'20100506 13:00:00 PST'
        duration = '1 D'
        
        spx_bars = ib.reqHistoricalData(
                spx,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1)
        
        vix_bars = ib.reqHistoricalData(
                vix,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1)
        
        self.spx_df = util.df(spx_bars).set_index('date')
        self.spx_df.index = pd.to_datetime(self.spx_df.index)
        
        self.vix_df = util.df(vix_bars).set_index('date')
        self.vix_df.index = pd.to_datetime(self.vix_df.index)
        
        self.curr_spx = self.spx_df.reset_index().loc[len(self.spx_df) - 1, 'close']
        
        self.curr_vix = self.vix_df.reset_index().loc[len(self.vix_df) - 1, 'close']
        
        self.curr_skew = ib.reqMktData(skew, '', False, False).marketPrice()
        
        if math.isnan(self.curr_skew):
            skew = pd.read_csv('http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/skewdailyprices.csv', 
                               skiprows = 1)[['Date','SKEW']].set_index('Date')
            self.curr_skew = skew.reset_index().loc[len(skew) - 1, 'SKEW']
        
        try:
            chains = ib.reqSecDefOptParams(spx.symbol, '', spx.secType, spx.conId)
    
            chains_df = util.df(chains)
            chain = next(c for c in chains if c.tradingClass == 'SPXW' and c.exchange == 'SMART')
    
            bounds = self.curr_spx*(self.curr_vix/(100*np.sqrt(52)))
            
            strikes = [strike for strike in chain.strikes
                       if strike % 5 == 0
                       and self.curr_spx - bounds < strike < self.curr_spx + bounds]
            expirations = sorted(exp for exp in chain.expirations)[:number_of_expiries]
            self.expirations_dates = [dt.datetime.strptime(x, '%Y%m%d') for x in expirations]
            
            rights = ['P', 'C']

            contracts = [Option('SPX', expiration, strike, right, 'SMART')
                    for right in rights
                    for expiration in expirations
                    for strike in strikes]
            
            ib.qualifyContracts(*contracts)
            
            spx_options = ib.reqTickers(*contracts)
            
            puts_rows = []
            calls_rows = []
            
            for curr_option in spx_options:
                curr_row = create_dfrow(curr_option)
                if curr_option.contract.right == 'P':
                    puts_rows.append(curr_row)
                elif curr_option.contract.right == 'C':
                    calls_rows.append(curr_row)
                    
            puts = pd.concat(puts_rows).reset_index(drop = True).sort_values(['Expiry','Strike'])
            calls = pd.concat(calls_rows).reset_index(drop = True).sort_values(['Expiry','Strike'])
            
            self.puts = time_remaining(puts, self.curr_spx)
            self.calls = time_remaining(calls, self.curr_spx)
            
            curr_time = dt.datetime.now().strftime('%Y%m%d-%H-%M')
            self.puts.to_csv('ib_puts' + curr_time + '.csv')
            self.calls.to_csv('ib_calls' + curr_time + '.csv')
            
        except:
            latest_calls = sorted(list(filter(lambda x: 'ib_calls' in x, os.listdir())))[-1]
            latest_puts = sorted(list(filter(lambda x: 'ib_puts' in x, os.listdir())))[-1]
            self.calls = pd.read_csv(latest_calls, index_col = 0)
            self.calls.Expiry = pd.to_datetime(self.calls.Expiry)
            self.puts = pd.read_csv(latest_puts, index_col = 0)
            self.puts.Expiry = pd.to_datetime(self.puts.Expiry)
            
            if 'time_remaining' not in self.calls.columns.tolist():
                self.calls = time_remaining(self.calls, self.curr_spx)
            if 'time_remaining' not in self.puts.columns.tolist():
                self.puts = time_remaining(self.puts, self.curr_spx)

        ib.disconnect()
        return
    
#%%

