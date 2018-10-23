# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 23:37:31 2018

@author: Fang
"""

import numpy as np
import pandas as pd
import datetime as dt
from pandas_datareader.data import Options

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
        
    # return df.fillna(0).reset_index()[df.columns]
    return df.reset_index()[df.columns]

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
    # return df.fillna(0).reset_index()[df.columns]
    return df.reset_index()[df.columns]

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
    
    # return df.fillna(0).reset_index()[df.columns]
    return df.reset_index()[df.columns]


def gamma(options_df, interest_rate = 0.0193, q = 0, year = 252):
    D1 = d1(options_df, interest_rate, q, year)
    numerator = np.exp(-q * options_df.DTE/year) * norm.pdf(D1)
    denominator = options_df.Underlying_Price * options_df.IV * np.sqrt(options_df.DTE/year)
    options_df['Gamma'] = numerator / denominator

    return options_df#.fillna(0)


def vega(options_df, interest_rate = 0.0193, q = 0, year = 252):
    D1 = d1(options_df, interest_rate, q, year)
    options_df['Vega'] = options_df.Underlying_Price * np.exp(-q * options_df.DTE/year) * norm.pdf(D1) * np.sqrt(options_df.DTE/year) * 0.01

    return options_df#.fillna(0)


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
    
    #return df.fillna(0).reset_index()[df.columns]
    return df.reset_index()[df.columns]

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

#%%

def spx_put_backratios(dte_ub, dte_lb, moneyness, long_contracts, max_distance):
    spxchain = all_greeks(all_options("^SPX",dte_ub,dte_lb,moneyness))
    
    puts = spxchain[(spxchain['Type'] == 'put') &
                    (spxchain['Strike'] <= spxchain['Underlying_Price'])].reset_index(drop = True)

    
    ratios = []
    
    for curr_dte in puts.DTE.drop_duplicates().tolist():
        
        curr_puts = puts[puts.DTE == curr_dte].sort_values('Strike', ascending = False).reset_index(drop = True)
    
        for idx, row in curr_puts.iterrows():
            short = row
            longs = curr_puts[(curr_puts.Strike < short.Strike) & (short.Strike - curr_puts.Strike <= max_distance)]
            if len(longs) > 0:
                longs['Short_Strike'] = short.Strike
                longs['Short_Mid'] = short.Mid
                longs = longs[longs.Mid*long_contracts < longs.Short_Mid]
                longs['S_Delta'] = longs.Delta*long_contracts - short.Delta
                longs['S_Gamma'] = longs.Gamma*long_contracts - short.Gamma
                longs['S_Vega'] = longs.Vega*long_contracts - short.Vega
                longs['S_Theta'] = longs.Theta*long_contracts - short.Theta
                ratios.append(longs)
    ratio_df = pd.concat(ratios, axis = 0)
    ratio_df['Credit'] = ratio_df['Short_Mid'] - ratio_df['Mid']*long_contracts
    ratio_df = ratio_df.sort_values('Credit', ascending = False).reset_index()
    return ratio_df