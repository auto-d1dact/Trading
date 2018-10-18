# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 17:26:09 2018

@author: Fang
"""

import pandas as pd
import numpy as np
import datetime as dt

# Simple portfolio class
class portfolio:
    
    def __init__(self, cash):
        self.cash = cash
        
        self.holdings = {}
        
        self.nav = cash
        
        return
    
    
    def update(self, period_data, contract_multiplier = 1):
        
        equity_val = 0
        
        for tick, values in self.holdings.items():
            try:
                market_price = period_data.loc[tick, 'Price']
            except:
                market_price = values[-1]
                print("No Market Price")
            
            purchase_price, shares, cb, old_market_price = values
            self.holdings[tick] = (purchase_price, shares, cb, market_price)
            
            equity_val += shares*market_price*contract_multiplier
            
        self.nav = self.cash + equity_val
        
        return
        
        
    def buy(self, ticker, price, shares, contract_multiplier = 1):
        
        if self.holdings.get(ticker, 0) != 0:
            old_bought_price, old_shares, old_cb, old_price = self.holdings.get(ticker)
            new_bought_price, new_shares, new_cb, new_price = (price, old_shares + shares, 
                                                               old_cb + price*shares*contract_multiplier, price)
            self.holdings[ticker] = (new_bought_price, new_shares, new_cb, new_price)
            
            if new_shares == 0:
                self.holdings.pop(ticker)
            
        else:
            self.holdings[ticker] = (price, shares, price*shares*contract_multiplier, price)
        
        self.cash = self.cash - price*shares*contract_multiplier
        
        #self.update(period_data)
        
        return
    
    def sell(self, ticker, price, shares, contract_multiplier = 1):
        
        if self.holdings.get(ticker, 0) != 0:
            old_sold_price, old_shares, old_cb, old_price = self.holdings.get(ticker)
            new_sold_price, new_shares, new_cb, new_price = (price, old_shares - shares,
                                                             old_cb - price*shares*contract_multiplier, price)
            self.holdings[ticker] = (new_sold_price, new_shares, new_cb, new_price)
            
            if new_shares == 0:
                self.holdings.pop(ticker)
        else:
            self.holdings[ticker] = (price, -shares, price*shares*contract_multiplier, price)
            
        self.cash = self.cash + price*shares*contract_multiplier
        
        #self.update(period_data)
        
        return
    
    
    def close_positions(self, period_data, contract_multiplier = 1):
        
        curr_holdings = self.holdings.copy().items()

        for tick, values in curr_holdings:
            
            ticker_price = period_data.loc[tick, 'Price']
            
            if values[1] > 0:
                self.sell(tick, ticker_price, abs(values[1]), contract_multiplier)
            elif values[1] < 0:
                self.buy(tick, ticker_price, abs(values[1]), contract_multiplier)
            else:
                None
        return
    
    def __repr__(self):
        return "Cash: {0}\nNAV: {1}\nHoldings: ".format(self.cash, self.nav) + str(self.holdings)
    
    def __str__(self):
        return "Cash: {0}\nNAV: {1}\nHoldings: ".format(self.cash, self.nav) + str(self.holdings)
    
    
    
# Simple backtest class
class backtest:
    
    def __init__(self, data, date_col, price_col, ticker_col, bid_col, ask_col, **extra_cols):
        
        self.pnl = []
        self.portfolio_nav = []
        self.portfolio_holdings = []
        
        try:
            
            self.data = data[[ticker_col, bid_col, ask_col, price_col]]
            self.data.columns = ['Symbol', 'Bid', 'Ask', 'Price']
            
            for name, col_name in extra_cols.items():
                self.data[name] = data[col_name]
            
            
            self.data.index = pd.to_datetime(data[date_col])
            
            self.period = pd.to_datetime(data[date_col].drop_duplicates()).sort_values().reset_index(drop = True)
            
            print("Backtest Initialized")
            return
        
        except:
            print("Failed Backtest Initialization")
            return
    
    def run(self, starting_cash, trade_rules):
        
        i = 0
        
        #self.period = self.period[:30]
        time_length = len(self.period)
        for date in self.period:
            
            if i == 0:
                pf = portfolio(5000)
                self.pnl = [0]
                self.portfolio_nav = [starting_cash]
                self.portfolio_holdings = [{}]
                i = 1
            
            #print(date)
            period_data = self.data[self.data.index == date].set_index('Symbol')
            
            trade_rules(pf, period_data)
            
            pf.update(period_data, 100)
            
            self.pnl.append(pf.nav - self.portfolio_nav[-1])
            self.portfolio_nav.append(pf.nav)
            self.portfolio_holdings.append(pf.holdings)
            #print(pf.holdings)
            
            completed = 100*len(self.portfolio_nav)/time_length
            print("Completed: {}%".format("%.2f" % completed))
            
        return pd.DataFrame({'PnL': self.pnl[:-1], 'NAV': self.portfolio_nav[:-1]},
                            index = self.period)