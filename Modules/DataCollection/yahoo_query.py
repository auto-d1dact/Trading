# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 21:28:57 2018

@author: Fang
"""

import pandas as pd
import datetime as dt
import json
import numpy as np
from pandas.io.json import json_normalize
import urllib.request as urlreq

# Creating class for querying yahoo data
# yahoo_query(str[self.ticker], datetime.date([starting_date]), datetime.date([ending_date])):
    
'''
Inputs for the ?modules= query:

modules = [
   'assetProfile',
   'incomeStatementHistory',
   'incomeStatementHistoryQuarterly',
   'balanceSheetHistory',
   'balanceSheetHistoryQuarterly',
   'cashflowStatementHistory',
   'cashflowStatementHistoryQuarterly',
   'defaultKeyStatistics',
   'financialData',
   'calendarEvents',
   'secFilings',
   'recommendationTrend',
   'upgradeDowngradeHistory',
   'institutionOwnership',
   'fundOwnership',
   'majorDirectHolders',
   'majorHoldersBreakdown',
   'insiderTransactions',
   'insiderHolders',
   'netSharePurchaseActivity',
   'earnings',
   'earningsHistory',
   'earningsTrend',
   'industryTrend',
   'indexTrend',
   'sectorTrend' ]
Example URL:

https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=assetProfile%2CearningsHistory
Querying for: assetProfile and earningsHistory

'''


# Creating class for querying yahoo data
# yahoo_query(str[self.ticker], datetime.date([starting_date]), datetime.date([ending_date])):
class yahoo_query:
    
    # Initializing yahoo_query class with self.ticker and creating
    # relevant URL api calls to query relevant data
    def __init__(self, ticker, start_date, end_date = dt.datetime.today()):
        start_date_unix = int(start_date.timestamp())
        end_date_unix = int(end_date.timestamp())
        
        self.ticker = ticker
        self.minute_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1m'.format(self.ticker)
        self.hist_price_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&period1={1}&period2={2}&interval=1d'.format(self.ticker,start_date_unix,end_date_unix)
        self.options_url = 'https://query1.finance.yahoo.com/v7/finance/options/{}'.format(self.ticker)
        self.quick_summary_url = 'https://query1.finance.yahoo.com/v7/finance/quote?symbols={}'.format(self.ticker)
        
        modules = '%2C'.join(['assetProfile','incomeStatementHistory', 'balanceSheetHistoryQuarterly',
                              'balanceSheetHistory','cashflowStatementHistory', 'cashflowStatementHistoryQuarterly',
                              'defaultKeyStatistics','financialData','incomeStatementHistoryQuarterly',
                              'calendarEvents','secFilings', 'recommendationTrend', 'institutionOwnership',
                              'fundOwnership', 'majorDirectHolders', 'majorHoldersBreakdown',
                              'insiderTransactions', 'insiderHolders', 'netSharePurchaseActivity',
                              'earnings', 'earningsHistory', 'earningsTrend', 'industryTrend', 'indexTrend',
                              'sectorTrend'])
        self.full_info_url = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{0}?modules={1}'.format(self.ticker, modules)
        
        fields = ','.join(['ebitda','shortRatio','priceToSales','priceToBook','trailingPE','forwardPE','marketCap',
                           'trailingAnnualDividendRate','trailingAnnualDividendYield','sharesOutstanding','bookValue',
                           'epsTrailingTwelveMonths','epsForward'])
        self.fin_statements_url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={0}&fields={1}".format(self.ticker,fields)
    
    # Class method for querying yahoo minute data using
    # minute_url defined on initialization
    def minute_query(self):
        with urlreq.urlopen(self.minute_url) as url:
            data = json.loads(url.read().decode())
            self.minute_prices = pd.DataFrame(data['chart']['result'][0]['indicators']['quote'][0],
                                            index = [dt.datetime.utcfromtimestamp(int(x)) - 
                                                     dt.timedelta(hours = 4) for x in 
                                                     data['chart']['result'][0]['timestamp']])
            self.minute_prices.index = pd.to_datetime(self.minute_prices.index)
            self.minute_prices.columns = ["{0}_{1}".format(self.ticker, x) for x in self.minute_prices.columns]
            
    # Class method for querying yahoo historical prices
    # using hist_price_url on initialization
    def hist_prices_query(self):
        with urlreq.urlopen(self.hist_price_url) as url:
            data = json.loads(url.read().decode())
            self.hist_prices = pd.DataFrame(data['chart']['result'][0]['indicators']['quote'][0],
                                            index = [dt.datetime.utcfromtimestamp(int(x)).date() for x in 
                                                     data['chart']['result'][0]['timestamp']])
            self.hist_prices.index = pd.to_datetime(self.hist_prices.index)
            self.hist_prices.columns = ["{0}_{1}".format(self.ticker, x) for x in self.hist_prices.columns]
            
    # Class method for querying yahoo data for all modules from the above defined modules list
    # using full_info_url on initialization
    def full_info_query(self):
        with urlreq.urlopen(self.full_info_url) as url:
            data = json.loads(url.read().decode())
                        
            ########## Creating earnings_history dataframe
            earnings_annual = pd.concat([pd.DataFrame(earnings).loc['raw'] for 
                                         earnings in 
                                         data['quoteSummary']['result'][0]['earnings']['financialsChart']['yearly']], axis = 1).T
            earnings_annual.index = earnings_annual.date
            self.earnings_annual = earnings_annual.drop(['date'], axis = 1)
            
            
            earnings_quarterly = pd.concat([pd.DataFrame(quarter_earnings).loc['raw'] for 
                                            quarter_earnings in 
                                            data['quoteSummary']['result'][0]['earningsHistory']['history']],
                                         axis = 1).T
            earnings_quarterly.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                       x in earnings_quarterly['quarter'].tolist()])
            self.earnings_quarterly = earnings_quarterly.drop(['period','maxAge','quarter'], axis = 1)
            
            ########### Creating company profile (risk metrics) dataframe
            profileKeys = list(data['quoteSummary']['result'][0]['assetProfile'].keys())
            checkKeys = ['industry', 'sector', 'fullTimeEmployees', 'auditRisk', 
                         'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 
                         'overallRisk']
            keyList = list(filter(lambda x: x in checkKeys, profileKeys))
            
            self.profile = pd.DataFrame(dict((k, data['quoteSummary']['result'][0]['assetProfile'][k]) for 
                                             k in keyList), index = [self.ticker])
            
            try:
                ########### Creating executives profile dataframe
                executives = pd.concat([pd.DataFrame(executive).loc['raw'] for 
                                        executive in data['quoteSummary']['result'][0]['assetProfile']['companyOfficers']],
                                       axis = 1).T
                executives.index = executives.title
                self.executives = executives.drop(['title','maxAge','yearBorn'], axis = 1)
            except:
                pass
            
            ########### Creating historical cashflow statements dataframe
            cashFlowStatementAnnual = pd.concat([pd.DataFrame(cfstatement).loc['raw'] for cfstatement in 
                                                 data['quoteSummary']['result'][0]['cashflowStatementHistory']['cashflowStatements']],
                                                axis = 1).T
            cashFlowStatementAnnual.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                            x in cashFlowStatementAnnual['endDate'].tolist()])
            self.cashFlowStatementAnnual = cashFlowStatementAnnual.drop(['endDate', 'maxAge'], axis = 1)
            
            cashFlowStatementQuarter = pd.concat([pd.DataFrame(cfstatement).loc['raw'] for cfstatement in 
                                                  data['quoteSummary']['result'][0]['cashflowStatementHistoryQuarterly']['cashflowStatements']],
                                                 axis = 1).T
            cashFlowStatementQuarter.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                             x in cashFlowStatementQuarter['endDate'].tolist()])
            self.cashFlowStatementQuarter = cashFlowStatementQuarter.drop(['endDate', 'maxAge'], axis = 1)
            
            try:
                ########### Creating institutional ownership information for company
                institutionOwn = pd.concat([pd.DataFrame(owners).loc['raw'] for owners in 
                                            data['quoteSummary']['result'][0]['institutionOwnership']['ownershipList']],
                                          axis = 1).T
                institutionOwn.reportDate = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for
                                                            x in institutionOwn['reportDate'].tolist()])
                institutionOwn.index = institutionOwn.organization
                self.institutionOwners = institutionOwn.drop(['maxAge', 'organization'], axis = 1)
            except:
                pass
            
            try:
                ########### Creating major holders info dataframe
                self.majorHolderInfo = pd.DataFrame(data['quoteSummary']['result'][0]['majorHoldersBreakdown']).drop('maxAge', 
                                                                                                                axis = 1).loc[['raw']]
                self.majorHolderInfo.index = [self.ticker]
            except:
                pass
            
            try:
                ########### Creating recommendation trend dataframe
                self.recommendationTrend = pd.concat([pd.DataFrame(trend, index = [trend['period']]) for trend in 
                                                      data['quoteSummary']['result'][0]['recommendationTrend']['trend']],
                                                     axis = 0).T.drop('period')
            except:
                pass
            
            ########### Creating key statistics dataframe
            self.keyStats = pd.DataFrame(data['quoteSummary']['result'][0]['defaultKeyStatistics']).loc[['raw']]
            self.keyStats.index = [self.ticker]
            
            try:
                ########### Creating share purchase dataframe
                self.purchaseActivity = pd.DataFrame(data['quoteSummary']['result'][0]['netSharePurchaseActivity']).loc[['raw']]
                self.purchaseActivity.index = [self.ticker]
            except:
                pass
            
            try:
                ########### Creating insider transactions dataframe
                insiderTxns = pd.concat([pd.DataFrame(filer).loc[['raw']] for filer in data['quoteSummary']['result'][0]['insiderTransactions']['transactions']])
                insiderTxns.index = insiderTxns.filerName
                insiderTxns.startDate = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                        x in insiderTxns.startDate.tolist()])
                self.insiderTxns = insiderTxns.drop(['filerUrl','maxAge','moneyText','filerName'], axis = 1)
            except:
                pass
            
            ########### Creating historical income statement dataframe
            incomeStatementAnnual = pd.concat([pd.DataFrame(incomeStatement).loc['raw'] for 
                                   incomeStatement in 
                                   data['quoteSummary']['result'][0]['incomeStatementHistory']['incomeStatementHistory']],
                                  axis = 1).T

            incomeStatementAnnual.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                          x in incomeStatementAnnual['endDate'].tolist()])
            self.incomeStatementAnnual = incomeStatementAnnual.drop('endDate', axis = 1)


            incomeStatementQuarter = pd.concat([pd.DataFrame(incomeStatement).loc['raw'] for 
                                                incomeStatement in 
                                                data['quoteSummary']['result'][0]['incomeStatementHistoryQuarterly']['incomeStatementHistory']],
                                               axis = 1).T

            incomeStatementQuarter.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                           x in incomeStatementQuarter['endDate'].tolist()])
            self.incomeStatementQuarter = incomeStatementQuarter.drop('endDate', axis = 1)
            
            ############ Creating historical balance sheet statement dataframe
            balanceSheetAnnual = pd.concat([pd.DataFrame(balanceSheet).loc['raw'] for 
                                   balanceSheet in 
                                   data['quoteSummary']['result'][0]['balanceSheetHistory']['balanceSheetStatements']],
                                  axis = 1).T

            balanceSheetAnnual.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                          x in balanceSheetAnnual['endDate'].tolist()])
            self.balanceSheetAnnual = balanceSheetAnnual.drop('endDate', axis = 1)


            balanceSheetQuarter = pd.concat([pd.DataFrame(balanceSheet).loc['raw'] for 
                                                balanceSheet in 
                                                data['quoteSummary']['result'][0]['balanceSheetHistoryQuarterly']['balanceSheetStatements']],
                                               axis = 1).T

            balanceSheetQuarter.index = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)).date() for 
                                                           x in balanceSheetQuarter['endDate'].tolist()])
            self.balanceSheetQuarter = balanceSheetQuarter.drop('endDate', axis = 1)
            
            try:
                ############ Creating fund ownership dataframe
                self.fundOwnership = pd.concat([pd.DataFrame(fundOwner).loc['raw'] for 
                                                fundOwner in 
                                                data['quoteSummary']['result'][0]['fundOwnership']['ownershipList']],
                                               axis = 1).T.drop('maxAge', axis = 1)
    
                self.fundOwnership.index = self.fundOwnership.organization
                self.fundOwnership.reportDate = pd.to_datetime([dt.datetime.utcfromtimestamp(int(x)) for 
                                                                x in self.fundOwnership.reportDate])
            except:
                pass
            
            try:
                ############ Creating insider holders dataframe
                self.insiderHolders = pd.concat([pd.DataFrame(holder).loc['raw'] for 
                                                 holder in data['quoteSummary']['result'][0]['insiderHolders']['holders']],
                                                axis = 1).T.drop('maxAge', axis = 1)
                self.insiderHolders.index = range(len(self.insiderHolders))
            except:
                pass
            
            try:
                ############ Creating calendar events dataframe (dividend dates and earnings dates)
                self.currEarnings = pd.DataFrame(data['quoteSummary']['result'][0]['calendarEvents']['earnings']).loc[['raw']]
                self.dividends = pd.DataFrame(dict((k, dt.datetime.utcfromtimestamp(int(data['quoteSummary']['result'][0]['calendarEvents'][k]['raw'])).date()) for
                                                   k in ('dividendDate','exDividendDate')), index = [self.ticker])
            except:
                pass
            
            try:
                ############ Creating earnings estimate dataframe
                earningEsts = pd.concat([pd.DataFrame(estimate).loc['raw'] for estimate in 
                                         data['quoteSummary']['result'][0]['earningsTrend']['trend']],
                                        axis = 1).T.dropna(subset=['endDate'])
                earningEsts.index = pd.to_datetime([dt.datetime.strptime(date,'%Y-%m-%d').date() for date in earningEsts.endDate])
                self.earningEsts = earningEsts.drop('endDate', axis = 1)
            except:
                pass
            
            
            ############ Creating financial data
            self.finData = pd.DataFrame(data['quoteSummary']['result'][0]['financialData']).loc[['raw']]
            self.finData.index = [self.ticker]

        
    # Class method for querying most near-term options chain
    # using  options_url on initialization
    def latest_options_query(self):
        with urlreq.urlopen(self.options_url) as url:
            data = json.loads(url.read().decode())
            options = data['optionChain']['result'][0]['options'][0]
            options = pd.merge(json_normalize(options['calls']), json_normalize(options['puts']), how='inner', on = 'strike',
                               suffixes=('_calls', '_puts'))
            options['expiry'] = dt.datetime.utcfromtimestamp(options['expiration_calls'][0]).date()

            self.options = options.drop(['expiration_calls','expiration_puts','contractSize_calls',
                                         'currency_calls','contractSize_puts','currency_puts',
                                         'lastTradeDate_calls', 'lastTradeDate_puts', 'percentChange_calls',
                                         'percentChange_puts'], axis = 1)
            self.options.index = self.options.strike
            
    # Class method for querying quick quote summary using
    # quick_summary_url on initialzation
    def quick_quote_query(self):
        with urlreq.urlopen(self.quick_summary_url) as url:
            data = json.loads(url.read().decode())
            self.quick_quote = pd.DataFrame(data['quoteResponse']['result'][0], index = [self.ticker])
            for col in ['dividendDate','earningsTimestamp',
                        'earningsTimestampEnd', 'earningsTimestampStart']:
                self.quick_quote.loc[self.ticker,col] = dt.datetime.utcfromtimestamp(int(self.quick_quote.loc[self.ticker,col])).date()
    
    # Class method for querying financials quote summary using
    # fin_statements_url on initialization
    def financials_query(self):
        with urlreq.urlopen(self.fin_statements_url) as url:
            data = json.loads(url.read().decode())
            self.financials = pd.DataFrame(data['quoteResponse']['result'][0], index = [self.ticker])
            