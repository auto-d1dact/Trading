# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 23:56:29 2018

@author: Fang
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import datetime as dt

import os
main_dir = os.getcwd()

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\DataCollection')

from yahoo_query import *
from option_slam_earnings import *
from reuters_query import reuters_query


os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\\Modules\\Earnings')
from yahoo_earnings import *

os.chdir(main_dir)

#%%
def scoring(table, lower = True):
    score_df = table[['Underlying','Field']]
    
    if lower:
        score_df['Score'] = (table['Company'] < table['industry'])*1 + (table['Company'] < table['sector'])*1
    else:
        score_df['Score'] = (table['Company'] > table['industry'])*1 + (table['Company'] > table['sector'])*1
    score = np.round(score_df.Score.sum()/(2*len(score_df))*100, decimals = 2)
    return score

def fin_scoring(curr_finstrength, lower = True):
    fin_score_raw = 0
    total_score_raw = 0
    
    for idx, row in curr_finstrength.iterrows():
        
        if 'Quick Ratio' in row.Field or 'Current Ratio' in row.Field or 'Interest Coverage' in row.Field:
            score = (row['Company'] > row['industry'])*1 + (row['Company'] > row['sector'])*1
        elif 'Debt to Equity' in row.Field:
            score = (row['Company'] < row['industry'])*1 + (row['Company'] < row['sector'])*1
        else:
            score = 0
            
        fin_score_raw += score
        total_score_raw += 2
        
    fin_score = np.round(fin_score_raw/total_score_raw*100, decimals = 2)
    
    return fin_score

def perf_scoring(curr_performance_summary):
    total_rank = 99*len(curr_performance_summary)
    rank_score = (curr_performance_summary.IndustryRank.sum() + curr_performance_summary.RankInIndustry.sum())/total_rank
    rank_score = np.round(rank_score/2*100, decimals = 2)
    
    total_return_score = len(curr_performance_summary)
    return_score = ((curr_performance_summary.PercentActual > 0).sum() + (curr_performance_summary['Percentvs.S&P500'] > 0).sum())/total_return_score
    return_score = np.round(return_score/2*100, decimals = 2)

    return rank_score, return_score


def lookup_earnings(focus_names, start_date):
    eps_cols = ['epsActual', 'epsDifference', 'epsEstimate', 'surprisePercent', 'Underlying',
                'Quarter', 'Stock_closeToOpen', 'EarningsDate']
    
    eps_lst = []
    summary_list = []
    scores_list = []
    analyst_list = []
    valuations_list = []
    growthrate_list = []
    profitability_list = []
    finstrength_list = []
    perf_list = []
    revenue_revisions = []
    earnings_revisions = []
    insider_txns = []
    growth_summaries = []
    
    total_length = len(focus_names)
    curr_counter = 0
    for ticker in focus_names:
        
        try: 
            # Reuters Data
            curr_score_dict = {'ValuationScore':[],
                               'GrowthRateScore':[],
                               'ProfitScore':[],
                               'FinStrengthScore':[],
                               'PerfRankScore':[],
                               'ReturnScore':[]}
            curr_reuters = reuters_query(ticker)
            
            # Earnings Historical Results
            curr_earnings_report = earnings_report(ticker).dropna()
            curr_earnings_report.columns = [x.replace(ticker,'Stock') for x in curr_earnings_report.columns[:-1]] + ['52WeekSectorReturn']
            
            # Yahoo Query
            curr_yahoo = yahoo_query(ticker, start_date)
            curr_yahoo.full_info_query()
            
            
            # EPS Data
            curr_eps = curr_yahoo.earnings_quarterly
            curr_eps['Underlying'] = ticker
            
            curr_eps['Quarter'] = curr_eps.index
            curr_eps = curr_eps.reset_index(drop = True)
        
            curr_earnings_report = curr_earnings_report.dropna().tail(len(curr_eps))
            curr_earnings_report['EarningsDate'] = curr_earnings_report.index
            curr_earnings_report = curr_earnings_report.reset_index(drop = True)
            
            curr_eps = curr_eps.join(curr_earnings_report)[eps_cols]
            curr_eps['surprisePercent'] = pd.to_numeric(curr_eps['surprisePercent'])
            
            surpriseReturnCorrelation = curr_eps[['surprisePercent','Stock_closeToOpen']].corr().iloc[0,1]
            
            eps_lst.append(curr_eps)
            
            # Summary
            curr_overall_df = curr_reuters.overall_df
            summary_list.append(curr_overall_df)
            
            # Analyst recommendations
            curr_analyst_recs = curr_reuters.analyst_recs
            curr_analyst_recs = curr_analyst_recs[curr_analyst_recs['1-5LinearScale'] == 'Mean Rating'].set_index('Underlying')
            analyst_list.append(curr_analyst_recs)
            
            # Valuations - Lower is better for scoring
            curr_valuations = curr_reuters.valuations
            curr_valuations = curr_valuations[curr_valuations.Field != '% Owned Institutions']
            curr_vscore = scoring(curr_valuations, lower = True)
            curr_score_dict['ValuationScore'].append(curr_vscore)
            
            # Growthrates - Higher is better for scoring
            curr_growthrate = curr_reuters.growthrate
            curr_growthratescore = scoring(curr_growthrate, lower = False)
            curr_score_dict['GrowthRateScore'].append(curr_growthratescore)
            
            # Profitability -Higher is better for scoring
            curr_profitability = curr_reuters.profitability
            curr_profitability = curr_profitability[curr_profitability["Field"].str.contains('Tax Rate')==False]
            curr_profitscore = scoring(curr_profitability, lower = False)
            curr_score_dict['ProfitScore'].append(curr_profitscore)
            
            # Financial Strength
            curr_finstrength = curr_reuters.finstrength
            curr_finscore = fin_scoring(curr_finstrength)
            curr_score_dict['FinStrengthScore'].append(curr_finscore)
            
            # Performance Summary
            curr_performance_summary = curr_reuters.performance_summary
            curr_rankscore, curr_returnscore = perf_scoring(curr_performance_summary)
            curr_score_dict['PerfRankScore'].append(curr_rankscore)
            curr_score_dict['ReturnScore'].append(curr_returnscore)
            
            # Creating score dataframe
            curr_scores = pd.DataFrame(curr_score_dict)
            curr_scores.index = [ticker]
            scores_list.append(curr_scores)
            
            # Extra information
            curr_revenue_revisions = curr_reuters.revenue_revisions
            curr_earnings_revisions = curr_reuters.earnings_revisions
            curr_insiders_txns = curr_reuters.insiders_txns
            curr_growth_summary = curr_reuters.growth_summary
            
            
            # Appending to dataframe lists
            valuations_list.append(curr_valuations)
            growthrate_list.append(curr_growthrate)
            profitability_list.append(curr_profitability)
            finstrength_list.append(curr_finstrength)
            perf_list.append(curr_performance_summary)
            revenue_revisions.append(curr_revenue_revisions)
            earnings_revisions.append(curr_earnings_revisions)
            insider_txns.append(curr_insiders_txns)
            growth_summaries.append(curr_growth_summary)
        except:
            continue
        
        curr_counter += 1
        print("{}% Completed".format('%.2f' % float(100*curr_counter/total_length)))
    
    eps_df = pd.concat(eps_lst, axis = 0)
    summary_df = pd.concat(summary_list, axis = 0)
    scores_df = pd.concat(scores_list, axis = 0)
    analyst_df = pd.concat(analyst_list, axis = 0)
    valuations_df = pd.concat(valuations_list, axis = 0)
    growthrate_df = pd.concat(growthrate_list, axis = 0)
    profitability_df = pd.concat(profitability_list, axis = 0)
    finstrength_df = pd.concat(finstrength_list, axis = 0)
    perf_df = pd.concat(perf_list, axis = 0)
    rev_revisions_df = pd.concat(revenue_revisions, axis = 0)
    earnings_revisions_df = pd.concat(earnings_revisions, axis = 0)
    insider_trades = pd.concat(insider_txns, axis = 0)
    growth_sums = pd.concat(growth_summaries, axis = 0)
    
    return [eps_df, summary_df, scores_df, analyst_df, valuations_df, growthrate_df, profitability_df, finstrength_df, perf_df, rev_revisions_df, earnings_revisions_df,insider_trades,growth_sums]




