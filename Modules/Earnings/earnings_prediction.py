# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 13:44:56 2018

@author: Fang
"""

import os
import pandas as pd
import datetime as dt
import numpy as np


from sklearn import tree
from sklearn.cross_validation import train_test_split

main_dir = 'C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\Earnings'
os.chdir(main_dir)
from option_slam_earnings import *
from yahoo_earnings import *

os.chdir('C:\\Users\\Fang\\Desktop\\Python Trading\\Trading\\Trading\Modules\\DataCollection')
from yahoo_query import *

# Initializing Cleaned Earnings Data for Modelling
os.chdir('..\\')
os.chdir('..\\')
os.chdir('..\\Data\\Historical Queries\\Stock Prices')

#file_date = '2018-10-14'
#testsize = 0.8
#return_bounds = 0.05

def clf_predict_earnings(file_date, return_bounds, testsize = 0.8):
    rawdf = pd.read_csv('earnings_input_data-{}.csv'.format(file_date), index_col = 0)
    rawdf['index'] = pd.to_datetime(rawdf['index'])
    os.chdir(main_dir)
    
    rawdf['alpha52WeekVsIndustry'] = rawdf['Stock52WeekReturn'] - rawdf['Industry52WeekReturn']
    rawdf['alpha52WeekVsMarket'] = rawdf['Stock52WeekReturn'] - rawdf['SPY52WeekReturn']
    
    #%% Creating X
    inputCols = ['current_ratio_quarterly',
                 'total_debt_equity_ratio_quarterly',
                 'day_payable_outstanding_quarterly',
                 'total_liabilities_total_assets_quarterly', 'gross_margin_quarterly',
                 'operating_margin_quarterly', 'net_profit_margin_quarterly',
                 'changeInCash_quarterly', 'changeToLiabilities_quarterly',
                 'changeToNetincome_quarterly', 'changeToOperatingActivities_quarterly',
                 'current_ratio_annual', 'total_debt_equity_ratio_annual',
                 'day_payable_outstanding_annual',
                 'total_liabilities_total_assets_annual', 'gross_margin_annual',
                 'operating_margin_annual', 'net_profit_margin_annual',
                 'changeInCash_annual', 'changeToLiabilities_annual',
                 'changeToNetincome_annual', 'changeToOperatingActivities_annual',
                 'current_ratio_change', 'total_debt_equity_ratio_change',
                 'day_payable_outstanding_change',
                 'total_liabilities_total_assets_change', 'gross_margin_change',
                 'operating_margin_change', 'net_profit_margin_change',
                 'changeInCash_change', 'changeToLiabilities_change',
                 'changeToNetincome_change', 'changeToOperatingActivities_change','IndustryBeta',
                 'MarketBeta', 'alpha52WeekVsIndustry',
                 'alpha52WeekVsMarket']
    
    X_df = pd.concat([rawdf[inputCols], pd.get_dummies(rawdf[['sector']])], axis = 1)
    
    # Creating Y
    Y_df = (rawdf['PostEarningsReturn'] <= -return_bounds)*-1 + (rawdf['PostEarningsReturn'] >= return_bounds)*1
    
    #%% Decision Tree Model
    X_train, X_test, y_train, y_test = train_test_split(X_df, Y_df, test_size = testsize, random_state = 0)
    
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(X_train, y_train)
    
    #
    y_actu = pd.Series(np.array(y_test), name='Actual')
    y_pred = pd.Series(clf.predict(X_test), name='Predicted')
    df_confusion = pd.crosstab(y_actu, y_pred)
    
    print(df_confusion)
        
    return clf


#%%
# DecisionTreeClassifier is capable of both binary (where the labels are [-1, 1]) classification and multiclass (where the labels are [0, …, K-1]) classification.

# Using the Iris dataset, we can construct a tree as follows:

#from sklearn.datasets import load_iris
#from sklearn import tree
#iris = load_iris()
#clf = tree.DecisionTreeClassifier()
#clf = clf.fit(iris.data, iris.target)
#
#import graphviz 
#dot_data = tree.export_graphviz(clf, out_file=None) 
#graph = graphviz.Source(dot_data) 
#graph.render("iris") 
#
#dot_data = tree.export_graphviz(clf, out_file=None, 
#                                feature_names=iris.feature_names,  
#                                class_names=iris.target_names,  
#                                filled=True, rounded=True,  
#                                special_characters=True)  
#graph = graphviz.Source(dot_data)  
#graph 

#from sklearn.tree import convert_to_graphviz
#import graphviz

#graphviz.Source(export_graphviz(tree))