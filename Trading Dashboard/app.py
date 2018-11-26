# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 21:58:46 2018

@author: fchen
"""

# Import required libraries
import os
import pandas as pd
# import datetime as dt

# import numpy as np
# import pandas as pd
# import plotly.plotly as py
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import dash_table_experiments as dasht
import data_processing
# import plotly.graph_objs as go

# Creating Dash application instance for browser application
app = dash.Dash()

# Loading external CSS for dashboard style
external_css = ["https://fonts.googleapis.com/css?family=Overpass:300,300i",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/dab6f937fd5548cebf4c6dc7e93a10ac438f5efb/dash-technical-charting.css"]

for css in external_css:
    app.css.append_css({"external_url": css})

# Dyno for local instance of application
if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })



#%% Make app layout
app.layout = html.Div(
    [
####### Page headers and title section #############################
        html.Div([
            html.Img(
                src="http://fchen.info/wp-content/uploads/2016/10/fclogo2.png",
                className='two columns',
                style={
                    'height': '60',
                    'width': '60',
                    'float': 'left',
                    'position': 'relative',
                },
            ),
            html.H1(
                'Market Dashboard',
                className='ten columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row'
        ),
        html.Div([
            html.H3(
                'INF 510 - Fall 2018',
                className='twelve columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row'
        ),
        html.Hr(style={'margin': '0', 'margin-bottom': '5'}),
        
####### SPX Intraday Plots and Summaries ################################
        html.Div([
            html.H4(
                'SPX Intraday Movements',
                className='twelve columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row',
            style={'margin-bottom': '20'}
        ), 
        html.Div([
            html.Div([
                    dcc.Graph(id='intraday_spx_plot', style={'max-height': '600', 'height': '60vh'}),
                    dcc.Interval(
                            id='interval-component',
                            interval=30*1000, # in milliseconds
                            n_intervals=0
                            )],
                    className = 'eight columns',
            ),
            html.Div([
                    html.Label('Intraday Summary'),
                    dasht.DataTable(
                            # Initialise the rows
                            rows=[{}],
                            row_selectable=True,
                            filterable=True,
                            sortable=True,
                            selected_row_indices=[],
                            id='intraday_table'
                            )],
                    className = 'four columns',
            )
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),
                    
####### Latest VIX Term Structure and VIX, VVIX, and Historical Vol Summary ###
        html.Div([
            html.H4(
                'Latest VIX Term Structure',
                className='twelve columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row',
            style={'margin-bottom': '20'}
        ), 
        html.Div([
            html.Div([
                    dcc.Graph(id='vix_plot', style={'max-height': '600', 'height': '60vh'})],
                    className = 'nine columns',
            ),
            html.Div([
                    html.Label('Volatility Summaries'),
                    dasht.DataTable(
                            # Initialise the rows
                            rows=[{}],
                            row_selectable=True,
                            filterable=True,
                            sortable=True,
                            selected_row_indices=[],
                            id='vol_table'
                            )],
                    className = 'three columns',
            )
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),

####### Industry Correlations ############################################
        html.Div([
            html.H4(
                'Industry to SPX Correlation - 60 day',
                className='twelve columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row',
            style={'margin-bottom': '20'}
        ), 
        html.Div([
            html.Div([
                    dcc.Graph(id='corr_sum_plot', style={'max-height': '600', 'height': '60vh'})],
                    className = 'twelve columns',
            )
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),
        html.Div([
            html.Div([
                    dcc.Graph(id='corr_plot', style={'max-height': '600', 'height': '60vh'})],
                    className = 'twelve columns',
            )
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),
            
####### SPX Daily Plotting ############################################
        html.Div([
            html.H4(
                'Latest SPX Daily Plot',
                className='twelve columns',
                style={'text-align': 'center'}
            ),
        ],
            className='row',
            style={'margin-bottom': '20'}
        ), 
        html.Div([
            html.Div([
                    dcc.Graph(id='spx_daily_plot', style={'max-height': '600', 'height': '60vh'})],
                    className = 'twelve columns',
            )
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),

####### Temporary hack for live dataframe caching ##################
# When 'hidden' is set to 'loaded', this triggers next callback
        html.P(
                hidden = '',
                id = 'cache_minute',
                style={'display':'none'}),
        html.P(
                hidden = '',
                id = 'cache_total',
                style={'display':'none'}),
        html.Div([
            html.Label('Days Lookback'),
            dcc.Slider(
                id='days-slider',
                min=20,
                max=60,
                marks={i: '{}'.format(i) if i == 1 else str(i) for i in range(20, 60)},
                value=60,
            ),
        ],
            className='row',
            style={'margin-bottom': '20'}
        ),
        
    ],
    style={
        'width': '85%',
        'max-width': '1200',
        'margin-left': 'auto',
        'margin-right': 'auto',
        'font-family': 'overpass',
        'background-color': '#FFFFFF',
        'padding': '40',
        'padding-top': '20',
        'padding-bottom': '20',
    },
)

#%% Function Callbacks for Dashboard
# Cache raw data
@app.callback(Output('cache_total', 'hidden'),
              [Input('days-slider', 'value')])
def cache_raw_day_data(days):

    global spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations
    spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations = data_processing.process_data('full', False)
    
    print('Loaded raw data')

    return 'loaded'

@app.callback(Output('cache_minute', 'hidden'),
              [Input('interval-component', 'n_intervals')])
def cache_raw_data(n):

    global spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations
    spx_trend_table, vol_summary_table, vix_term, spx_daily, spx_intraday, market_correlations = data_processing.process_data('intraday', False)
#    svxy_data = svxy_data()
    
    print('Loaded raw data')

    return 'loaded'

# Callback to update Intraday Summary
@app.callback(Output('intraday_table', 'rows'),
              [Input('cache_minute', 'hidden')])
def update_intraday_spx_table(hidden):
    
    if hidden == 'loaded':
        
        return spx_trend_table.apply(pd.to_numeric).round(4).reset_index().to_dict('records')
    
# Callback to update Intraday Summary
@app.callback(Output('vol_table', 'rows'),
              [Input('cache_minute', 'hidden')])
def update_volatility_table(hidden):
    
    if hidden == 'loaded':
        
        return vol_summary_table.apply(pd.to_numeric).round(4).reset_index().to_dict('records')
    
# Callback to update SPX Intraday Movements
@app.callback(Output('intraday_spx_plot', 'figure'),
              [Input('cache_minute', 'hidden')])
def update_spx_intraday_plot(hidden):
    if hidden == 'loaded':
        
        trace1 = go.Scatter(
                    x=spx_intraday.index,
                    y=spx_intraday['Last'],
                    mode='lines',
                    yaxis='y1',
                    name='SPX')
        
        trace2 = go.Scatter(
                    x=spx_intraday.index,
                    y=spx_intraday['SMA 5'],
                    mode='lines',
                    yaxis='y2',
                    name='SMA 5')
        
        trace3 = go.Scatter(
                    x=spx_intraday.index,
                    y=spx_intraday['SMA 20'],
                    mode='lines',
                    yaxis='y3',
                    name='SMA 20')
        
        trace4 = go.Bar(
                    x=spx_intraday.index,
                    y=spx_intraday['Dollar Std Move'],
                    yaxis='y4',
                    name='Dollar Std Move')
        
            
        layout = go.Layout(
                title='Intraday SPX Movements',
                yaxis=dict(
                        #range=[spx_intraday['Last'].min(),spx_intraday['Last'].max()],
                        title='',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        ),
                yaxis2=dict(
                        range=[spx_intraday['Last'].min(),spx_intraday['Last'].max()],
                        title='',
                        overlaying='y',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        ),
                yaxis3=dict(
                        range=[spx_intraday['Last'].min(),spx_intraday['Last'].max()],
                        title='',
                        overlaying='y',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        ),
                yaxis4=dict(
                        title='',
                        side='right',
                        overlaying='y',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        )
                )
            
        data = [trace1, trace2, trace3, trace4]
        figure = dict(data=data, layout=layout)
        return figure
    
## Multiple components can update everytime interval gets fired.
@app.callback(Output('vix_plot', 'figure'),
              [Input('cache_minute', 'hidden')])
def update_vix_term_structure(hidden):
    
    if hidden == 'loaded':
        trace1 = go.Scatter(
                x = vix_term.index,
                y = vix_term.PreviousStructure,
                hoverinfo='name+x+y',
                yaxis='y1',
                mode = 'lines+markers',
                name = 'VX Previous')
        
        trace2 = go.Scatter(
                x = vix_term.index,
                y = vix_term.CurrentStructure,
                hoverinfo='name+x+y',
                yaxis='y2',
                mode = 'lines+markers',
                name = 'VX Current')
        
        layout = go.Layout(
                title='VIX Term Structure',
                yaxis=dict(
                        range=[min(vix_term.min()) - 0.2,max(vix_term.max()) + 0.2],
                        title='',
                        #anchor='free',
                        side='left',
                        #showgrid=False,
                        #zeroline=False,
                        #showline=False,
                        #ticks='',
                        #showticklabels=False
                        ),
                yaxis2=dict(
                        range=[min(vix_term.min()) - 0.2,max(vix_term.max()) + 0.2],
                        title='',
                        overlaying='y',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        )
                )
            
        data = [trace1, trace2]
        figure = dict(data=data, layout=layout)
        return figure

# Callback to update Main Correlation Plot
@app.callback(Output('corr_sum_plot', 'figure'),
              [Input('cache_total', 'hidden')])
def update_main_correlations(hidden):
    if hidden == 'loaded':
        
        trace1 = go.Scatter(
                    x=market_correlations.index,
                    y=market_correlations['Avg_Corr'],
                    mode='lines',
                    yaxis='y1',
                    name='Average Correlation')
        
        trace2 = go.Scatter(
                    x=market_correlations.index,
                    y=market_correlations['SPX_cum'],
                    mode='lines',
                    yaxis='y2',
                    name='SPX Cumulative Return')        
            
        layout = go.Layout(
                title='Market Correlations and Cumulative Returns',
                yaxis=dict(
                        #range=[spx_intraday['Last'].min(),spx_intraday['Last'].max()],
                        title='Average Correlation',
                        #anchor='free',
                        side='left',
                        #showgrid=False,
                        #zeroline=False,
                        #showline=False,
                        #ticks='',
                        #showticklabels=False
                        ),
                yaxis2=dict(
                        title='',
                        overlaying='y',
                        #anchor='free',
                        side='right',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        )
                )
            
        data = [trace1, trace2]
        figure = dict(data=data, layout=layout)
        return figure
    
# Callback to update Industry Correlation Plot
@app.callback(Output('corr_plot', 'figure'),
              [Input('cache_total', 'hidden')])
def update_industry_correlations(hidden):
    if hidden == 'loaded':
        
        traces = []

        for i, col in enumerate(market_correlations.columns.tolist()[:-2]):
            curr_trace = go.Scatter(
                                x=market_correlations.index,
                                y=market_correlations[col],
                                mode='lines',
                                yaxis='y{}'.format(i + 1),
                                name=col)
            traces.append(curr_trace)
            
        subplotdict=dict(
                        range=[-1,1],
                        title='',
                        overlaying='y',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        )
            
        layout = go.Layout(
                title='Industry Correlations to Market',
                yaxis=dict(
                        range=[-1,1],
                        title='Correlation',
                        #anchor='free',
                        side='left',
                        #showgrid=False,
                        #zeroline=False,
                        #showline=False,
                        #ticks='',
                        #showticklabels=False
                        ),
                yaxis2=subplotdict,
                yaxis3=subplotdict,
                yaxis4=subplotdict,
                yaxis5=subplotdict,
                yaxis6=subplotdict,
                yaxis7=subplotdict,
                yaxis8=subplotdict,
                yaxis9=subplotdict,
                yaxis10=subplotdict
                )
            
        data = traces
        figure = dict(data=data, layout=layout)
        return figure

# Callback to update SPX Intraday Movements
@app.callback(Output('spx_daily_plot', 'figure'),
              [Input('cache_total', 'hidden')])
def update_spx_daily_plot(hidden):
    if hidden == 'loaded':
        
        trace1 = go.Scatter(
                    x=spx_daily.index,
                    y=spx_daily['GSPC'],
                    mode='lines',
                    yaxis='y1',
                    name='SPX')
        
        trace2 = go.Scatter(
                    x=spx_daily.index,
                    y=spx_daily['SMA 200'],
                    mode='lines',
                    yaxis='y2',
                    name='SMA 200')
        
        trace3 = go.Scatter(
                    x=spx_daily.index,
                    y=spx_daily['SMA 20'],
                    mode='lines',
                    yaxis='y3',
                    name='SMA 20')
        
            
        layout = go.Layout(
                title='Daily SPX Movement',
                yaxis=dict(
                        range=[min(spx_daily.min()) - 20,max(spx_daily.max()) + 20],
                        title='',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        ),
                yaxis2=dict(
                        range=[min(spx_daily.min()) - 20,max(spx_daily.max()) + 20],
                        title='',
                        overlaying='y',
                        #anchor='free',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        ),
                yaxis3=dict(
                        range=[min(spx_daily.min()) - 20,max(spx_daily.max()) + 20],
                        title='',
                        overlaying='y',
                        side='left',
                        showgrid=False,
                        zeroline=False,
                        showline=False,
                        ticks='',
                        showticklabels=False
                        )
                )
            
        data = [trace1, trace2, trace3]
        figure = dict(data=data, layout=layout)
        return figure


if __name__ == '__main__':
    app.server.run(port=1000, debug=True, threaded=True, use_reloader=False)