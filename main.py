from boto3 import resource
from dataclasses import dataclass
from decimal import Decimal
from dotenv import load_dotenv
from os import getenv
from s3_helper import CSVStream
from typing import Any

# Importing modules
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import math
import matplotlib.pyplot as plt
import sys

# set initial parameters
N = 18 # batch size used to build model
M = 600 # timespan we use to train the model
score_threshold = 0.7 # rsrs threshold
# Parameters used to calculate moving average
mean_day = 20 # timespan to calculate the most recent close price
mean_diff_day = 3 # time difference to calculate the before close price

def get_ols(x, y):
    slope, intercept = np.polyfit(x, y, 1)
    r2 = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
    return (intercept, slope, r2)

def initial_slope_series(start_time):
    data = df.iloc[start_time-(N + M):start_time]
    return [get_ols(data.min_price[i:i+N], data.max_price[i:i+N])[1] for i in range(M)]

def get_zscore(slope_series):
    mean = np.mean(slope_series)
    std = np.std(slope_series)
    return (slope_series[-1] - mean) / std

# Main algorithm
def get_timing_signal(start_time):
    start_time = np.where(df.index==start_time)[0][0]
    # calculate MA signal
    close_data = df.iloc[start_time-(mean_day + mean_diff_day):start_time]
    # 23 days，take the last 20 days
    today_MA = close_data.mean_price[mean_diff_day:].mean() 
    # 23 days，take the first 20 days
    before_MA = close_data.mean_price[:-mean_diff_day].mean()
    # calculate rsrs signal
    high_low_data = pd.DataFrame({'low':df.iloc[start_time-N:start_time].min_price.to_list(),
                                  'high':df.iloc[start_time-N:start_time].max_price.to_list()})
    intercept, slope, r2 = get_ols(high_low_data.low, high_low_data.high)
    slope_series.append(slope)

    rsrs_score = get_zscore(slope_series[-M:]) * r2
    # use signal combo to get final signal
    if rsrs_score > score_threshold and today_MA > before_MA:
        return "BUY"
    elif rsrs_score < -score_threshold and today_MA < before_MA:
        return "SELL"
    else:
        return "NEUTRAL"

def to_iloc(start_time):
    return np.where(df.index==start_time)[0][0]

load_dotenv()

BUY = "buy"
SELL = "sell"

BUCKET = getenv("BUCKET_NAME")

XBT_2018_KEY = "xbt.usd.2018"
XBT_2020_KEY = "xbt.usd.2020"

ETH_2018_KEY = "eth.usd.2018"
ETH_2020_KEY = "eth.usd.2020"

S3 = resource("s3")
SELECT_ALL_QUERY = 'SELECT * FROM S3Object'

# Example s3 SELECT Query to Filter Data Stream
#
# The where clause fields refer to the timestamp column in the csv row.
# To filter the month of February, for example, (start: 1517443200, end: 1519862400) 2018
#                                               (Feb-01 00:00:00  , Mar-01 00:00:00) 2018
#
# QUERY = '''\
#     SELECT *
#     FROM S3Object s
#     WHERE CAST(s._4 AS DECIMAL) >= 1514764800
#       AND CAST(s._4 AS DECIMAL) < 1514764802
# '''

STREAM = CSVStream(
    'select',
    S3.meta.client,
    key=XBT_2018_KEY,
    bucket=BUCKET,
    expression=SELECT_ALL_QUERY,
)


context = pd.DataFrame(columns = ['pair','price','amount','time_stamp'])


counter = 0
@dataclass
class Trade:
    trade_type: str # BUY | SELL
    base: str
    volume: Decimal

def algorithm(csv_row: str, context: dict[str, Any],):
    csv_row =  pd.DataFrame(csv_row.split(',')).T
    csv_row.rename(columns={0:'pair',1:'price',2:'amount',3:'time_stamp'},inplace = True)
    context = pd.concat([context,csv_row], ignore_index=True,axis = 0)
    
    if counter < 1000000:
        counter += 1
        yield None
    if counter == 1000000:
        df = context[-1000000:].deepcopy()
        # Data preprocessing
        df['time_stamp'] = df['time_stamp'].apply(datetime.fromtimestamp)
        df.sort_values(by=['time_stamp'],inplace=True)
        history_price = pd.DataFrame(df.groupby(['time_stamp']).price.mean())
        
        # agg the data into 1 mins
        time_span = '1T'
        start = df['time_stamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        end = df['time_stamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
        index = pd.date_range(start, end, freq=time_span)
        # get the features we need
        get_price = pd.DataFrame(index = index)
        get_price['mean_price'] = history_price.resample(time_span).mean()
        get_price['min_price'] = history_price.resample(time_span).min()
        get_price['max_price'] = history_price.resample(time_span).max()
        
        # build the model
        df = get_price
        error_value = np.unique(np.where(np.isfinite(df)==0)[0]).tolist()
        df.drop(df.iloc[error_value].index, axis = 0, inplace = True)
        start_time = str(df[-1].index)
        slope_series = initial_slope_series(to_iloc(start_time))[:-1]

        counter += 1
        response = yield get_timing_signal(start_time)
    if counter > 1000000:
        counter = 0
        yield None

    """ Trading Algorithm

    Add your logic to this function. This function will simulate a streaming
    interface with exchange trade data. This function will be called for each
    data row received from the stream.

    The context object will persist between iterations of your algorithm.

    Args:
        csv_row (str): one exchange trade (format: "exchange pair", "price", "volume", "timestamp")
        context (dict[str, Any]): a context that will survive each iteration of the algorithm

    Generator:
        response (dict): "Fill"-type object with information for the current and unfilled trades
    
    Yield (None | Trade | [Trade]): a trade order/s; None indicates no trade action
    """
    # algorithm logic...

    # response = yield None # example: Trade(BUY, 'xbt', Decimal(1))

    # algorithm clean-up/error handling...

if __name__ == '__main__':
    # example to stream data
    for row in STREAM.iter_records():
        print(row)

# Example Interaction
#
# Given the following incoming trades, each line represents one csv row:
#   (1) okfq-xbt-usd,14682.26,2,1514765115
#   (2) okf1-xbt-usd,13793.65,2,1514765115
#   (3) stmp-xbt-usd,13789.01,0.00152381,1514765115
#
# When you receive trade 1 through to your algorithm, if you decide to make
# a BUY trade for 3 xbt, the order will start to fill in the following steps
#   [1] 1 unit xbt from trade 1 (%50 available volume from the trade data)
#   [2] 1 unit xbt from trade 2
#   [3] receiving trade 3, you decide to put in another BUY trade:
#       i. Trade will be rejected, because we have not finished filling your 
#          previous trade
#       ii. The fill object will contain additional fields with error data
#           a. "error_code", which will be "rejected"; and
#           b. "error_msg", description why the trade was rejected.
#
# Responses during these iterations:
#   [1] success resulting in:
#       {
#           "price": 14682.26,
#           "volume": 1,
#           "unfilled": {"xbt": 2, "eth": 0 }
#       }
#   [2]
#       {
#           "price": 13793.65,
#           "volume": 1,
#           "unfilled": {"xbt": 1, "eth": 0 }
#       }
#   [3]
#       {
#           "price": 13789.01,
#           "volume": 0.000761905,
#           "error_code": "rejected",
#           "error_msg": "filling trade in progress",
#           "unfilled": {"xbt": 0.999238095, "eth": 0 }
#       }
#
# In step 3, the new trade order that you submitted is rejected; however,
# we will continue to fill that order that was already in progress, so
# the price and volume are CONFIRMED in that payload.
