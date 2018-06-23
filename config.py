'''
Created on 09-Aug-2017

@author: suhaheer
'''


import json


"""
Standard Constants
"""
#STD_PATH = "/home/ubuntu/PycharmProjects/sources/"
STD_PATH = ""
SYSTEM_CLOSE_HR = 23
SYSTEM_CLOSE_MIN = 45
GlobalInstObj = None

"""
Multistock constants
"""
MULTISTOCK = {}

"""
Simulation Constants
"""
SIMULATION_MODE = False
OPEN_ORDERS = {}

'''
NIFTY, NSE, NFO constants
'''
OPEN_HR = 9
OPEN_MIN = 15

CLOSE_HR = 15
CLOSE_MIN = 30

'''
Check to execute Simple Moving average only once
'''
SMA_7PERIOD=False
SMA_54PERIOD=False
SMA_200PERIOD=False

SMA_1PERIOD = 1
SMA_14PERIOD = 14
SMA_5PERIOD = 5
SMA_34PERIOD = 34

UPTREND = "UPTREND"
DOWNTREND = "DOWNTREND"

EMA_7PERIOD_DATA=[]
EMA_54PERIOD_DATA=[]
EMA_200PERIOD_DATA=[]

'''
SUPERTREND CONSTANTS
'''
SUPER_MULTIPLIER=3
SUPER_PERIOD=7
TRUE_RANGE = {}
AVG_TRUE_RANGE = {}
SUPERTREND = {}
SUPERTREND_TREND = {}
#these values are used to plot the graph
SUPERTREND_STARTING_TIME=None
SUPERTREND_COUNT = 1

'''
Linear Regression constants.
'''
LINEAR_REGRESSION = {}
LINEAR_REGRESSION_PREDICTOR = {}

'''
RSI Constants
'''
RSI = {}
RSI_PERIOD = 5
RSI_OVERBOUGHT = 95.0
RSI_OVERSOLD = 10.0
'''
Number of lots for trading
'''
BUY_LOT_QUANTITY=0
SELL_LOT_QUANTITY=0
TRADE_COUNT=0
BUY_PRICE = {}
SELL_PRICE = {}

'''
Trading Instrument
'''
TRADE_INSTRUMENT = 12374274 #53703431 #256265 #53556743

def PRICE_FORMAT(price):
    price = float(price)
    number, decimal = str(price).split(".")

    temp = price - float(number)
    temp = (float(format(temp, '.2f')))

    decimal = int(temp*100)
    if decimal<5 and decimal>0:
        decimal = 5
    formatted_price = int(decimal) - (int(decimal)%5)
    final_price = float(number)+(float(formatted_price)/100.0)

    return (float(format(final_price, '.2f')))

def init():
    """
    tick - contains OHLC calculated based on the time interval
    tick_secondary - contains OHLC calculated based on the secondary time interval
    ORDER_MANAGER - Details of all the orders placed
    OrderCancelDeadline - Time to wait before cancelling the order
    time_interval - Time interval for collecting ticks required for calculating OHLC
    time_interval_secondary = Time interval for collecting ticks required for calculating secondary candle OHLC
    """
    global tick , ORDER_MANAGER , OrderCancelDeadline , time_interval, tick_secondary , time_interval_secondary
    global KITE, MARKET_LTP, CRUDE_PIP, NIFTY_PIP, PIP, NIFTY_PROFIT_MARGIN, NIFTY_EXPECTED_ATR
    global save2file_flag
    global ohlc_data, plotted
    global SMA_5PERIOD_VALUE, SMA_34PERIOD_VALUE, SMA_1PERIOD_VALUE, SMA_14PERIOD_VALUE
    credentials_dict = json.load(open(STD_PATH+"configfiles/credentials.txt"))

    save2file_flag = True

    SMA_5PERIOD_VALUE = None
    SMA_34PERIOD_VALUE = None
    SMA_1PERIOD_VALUE = None
    SMA_14PERIOD_VALUE = None
    CRUDE_PIP = 1.0
    NIFTY_PIP = 3.0
    NIFTY_PROFIT_MARGIN = 8.0
    NIFTY_EXPECTED_ATR = 5.0
    PIP = None


    tick = {}
    tick_secondary = {}
    ORDER_MANAGER= {}
    OrderCancelDeadline = 5
    time_interval = 5
    time_interval_secondary = 60


    MARKET_LTP = None
    KITE = None

    '''
    sample variable for testing candlestick chart
    '''
    ohlc_data = []
    plotted = False

    '''
    Authentication constant Variables
    '''
    global  CLIENT_ID , API_KEY , API_SECRET , REQUEST_TOKEN , ACCESS_TOKEN , PUBLIC_TOKEN , REFRESH_TOKEN

    CLIENT_ID = credentials_dict['login_cred']['username']
    API_KEY = credentials_dict['API_KEY']
    API_SECRET = credentials_dict['API_SECRET']

    REQUEST_TOKEN = "Qqghad4b7gXK0szsN15y7QrCXN8l5ayH"
    ACCESS_TOKEN = "P8TCFKf04P9U8YFanEQa3DzFulVEu2AN"
    PUBLIC_TOKEN = "c120f2f8edd54f01caa65c8b54b77e7b"
    REFRESH_TOKEN = "sample"

    ''''
    Parameters required for placing order
    '''

    global trading_symbol, trading_exchange , trading_order_type, trading_product , order_variety , trading_quantity

    #Possible values for trading symbol are symbols for the scripts you want to buy or sell . You can get it by retrieving instruments
    trading_symbol = "NIFTY18JUNFUT" #"NIFTY50"#"CRUDEOIL18APRFUT"

    #Possible values for trading_exchange are "NSE", "BSE", "NFO",  "CDS",  "MCX"
    trading_exchange = "NFO"

    #possible values for trading_order_type are "LIMIT" , "MARKET" and "SL"
    trading_order_type = "LIMIT"

    #possible values for trading_product are MIS , CNC and NRML
    trading_product = "NRML"

    #possible values for order_variety are "regular", "amo",  "bo",  "co"
    order_variety = "BO"

    #number of shares to buy trading_quantity
    trading_quantity = 75
