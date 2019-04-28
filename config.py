'''
Created on 09-Aug-2017

@author: suhaheer
'''


import json
import csv
import pandas as pd
import threading


#######################################################################################################################
"""
Standard Constants
"""
#STD_PATH = "/home/ubuntu/PycharmProjects/sources/"
STD_PATH = ""
SYSTEM_CLOSE_HR = 23
SYSTEM_CLOSE_MIN = 45
SYSTEM_STARTED_TIME = None
GlobalInstObj = None
TIMER_STD_VAL = 1 #1 second default value
OPEN_COVER_ORDERS = {}
OPEN = 0
HIGH = 1
LOW = 2
CLOSE = 3
MUTEX = threading.Lock()

#######################################################################################################################



credentials_dict = json.load(open(STD_PATH+"configfiles/credentials_upstox.txt"))

#######################################################################################################################
"""
TRADE_INSTRUMENT = ["RELIANCE", "RELINFRA", "INFY", "ICICIBANK", "AXISBANK", "AUROPHARMA", "BANDHANBNK", "PEL", "SUNTV",
                    "MARUTI", "INDUSINDBK", "HEROMOTOCO", 'LUPIN', "ULTRACEMCO", "BAJAJFINSV","ABB", "BRITANNIA", "OFSS",
                    "HDFC", "ASIANPAINT", "BAJAJ-AUTO", "IBULHSGFIN", "LT", "UPL", "KOTAKBANK","COLPAL","DABUR","MARICO",
                    "HDFCBANK", "HINDUNILVR", "TITAN", "GRASIM", "HCLTECH", "TCS", "BAJFINANCE","PIDILITIND",
                    "CIPLA", "DRREDDY", "TECHM", "BATAINDIA", "MINDTREE", "ZEEL", "TATASTEEL", "SIEMENS", "INDIGO",
                    "GODREJCP", "ICICIGI", "SBILIFE", "BIOCON", "ACC", "CONCOR", "HAVELLS", "SRTRANSFIN", "LICHSGFIN",
                    "CANBK", "AJANTPHARM", "DIVISLAB", "RBLBANK", "GODREJIND", "TATACHEM", "BHARATFORG", "UBL", "CENTURYTEX",
                    "VOLTAS", "TVSMOTOR", "JUBLFOOD", "AMARAJABAT", "SRF", "TORNTPHARM", "MUTHOOTFIN", "RAMCOCEM",
                    "APOLLOHOSP", "CUMMINSIND", "GLENMARK", "CHOLAFIN", "BHARATFIN", "NIITTECH", "TATAELXSI", "PVR", "APLAPOLLO",
                    "SUNPHARMA", "SOBHA", "GODREJPROP", "BHARTIARTL", "MPHASIS", "WHIRLPOOL", "CRISIL", "GLAXO", "COLPAL", "PFIZER",
                    "GSKCONS", "KANSAINER", "SANOFI", "HEXAWARE", "SBIN", "YESBANK", "DHFL", "MGL", "GAIL", "ADANIPORTS", "ESCORTS",
                    "HEG", "GNFC", "GRUH", "BBTC", "LTI", "CYIENT", "NAUKRI", "GRAPHITE", "IBVENTURES", "DBL", "VIPIND",
                    "RAYMOND", "CESC", "WOCKPHARMA", "CAPF", "MFSL", "GODFRYPHLP", "JUSTDIAL", "BEML"]
"""
TRADE_INSTRUMENT = ["RELIANCE", "INFY", "TCS"]#["RELIANCE", "INFY", "TCS","AXISBANK","BAJFINANCE","HDFCBANK","HDFC","ASIANPAINT", "ACC","MARUTI"]
TRADE_INDICES = ["NIFTY_50"] #[ "NIFTY_IT", "NIFTY_BANK"]


TRADE_INSTRUMENT_MCX_FO = []

#For testing...
#TRADE_INSTRUMENT = TRADE_INSTRUMENT_MCX_FO
#######################################################################################################################


#######################################################################################################################
"""
Exchange timings
"""
OPEN_HR_COMMODITY = 10
OPEN_MIN_COMMODITY = 1

CLOSE_HR_COMMODITY = 23
CLOSE_MIN_COMMODITY = 30

OPEN_HR = 0
OPEN_MIN = 15

CLOSE_HR = 15
CLOSE_MIN = 30
#######################################################################################################################



#######################################################################################################################
'''
Multistock constants
'''
MULTISTOCK = {}
for stock in (TRADE_INSTRUMENT + TRADE_INDICES + TRADE_INSTRUMENT_MCX_FO):
    MULTISTOCK[stock]={}
    MULTISTOCK[stock]['CMP'] = None
    MULTISTOCK[stock]['LTP'] = {}

    MULTISTOCK[stock]['5MIN'] = {}
    MULTISTOCK[stock]['5MIN']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['5MIN']['10SMA'] = None
    MULTISTOCK[stock]['5MIN']['50SMA'] = None
    MULTISTOCK[stock]['5MIN']['100SMA'] = None
    MULTISTOCK[stock]['5MIN']['150SMA'] = None
    MULTISTOCK[stock]['5MIN']['200SMA'] = None
    MULTISTOCK[stock]['5MIN']['400SMA'] = None
    MULTISTOCK[stock]['5MIN']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['5MIN']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['5MIN']['SUPERTREND'] = None
    MULTISTOCK[stock]['5MIN']['MACD'] = { 'macd':[],
                                          'macdSig':[],
                                          'macdHist':[]}

    MULTISTOCK[stock]['10MIN'] = {}
    MULTISTOCK[stock]['10MIN']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['10MIN']['10SMA'] = None
    MULTISTOCK[stock]['10MIN']['50SMA'] = None
    MULTISTOCK[stock]['10MIN']['100SMA'] = None
    MULTISTOCK[stock]['10MIN']['150SMA'] = None
    MULTISTOCK[stock]['10MIN']['200SMA'] = None
    MULTISTOCK[stock]['10MIN']['400SMA'] = None
    MULTISTOCK[stock]['10MIN']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['10MIN']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['10MIN']['SUPERTREND'] = None
    MULTISTOCK[stock]['10MIN']['MACD'] = { 'macd': [],
                                           'macdSig': [],
                                           'macdHist': []}

    MULTISTOCK[stock]['15MIN'] = {}
    MULTISTOCK[stock]['15MIN']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['15MIN']['10SMA'] = None
    MULTISTOCK[stock]['15MIN']['50SMA'] = None
    MULTISTOCK[stock]['15MIN']['100SMA'] = None
    MULTISTOCK[stock]['15MIN']['150SMA'] = None
    MULTISTOCK[stock]['15MIN']['200SMA'] = None
    MULTISTOCK[stock]['15MIN']['400SMA'] = None
    MULTISTOCK[stock]['15MIN']['BB_50'] = None
    MULTISTOCK[stock]['15MIN']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['15MIN']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['15MIN']['SUPERTREND'] = None
    MULTISTOCK[stock]['15MIN']['MACD'] = { 'macd': [],
                                           'macdSig': [],
                                           'macdHist': []}

    MULTISTOCK[stock]['30MIN'] = {}
    MULTISTOCK[stock]['30MIN']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['30MIN']['10SMA'] = None
    MULTISTOCK[stock]['30MIN']['50SMA'] = None
    MULTISTOCK[stock]['30MIN']['100SMA'] = None
    MULTISTOCK[stock]['30MIN']['150SMA'] = None
    MULTISTOCK[stock]['30MIN']['200SMA'] = None
    MULTISTOCK[stock]['30MIN']['400SMA'] = None
    MULTISTOCK[stock]['30MIN']['BB_50'] = None
    MULTISTOCK[stock]['30MIN']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['30MIN']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['30MIN']['SUPERTREND'] = None
    MULTISTOCK[stock]['30MIN']['MACD'] = {'macd': [],
                                          'macdSig': [],
                                          'macdHist': []}

    MULTISTOCK[stock]['1HOUR'] = {}
    MULTISTOCK[stock]['1HOUR']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['1HOUR']['10SMA'] = None
    MULTISTOCK[stock]['1HOUR']['50SMA'] = None
    MULTISTOCK[stock]['1HOUR']['100SMA'] = None
    MULTISTOCK[stock]['1HOUR']['150SMA'] = None
    MULTISTOCK[stock]['1HOUR']['200SMA'] = None
    MULTISTOCK[stock]['1HOUR']['400SMA'] = None
    MULTISTOCK[stock]['1HOUR']['BB_50'] = None
    MULTISTOCK[stock]['1HOUR']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['1HOUR']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['1HOUR']['SUPERTREND'] = None
    MULTISTOCK[stock]['1HOUR']['MACD'] = { 'macd': [],
                                           'macdSig': [],
                                           'macdHist': []}

    MULTISTOCK[stock]['1DAY'] = {}
    MULTISTOCK[stock]['1DAY']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['1DAY']['10SMA'] = None
    MULTISTOCK[stock]['1DAY']['50SMA'] = None
    MULTISTOCK[stock]['1DAY']['100SMA'] = None
    MULTISTOCK[stock]['1DAY']['150SMA'] = None
    MULTISTOCK[stock]['1DAY']['200SMA'] = None
    MULTISTOCK[stock]['1DAY']['400SMA'] = None
    MULTISTOCK[stock]['1DAY']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['1DAY']['TRUE_RANGE'] = None
    MULTISTOCK[stock]['1DAY']['SUPERTREND'] = None
    MULTISTOCK[stock]['1DAY']['MACD'] = { 'macd': [],
                                          'macdSig': [],
                                          'macdHist': []}

    MULTISTOCK[stock]['1WEEK'] = {}
    MULTISTOCK[stock]['1WEEK']['TICKS'] = pd.DataFrame()
    MULTISTOCK[stock]['1WEEK']['10SMA'] = None
    MULTISTOCK[stock]['1WEEK']['50SMA'] = None
    MULTISTOCK[stock]['1WEEK']['100SMA'] = None
    MULTISTOCK[stock]['1WEEK']['150SMA'] = None
    MULTISTOCK[stock]['1WEEK']['200SMA'] = None
    MULTISTOCK[stock]['1WEEK']['400SMA'] = None
    MULTISTOCK[stock]['1WEEK']['AVG_TRUE_RANGE'] = None
    MULTISTOCK[stock]['1WEEK']['TRUE_RANGE'] =  None
    MULTISTOCK[stock]['1WEEK']['SUPERTREND'] = None
    MULTISTOCK[stock]['1WEEK']['MACD'] = { 'macd': [],
                                           'macdSig': [],
                                           'macdHist': []}

    MULTISTOCK[stock]['1MONTH'] = {}
    MULTISTOCK[stock]['1MONTH']['TICKS'] = pd.DataFrame()

    MULTISTOCK[stock]['DataFrame'] = pd.DataFrame(dtype=(float), index=['5Min', '10Min', '15Min', '30Min', '1H', '1D','1W'],
                                                  columns=['10SMA', '50SMAL','50SMA','50SMAH', '100SMA', '150SMA', '200SMA', '400SMA',
                                                           'BB_50_UP', 'BB_50_DOWN'])

    MULTISTOCK[stock]['Option_chain'] = pd.DataFrame(dtype=(float), index=range(0,3),
                                                                    columns=['Call-OI','Call-chngInOi','Call-Vol',
                                                                             'Strike','Put-Vol','Put-chngInOi',
                                                                             'Put-OI','Direction'])
    MULTISTOCK[stock]['Fibonacci_level'] = pd.DataFrame(dtype=(float))
#######################################################################################################################


#######################################################################################################################
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
#######################################################################################################################




#######################################################################################################################
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
    global KITE, MARKET_LTP, CRUDE_PIP, NIFTY_PIP, PIP, NIFTY_PROFIT_MARGIN, NIFTY_EXPECTED_ATR, COMMODITY_PROFIT_MARGIN
    global save2file_flag
    global ohlc_data, plotted


    save2file_flag = True

    PIP = None

    tick = {}
    tick_secondary = {}
    ORDER_MANAGER= {}
    OrderCancelDeadline = 5
    time_interval = 1  #default time interval=15min
    time_interval_secondary = 60


    MARKET_LTP = None
    KITE = None

    '''
    Authentication constant Variables
    '''
    global  CLIENT_ID , API_KEY , API_SECRET , REQUEST_TOKEN , ACCESS_TOKEN , PUBLIC_TOKEN , REFRESH_TOKEN
    global UPSTOX_SESSION

    CLIENT_ID = credentials_dict['login_cred']['username']
    API_KEY = credentials_dict['API_KEY']
    API_SECRET = credentials_dict['API_SECRET']

    REQUEST_TOKEN = "Qqghad4b7gXK0szsN15y7QrCXN8l5ayH"
    ACCESS_TOKEN = "P8TCFKf04P9U8YFanEQa3DzFulVEu2AN"
    PUBLIC_TOKEN = "c120f2f8edd54f01caa65c8b54b77e7b"
    REFRESH_TOKEN = "sample"
    UPSTOX_SESSION = None

    ''''
    Parameters required for placing order
    '''

    global trading_symbol, trading_exchange , trading_order_type, trading_product , order_variety , trading_quantity

    #Possible values for trading symbol are symbols for the scripts you want to buy or sell . You can get it by retrieving instruments
    trading_symbol = "CRUDEOILM18AUGFUT" #"NIFTY18JULFUT" #"NIFTY50"#"CRUDEOIL18APRFUT"

    #Possible values for trading_exchange are "NSE", "BSE", "NFO",  "CDS",  "MCX"
    trading_exchange = "MCX"

    #possible values for trading_order_type are "LIMIT" , "MARKET" and "SL"
    trading_order_type = "LIMIT"

    #possible values for trading_product are MIS , CNC and NRML
    trading_product = "NRML"

    #possible values for order_variety are "regular", "amo",  "bo",  "co"
    order_variety = "BO"

    #number of shares to buy trading_quantity
    trading_quantity = 1
#######################################################################################################################

