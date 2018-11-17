import  pdb

from threading import Thread
import config as CONFIG
import DataFetcher
from Authentication import  Authenticate
import ExchangeInterface
from OrderMonitoring import Orderm
from MovingAverage_Mgr import MA_Mgmt, Trade_finder
import superTrend
from superTrend import SuperTrend
import save2file
import datetime, time
import Controller
from simulator import simulator
from Controller import Controller
import requests
import json

OPEN_ORDERS={}
TRIGGER_PENDING={}

def placebracketorder(trading_quantity, trading_transaction_type, trading_price, trading_squareoff, trading_stoploss ,
                      trading_trailing_stoploss=None):
    url = "https://api.kite.trade/orders/" + CONFIG.order_variety
    var = "token " + CONFIG.API_KEY + ":" + CONFIG.ACCESS_TOKEN
    if trading_trailing_stoploss != None:
        payload = {
            "tradingsymbol": CONFIG.trading_symbol,
            "exchange": CONFIG.trading_exchange,
            "transaction_type": trading_transaction_type,
            "order_type": CONFIG.trading_order_type,
            "quantity": trading_quantity,
            "product": CONFIG.trading_product,
            "price": trading_price,
            "squareoff": trading_squareoff,
            "stoploss": trading_stoploss,
            "trailing_stoploss": trading_trailing_stoploss,
            "trigger_price":trading_price - 11
        }
    else:
        payload = {
            "tradingsymbol": CONFIG.trading_symbol,
            "exchange": CONFIG.trading_exchange,
            "transaction_type": trading_transaction_type,
            "order_type": CONFIG.trading_order_type,
            "quantity": trading_quantity,
            "product": CONFIG.trading_product,
            "price": trading_price,
            "squareoff": trading_squareoff,
            "stoploss": trading_stoploss,
            "trigger_price": trading_price - 11
        }
    headers = {
        "X-Kite-Version": '3',
        "Authorization": var
    }

    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
    response = requests.post(url, data=payload, headers=headers)
    json_order_place_response = json.loads(response.text)

    print(str(json_order_place_response))

    print("Bracket Order Placed successfully TYPE=%s, price=%s with squareoff=%s stoploss=%s and order_id=%s",
                trading_transaction_type,
                trading_price, trading_squareoff, trading_stoploss, json_order_place_response['data']['order_id'])
    OPEN_ORDERS[json_order_place_response['data']['order_id']] = json_order_place_response


def monitor():

    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
    url = 'https://api.kite.trade/orders/'
    var = "token " + CONFIG.API_KEY +":"+CONFIG.ACCESS_TOKEN
    headers = {
        "X-Kite-Version": '3',
        "Authorization": var
    }
    try:
        response = requests.get(url, headers=headers)
        json_data = json.loads(response.text)

    except Exception as e:
        print("Exception occured while retrieving the orders. Please check the connectivity..")
        print("Exception at ordermonitoring:", str(e))
        return

    cur_time = datetime.datetime.now().replace(microsecond=0)
    cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
    cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
    cur_idx = str(cur_mkt_idx)

    #open_orders=['180724000616333', '180724000146554', '180724000398250', '180724000421185', '180724000658724']
    parent_orders = []
    order_ids = []
    orders = {}

    if (json_data['status'] == 'success'):
        for x in json_data['data']:
            if (x['status'] == "COMPLETE"):
                #print("Order was successful id:", x['order_id'])
                #print(x)
                key = x['order_id']
                orders[key] = x
            print("Order was successful id:", x['order_id'])
            print(x)
            if (x['status'] == "TRIGGER PENDING" and x['order_id']!='180724000680794'):
                TRIGGER_PENDING[x['order_id']] = x



        '''
        for x in json_data['data']:
            """Since we are verifying for pending orders, we are looking for targets acheived or not"""
            if x['parent_order_id'] in orders.keys() and (x['status'] == 'TRIGGER PENDING'):
                print("parent order found. parent_order_id:", x['parent_order_id'])
                if x['transaction_type'] == "BUY":
                    if CONFIG.MARKET_LTP <= (orders[x['parent_order_id']]['price'] - CONFIG.NIFTY_EXPECTED_ATR):
                        print("Target acheived for parent_order_id:",x['parent_order_id'] )
                        print("Exit the current BUY order_id:", x['order_id'])

                elif x['transaction_type'] == "SELL":
                    if CONFIG.MARKET_LTP >= (orders[x['parent_order_id']]['price'] + CONFIG.NIFTY_EXPECTED_ATR):
                        print("Target acheived for parent_order_id:",x['parent_order_id'] )
                        print("Exit the current SELL order_id:", x['order_id'])

                else:
                    print("No targets are acheived...")
        '''


    return

import datetime
from datetime import  time
import threading
from threading import Timer


def exit_the_order(order_id, parent_order_id, instrument_token):
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
    url = "https://api.kite.trade/orders/co/" + order_id
    var = "token " + CONFIG.API_KEY + ":" + CONFIG.ACCESS_TOKEN
    headers = {
        "X-Kite-Version": '3',
        "Authorization": var
    }

    try:
        if str(CONFIG.TRADE_INSTRUMENT) != str(instrument_token):
            print("Nothing to delete for instrument token:", instrument_token)
            return

        response = requests.delete(url, headers=headers)
        json_data = json.loads(response.text)
        if json_data['status'] == 'success':
            print("Order id:", order_id, " exited successfully.")
            print(json_data)
        else:
            print(" Exiting the order:", order_id, " Failed...")
            print(json_data)

    except Exception as e:
        print("Exception occured while retrieving the orders. Please check the connectivity..")
        print("Exception at ordermonitoring:", str(e))
        return

    return


if __name__ == '__main__':
    CONFIG.init()

    auth = Authenticate()
    auth.login()

    ExchangeInterface.main()
    CONFIG.trading_exchange = "MCX"
    #ExchangeInterface.placebracketorder(1, "BUY", 4090, 6, 3)
    placebracketorder(1, "BUY", 4742, 6, 3)
    """
    Trading price has to be current market ltp. The order type, if given MARKET then it will be traded in LTP.
    """

    import time
    time.sleep(60)
    monitor()
    for key in TRIGGER_PENDING.keys():
        x = TRIGGER_PENDING.get(key)
        time.sleep(2)
        exit_the_order(x['order_id'], x['parent_order_id'], 53768199)
    """
    while True:
        Timer(CONFIG.TIMER_STD_VAL, monitor, []).run()
    """


