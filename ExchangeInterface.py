'''
Created on 30-Dec-2017

@author: kailah rudra
'''

import config
import datetime
import requests
import json
import logging
import time

global LOG_EI

def main():
    global LOG_EI
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=config.STD_PATH + 'logs/ExchangeInterface-' + timestr + '.log', mode='w')
    if config.SIMULATION_MODE:
        handler = logging.FileHandler(filename=config.STD_PATH + 'logs/Simulation/ExchangeInterface-' + timestr + '.log', mode='w')

    handler.setFormatter(formatter)
    LOG_EI = logging.getLogger("ExchangeInterface")
    LOG_EI.setLevel(logging.INFO)
    LOG_EI.addHandler(handler)

def placebracketorder(trading_quantity, trading_transaction_type, trading_price, trading_squareoff, trading_stoploss ,
                      trading_trailing_stoploss=None):
    global LOG_EI
    url = "https://api.kite.trade/orders/" + config.order_variety
    var = "token " + config.API_KEY + ":" + config.ACCESS_TOKEN
    if trading_trailing_stoploss !=None:
        payload = {
            "tradingsymbol": config.trading_symbol,
            "exchange": config.trading_exchange,
            "transaction_type": trading_transaction_type,
            "order_type": config.trading_order_type,
            "quantity": trading_quantity,
            "product": config.trading_product,
            "price": trading_price,
            "squareoff" : trading_squareoff,
            "stoploss" : trading_stoploss,
            "trailing_stoploss":trading_trailing_stoploss
        }
    else:
        payload = {
            "tradingsymbol": config.trading_symbol,
            "exchange": config.trading_exchange,
            "transaction_type": trading_transaction_type,
            "order_type": config.trading_order_type,
            "quantity": trading_quantity,
            "product": config.trading_product,
            "price": trading_price,
            "squareoff": trading_squareoff,
            "stoploss": trading_stoploss
        }
    headers = {
        "X-Kite-Version": '3',
        "Authorization": var
    }
    if (config.SIMULATION_MODE):
        cur_time = datetime.datetime.now()
        oid = int(cur_time.strftime("%s"))
        LOG_EI.info("Bracket Order Placed successfully TYPE=%s, price=%s with squareoff=%s stoploss=%s and order_id=%s",
                    trading_transaction_type,
                    trading_price, trading_squareoff, trading_stoploss, oid)
        cur_min = cur_time.minute - (cur_time.minute % config.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)
        payload["order_id"] = oid
        config.OPEN_ORDERS[cur_idx] = payload
        return

    cur_time = datetime.datetime.now()
    no_trading_time = datetime.datetime.now().replace(hour=config.CLOSE_HR, minute=0, second=0, microsecond=0)
    if ( cur_time >= no_trading_time):
        LOG_EI.error("Order rejected due to near market closing time.")
        return

    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
    response = requests.post(url, data=payload, headers=headers)
    json_order_place_response = json.loads(response.text)
    LOG_EI.info("Bracket Order Placed successfully TYPE=%s, price=%s with squareoff=%s stoploss=%s and order_id=%s", trading_transaction_type,
             trading_price, trading_squareoff, trading_stoploss, json_order_place_response['data']['order_id'])




def placeorder(trading_quantity,  trading_transaction_type,  trading_price, parentorder, target_price, ref=None):
    LOG = None
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=config.STD_PATH+'logs/ExchangeInterface-'+timestr+'.log',mode='w')
    handler.setFormatter(formatter)
    LOG = logging.getLogger("ExchangeInterface")
    LOG.setLevel(logging.INFO)
    LOG.addHandler(handler)
    url = "https://api.kite.trade/orders/" + config.order_variety
    var = "token " + config.API_KEY + ":" + config.ACCESS_TOKEN
    payload = {
        "tradingsymbol": config.trading_symbol,
        "exchange": config.trading_exchange,
        "transaction_type": trading_transaction_type,
        "order_type": config.trading_order_type,
        "quantity": trading_quantity,
        "product": config.trading_product,
        "price": trading_price
    }

    headers = {
        "X-Kite-Version": '3',
        "Authorization": var
    }
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
    response = requests.post(url, data=payload, headers=headers)
    json_order_place_response = json.loads(response.text)

    if(json_order_place_response['status'] == "success"):
        # if it is a primary order
        if (parentorder == "YES"):
            # if the primary order is buy
            if (trading_transaction_type == "BUY"):
                timestamp = datetime.datetime.now();
                temp_order = {str(json_order_place_response['data']['order_id']): {"order_id": str(json_order_place_response['data']['order_id']),
                                                                   "timestamp": timestamp, "status": "placed",
                                                                   "SYMBOL": config.trading_symbol,
                                                                   "QUANTITY": trading_quantity,
                                                                   "EXCHANGE": config.trading_exchange,
                                                                   "TRADE_TYPE": trading_transaction_type,
                                                                   "EXEC_PRICE": trading_price,
                                                                   "ORDER_TYPE": config.trading_order_type,
                                                                   "PRODUCT": config.trading_product,
                                                                   "TARGET_TYPE": "SELL",
                                                                   "TARGET_PRICE": target_price, "PARENTORDER": "YES",
                                                                   "REFERENCE": None}}
                config.ORDER_MANAGER.update(temp_order)
            # if the primary order is sell
            elif (trading_transaction_type == "SELL"):
                timestamp = datetime.datetime.now();
                temp_order = {str(json_order_place_response['data']['order_id']): {"order_id": str(json_order_place_response['data']['order_id']),
                                                                   "timestamp": timestamp, "status": "placed",
                                                                   "SYMBOL": config.trading_symbol,
                                                                   "QUANTITY": trading_quantity,
                                                                   "EXCHANGE": config.trading_exchange,
                                                                   "TRADE_TYPE": trading_transaction_type,
                                                                   "EXEC_PRICE": trading_price,
                                                                   "ORDER_TYPE": config.trading_order_type,
                                                                   "PRODUCT": config.trading_product,
                                                                   "TARGET_TYPE": "BUY",
                                                                   "TARGET_PRICE": target_price, "PARENTORDER": "YES",
                                                                   "REFERENCE": None}}
                config.ORDER_MANAGER.update(temp_order)
                #print config.ORDER_MANAGER
            LOG.info("Primary Order Placed successfully TYPE=%s, price=%s with order_id=%s", trading_transaction_type, trading_price, json_order_place_response['data']['order_id'])

        # if the order is secondary order
        elif (parentorder == "NO"):
            timestamp = datetime.datetime.now();
            temp_order = {
                str(json_order_place_response['data']['order_id']): {"order_id": str(json_order_place_response['data']['order_id']),
                                                     "timestamp": timestamp,
                                                     "status": "placed", "SYMBOL": config.trading_symbol,
                                                     "QUANTITY": trading_quantity, "EXCHANGE": config.trading_exchange,
                                                     "TRADE_TYPE": trading_transaction_type,
                                                     "EXEC_PRICE": trading_price,
                                                     "ORDER_TYPE": config.trading_order_type,
                                                     "PRODUCT": config.trading_product,
                                                     "TARGET_TYPE": "NONE", "TARGET_PRICE": 0,
                                                     "PARENTORDER": "NO", "REFERENCE": ref}}
            if ref != None:
                temp_parent = config.ORDER_MANAGER.get(ref)
                temp_parent['REFERENCE'] = str(json_order_place_response['data']['order_id'])
                config.ORDER_MANAGER[ref] = temp_parent
                config.ORDER_MANAGER.update(temp_order);
            LOG.info("Secondary Order Placed successfully TYPE=%s, price=%s with order_id=%s", trading_transaction_type,
                 trading_price, json_order_place_response['data']['order_id'])
        return "success"
    else:
        if (parentorder == "YES"):
            LOG.info("Primary Order Placing failed TYPE=%s, price=%s. Order will be skipped", trading_transaction_type,
                     trading_price)
            return
        elif(parentorder == "NO"):
            return "fail"
