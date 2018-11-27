'''
Created on 04-Aug-2017

@author: kailash Rudra
Kite APIs to fetch the stocks
'''

import datetime
from kiteconnect import KiteTicker
import logging
import time
import save2file
from upstox_api.api import *
import pandas as pd
import config as CONFIG
from authentication import Authenticate


'''
    Intially starttime is the start time receiving the ticks and endtime is starttime + time interval (Defined in config)
    temp_tick is the list to store ltp received during time interval and after the time interval it is placed in the global tick variable
'''
global endtime, LOG, temp_tick, dict_key
LOG = None
endtime = {}
DEBUG_LEVELV_NUM = 9
dict_key = {}
temp_tick = {}

'''
    on_tick is a callback function for every tick received
'''


def on_ticks(ws, ticks):
    global endtime, temp_tick, dict_key, LOG

    for eachscript in ticks:
        tick_timestamp = eachscript['timestamp']
        tick_price = float(eachscript['last_price'])
        local_tick_dict = {}
        if CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO":
            market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                              minute=CONFIG.CLOSE_MIN, second=0,
                                                              microsecond=0)
        elif CONFIG.trading_exchange == "MCX":
            market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR_COMMODITY,
                                                              minute=CONFIG.CLOSE_MIN_COMMODITY, second=0,
                                                              microsecond=0)

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO" or CONFIG.trading_exchange == "MCX"):
            if (datetime.datetime.now() >= market_end_time) or \
                    (tick_timestamp.time() < datetime.time(CONFIG.OPEN_HR, CONFIG.OPEN_MIN, 0, 0)):
                """ if time is near to market openning time """
                std_openning_time = datetime.datetime.now().replace(hour=CONFIG.OPEN_HR, minute=0, second=0,
                                                                    microsecond=0)
                if (datetime.datetime.now() >= std_openning_time) and \
                        (datetime.datetime.now() < std_openning_time.replace(hour=CONFIG.OPEN_HR,
                                                                             minute=CONFIG.OPEN_MIN,
                                                                             second=0, microsecond=0)):
                    print("Instrument_token=", eachscript['instrument_token'], " Waiting for indian Market to open")
                    LOG[eachscript['instrument_token']].info("Waiting for indian Market to open")
                    return

                LOG[eachscript['instrument_token']].info("No Candles available after NSE Market hours")
                print("Instrument_token=", eachscript['instrument_token'],
                      " No Candles available after NSE Market hours")
                CONFIG.GlobalInstObj.exit_system()

            else:
                if (endtime[eachscript['instrument_token']] == None):
                    if (tick_timestamp.minute % CONFIG.time_interval != 0):
                        return

                    if tick_timestamp < CONFIG.SYSTEM_STARTED_TIME:
                        LOG[eachscript['instrument_token']].error(
                            "Found some spurious ticks. Waitting for correct start of time.")
                        return

                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][
                        len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]

                    ohlc = [open, high, low, close]
                    dict_key[eachscript['instrument_token']] = str(tick_timestamp.replace(second=0, microsecond=0))
                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    CONFIG.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             CONFIG.time_interval, str(local_tick_dict),
                                                             close)
                    if CONFIG.time_interval == 30:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 44, 59)
                        elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, 14, 59)
                    elif CONFIG.time_interval == 60:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, 14, 59)
                        elif (tick_timestamp.minute < 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 14, 59)
                    else:
                        if (tick_timestamp.minute + CONFIG.time_interval - (
                                tick_timestamp.minute % CONFIG.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, (
                                                                                                tick_timestamp.minute + CONFIG.time_interval - tick_timestamp.minute % CONFIG.time_interval - 1) % 60,
                                                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour,
                                                                                        tick_timestamp.minute + CONFIG.time_interval - (
                                                                                                tick_timestamp.minute % CONFIG.time_interval) - 1,
                                                                                        59)
                elif (tick_timestamp <= endtime[eachscript['instrument_token']]):
                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][
                        len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]

                    ohlc = [open, high, low, close]
                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    CONFIG.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             CONFIG.time_interval,
                                                             str(local_tick_dict),
                                                             close)
                else:
                    # print(CONFIG.MULTISTOCK)
                    del temp_tick[eachscript['instrument_token']][:]
                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][
                        len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]

                    ohlc = [open, high, low, close]
                    dict_key[eachscript['instrument_token']] = str(tick_timestamp.replace(second=0, microsecond=0))
                    if (tick_timestamp.minute % CONFIG.time_interval != 0):  # A case when socket disconnection happens
                        cur_min = tick_timestamp.minute - (tick_timestamp.minute % CONFIG.time_interval)
                        cur_mkt_idx = tick_timestamp.replace(minute=cur_min, second=0, microsecond=0)
                        tick_timestamp = cur_mkt_idx
                        dict_key[eachscript['instrument_token']] = str(cur_mkt_idx)

                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    CONFIG.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             CONFIG.time_interval,
                                                             str(local_tick_dict),
                                                             close)
                    if CONFIG.time_interval == 30:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 44, 59)
                        elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, 14, 59)
                    elif CONFIG.time_interval == 60:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, 14, 59)
                        elif (tick_timestamp.minute < 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour, 14, 59)
                    else:
                        if (tick_timestamp.minute + CONFIG.time_interval - (
                                tick_timestamp.minute % CONFIG.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, (
                                                                                                tick_timestamp.minute + CONFIG.time_interval - tick_timestamp.minute % CONFIG.time_interval - 1) % 60,
                                                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour,
                                                                                        tick_timestamp.minute + CONFIG.time_interval - (
                                                                                                tick_timestamp.minute % CONFIG.time_interval) - 1,
                                                                                        59)


def on_connect(ws, response):
    ws.subscribe(CONFIG.TRADE_INSTRUMENT)
    ws.set_mode(ws.MODE_FULL, CONFIG.TRADE_INSTRUMENT)


def main_kite():
    global temp_tick, dict_key, endtime, LOG
    handler = {}
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for i in CONFIG.TRADE_INSTRUMENT:
        LOG[i] = None
        endtime[i] = None
        temp_tick[i] = []
        dict_key[i] = ""
        handler[i] = logging.FileHandler(
            filename=CONFIG.STD_PATH + 'logs/DataFetcher/DataFetcher-' + str(i) + '-' + timestr + '.log', mode='w')
        handler[i].setFormatter(formatter)
        LOG[i] = logging.getLogger("DataFetcher-" + str(i))
        LOG[i].setLevel(logging.INFO)
        LOG[i].addHandler(handler[i])

    kws = KiteTicker(CONFIG.API_KEY, CONFIG.ACCESS_TOKEN, CONFIG.CLIENT_ID)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect()


def event_handler_quote_disconnect(err):
    """Auto connect the websocket if it's disrupted"""
    print("Socket disconnected. Will auto reconnect - ", err)
    LOG.error("Socket disconnected. Will auto reconnect - %s", err)

    CONFIG.UPSTOX_SESSION.start_websocket(True)
    condition = threading.Condition()
    condition.acquire()
    condition.wait()


"""
As of today, the quote update is getting invoked for each individual stock
"""
def event_handler_quote_update(message):
    try:
        stock_name = str(message['symbol']).upper()
        timestamp = datetime.fromtimestamp(float(message['timestamp']) / 1000.0)
        LOG.info("Quote Update: timestamp=%s %s", timestamp, str(message))
        if (stock_name in CONFIG.TRADE_INSTRUMENT):
            timestamp = datetime.fromtimestamp(float(message['timestamp']) / 1000.0)
            if not (timestamp.hour > CONFIG.CLOSE_HR and timestamp.minute >= CONFIG.CLOSE_MIN):
                CONFIG.MULTISTOCK[stock_name]['CMP'] = float(format(float(message['ltp']), '.2f'))
                CONFIG.MULTISTOCK[stock_name]['LTP'][timestamp] = float(format(float(message['ltp']), '.2f'))
            else:
                LOG.error("%s - Not handling quotes as Current timestamp beyond trading time.", stock_name)
        else:
            LOG.error("%s - not available in instrument list", stock_name)
            return

    except Exception as e:
        LOG.error("Exception during quote update event handler.")
        LOG.error("%s", str(e))
        return

    return

def unsubscribe_stocks(exchange):
    if exchange not in ['NSE_EQ','NSE_FO','BSE_EQ', 'BSE_FO','MCX_FO']:
        LOG.error("Invalid Exchange. Stock unsubscprition failed.")
        return False

    retry = 1
    while (retry <= 5):
        try:
            if exchange == 'NSE_EQ':
                for stock in CONFIG.TRADE_INSTRUMENT:
                    CONFIG.UPSTOX_SESSION.unsubscribe(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                                                      LiveFeedType.LTP)
                    LOG.info("%s - NSE_EQ -Live feed unsubscription successful.", stock)
                break

            if exchange == 'MCX_FO':
                for stock in CONFIG.TRADE_INSTRUMENT:
                    CONFIG.UPSTOX_SESSION.unsubscribe(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('MCX_FO', "CRUDEOIL18DECFUT"),
                                                    LiveFeedType.LTP)
                    LOG.info("%s - MCX_FO - Live feed unsubscription successful.", stock)
                break

        except Exception as e:
            LOG.error("Exception during unsubscription. Attempt:%d", retry)
            retry = retry + 1
            if retry == 5:
                LOG.error("All retries failed for live feed unsubscription. Exiting Datafetcher")
                return -1
    return True


def subscribe_stocks(exchange):
    if exchange not in ['NSE_EQ','NSE_FO','BSE_EQ', 'BSE_FO','MCX_FO']:
        LOG.error("Invalid Exchange. Stock subscprition failed.")
        return False

    retry = 1
    while (retry <= 5):
        try:
            if exchange=='NSE_EQ':
                for stock in CONFIG.TRADE_INSTRUMENT:
                    CONFIG.UPSTOX_SESSION.subscribe(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                                                      LiveFeedType.Full)
                    LOG.info("%s - NSE_EQ - Live feed subscription successful.", stock)
                break

            if exchange=='MCX_FO':
                for stock in CONFIG.TRADE_INSTRUMENT:
                    CONFIG.UPSTOX_SESSION.subscribe(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('MCX_FO', stock),
                                                    LiveFeedType.Full)
                    LOG.info("%s - MCX_FO -Live feed subscription successful.", stock)
                break

        except Exception as e:
            LOG.error("Exception during subscription. Attempt:%d", retry)
            retry = retry + 1
            if retry == 5:
                LOG.error("All retries failed for live feed subscription. Exiting Datafetcher")
                return -1



def main_upstox():
    CONFIG.UPSTOX_SESSION.set_on_quote_update(event_handler_quote_update)
    CONFIG.UPSTOX_SESSION.set_on_disconnect(event_handler_quote_disconnect)
    CONFIG.UPSTOX_SESSION.get_master_contract('NSE_EQ')
    #CONFIG.UPSTOX_SESSION.get_master_contract('NSE_FO')
    #CONFIG.UPSTOX_SESSION.get_master_contract('MCX_FO')
    #print(CONFIG.UPSTOX_SESSION.get_master_contract('MCX_FO'))
    """
    First unsubscribe the stocks.
    """
    unsubscribe_stocks('NSE_EQ')
    #unsubscribe_stocks('MCX_FO')


    """
    Perform a fresh subscription.
    """
    subscribe_stocks('NSE_EQ')
    #subscribe_stocks('MCX_FO')

    LOG.info("Starting the websocket...")
    CONFIG.UPSTOX_SESSION.start_websocket(True)
    condition = threading.Condition()
    condition.acquire()
    condition.wait()


    return


def main():
    global LOG
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/DataFetcher-' + timestr + '.log', mode='w')
    handler.setFormatter(formatter)
    LOG = logging.getLogger("DataFetcher")
    LOG.setLevel(logging.INFO)
    LOG.addHandler(handler)

    credentials_dict = json.load(open(CONFIG.STD_PATH + "configfiles/credentials_upstox.txt"))
    if credentials_dict["BROKER"] == "UPSTOX":
        LOG.info("Websocket initialization for broker - UPSTOX")
        main_upstox()
    elif credentials_dict["BROKER"] == "ZERODHA":
        LOG.info("Websocket initialization for broker - ZERODHA")
        main_kite()
    else:
        LOG.error("Error: Unknown broker.")
        return False

    return


if __name__ == '__main__':
    CONFIG.init()
    auth = Authenticate()
    login_status = auth.login()
    if not login_status:
        print("Authentication failed. Exiting...")
        os._exit(1)

    main()


