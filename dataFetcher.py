'''
Created on 04-Aug-2017

@author: kailash Rudra
Kite APIs to fetch the stocks
'''

import config
import datetime
from kiteconnect import KiteTicker
import logging
import time
import save2file
from upstox_api.api import *
import pandas as pd

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
        if config.trading_exchange == "NSE" or config.trading_exchange == "NFO":
            market_end_time = datetime.datetime.now().replace(hour=config.CLOSE_HR,
                                                              minute=config.CLOSE_MIN, second=0,
                                                              microsecond=0)
        elif config.trading_exchange == "MCX":
            market_end_time = datetime.datetime.now().replace(hour=config.CLOSE_HR_COMMODITY,
                                                              minute=config.CLOSE_MIN_COMMODITY, second=0,
                                                              microsecond=0)

        if (config.trading_exchange == "NSE" or config.trading_exchange == "NFO" or config.trading_exchange == "MCX"):
            if (datetime.datetime.now() >= market_end_time) or \
                    (tick_timestamp.time() < datetime.time(config.OPEN_HR, config.OPEN_MIN, 0, 0)):
                """ if time is near to market openning time """
                std_openning_time = datetime.datetime.now().replace(hour=config.OPEN_HR, minute=0, second=0,
                                                                    microsecond=0)
                if (datetime.datetime.now() >= std_openning_time) and \
                        (datetime.datetime.now() < std_openning_time.replace(hour=config.OPEN_HR,
                                                                             minute=config.OPEN_MIN,
                                                                             second=0, microsecond=0)):
                    print("Instrument_token=", eachscript['instrument_token'], " Waiting for indian Market to open")
                    LOG[eachscript['instrument_token']].info("Waiting for indian Market to open")
                    return

                LOG[eachscript['instrument_token']].info("No Candles available after NSE Market hours")
                print("Instrument_token=", eachscript['instrument_token'],
                      " No Candles available after NSE Market hours")
                config.GlobalInstObj.exit_system()

            else:
                if (endtime[eachscript['instrument_token']] == None):
                    if (tick_timestamp.minute % config.time_interval != 0):
                        return

                    if tick_timestamp < config.SYSTEM_STARTED_TIME:
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
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             config.time_interval, str(local_tick_dict),
                                                             close)
                    if config.time_interval == 30:
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
                    elif config.time_interval == 60:
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
                        if (tick_timestamp.minute + config.time_interval - (
                                tick_timestamp.minute % config.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, (
                                                                                                tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour,
                                                                                        tick_timestamp.minute + config.time_interval - (
                                                                                                tick_timestamp.minute % config.time_interval) - 1,
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
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             config.time_interval,
                                                             str(local_tick_dict),
                                                             close)
                else:
                    # print(config.MULTISTOCK)
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
                    if (tick_timestamp.minute % config.time_interval != 0):  # A case when socket disconnection happens
                        cur_min = tick_timestamp.minute - (tick_timestamp.minute % config.time_interval)
                        cur_mkt_idx = tick_timestamp.replace(minute=cur_min, second=0, microsecond=0)
                        tick_timestamp = cur_mkt_idx
                        dict_key[eachscript['instrument_token']] = str(cur_mkt_idx)

                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f",
                                                             config.time_interval,
                                                             str(local_tick_dict),
                                                             close)
                    if config.time_interval == 30:
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
                    elif config.time_interval == 60:
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
                        if (tick_timestamp.minute + config.time_interval - (
                                tick_timestamp.minute % config.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour + 1, (
                                                                                                tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year,
                                                                                        tick_timestamp.month,
                                                                                        tick_timestamp.day,
                                                                                        tick_timestamp.hour,
                                                                                        tick_timestamp.minute + config.time_interval - (
                                                                                                tick_timestamp.minute % config.time_interval) - 1,
                                                                                        59)


def on_connect(ws, response):
    ws.subscribe(config.TRADE_INSTRUMENT)
    ws.set_mode(ws.MODE_FULL, config.TRADE_INSTRUMENT)


def main_kite():
    global temp_tick, dict_key, endtime, LOG
    handler = {}
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for i in config.TRADE_INSTRUMENT:
        LOG[i] = None
        endtime[i] = None
        temp_tick[i] = []
        dict_key[i] = ""
        handler[i] = logging.FileHandler(
            filename=config.STD_PATH + 'logs/DataFetcher/DataFetcher-' + str(i) + '-' + timestr + '.log', mode='w')
        handler[i].setFormatter(formatter)
        LOG[i] = logging.getLogger("DataFetcher-" + str(i))
        LOG[i].setLevel(logging.INFO)
        LOG[i].addHandler(handler[i])

    kws = KiteTicker(config.API_KEY, config.ACCESS_TOKEN, config.CLIENT_ID)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect()


def event_handler_quote_update(message):
    stock_name = str(message['symbol']).upper()

    if (stock_name in config.TRADE_INSTRUMENT):
        timestamp = datetime.fromtimestamp(float(message['timestamp']) / 1000.0)
        if not (timestamp.hour > 23 and timestamp.minute >= config.CLOSE_MIN):
            try:
                config.MULTISTOCK[stock_name]['CMP'] = float(format(float(message['ltp']), '.2f'))
                config.MULTISTOCK[stock_name]['LTP'][timestamp] = float(format(float(message['ltp']), '.2f'))
                df = pd.DataFrame(list(config.MULTISTOCK[stock_name]['LTP'].items()), columns=['Date', 'DateValue'])
                data = df.set_index(['Date'])
                """The LTP data after resample are upto the current timeframe accuracy.
                """
                data_5min = data['DateValue'].resample('5min').ohlc()
                data_10min = data['DateValue'].resample('10min').ohlc()
                data_15min = data['DateValue'].resample('15min', base=15).ohlc()
                data_30min = data['DateValue'].resample('30Min', base=15).ohlc()
                data_1hour = data['DateValue'].resample('60Min', base=15).ohlc()

            except Exception as e:
                LOG.error("Exception during resampling.")
                LOG.error("%s", str(e))
                return

            if not config.MULTISTOCK[stock_name]['5MIN']['TICKS'].empty:
                config.MULTISTOCK[stock_name]['5MIN']['TICKS'] \
                    = pd.concat([config.MULTISTOCK[stock_name]['5MIN']['TICKS'], data_5min]).drop_duplicates()
                config.MULTISTOCK[stock_name]['5MIN']['TICKS'] = \
                    config.MULTISTOCK[stock_name]['5MIN']['TICKS'][
                        ~config.MULTISTOCK[stock_name]['5MIN']['TICKS'].index.duplicated(keep='last')]
                LOG.info("%s - 5Min TICKS\n%s\n", stock_name, config.MULTISTOCK[stock_name]['5MIN']['TICKS'].tail(3))

            if not config.MULTISTOCK[stock_name]['10MIN']['TICKS'].empty:
                config.MULTISTOCK[stock_name]['10MIN']['TICKS'] \
                    = pd.concat([config.MULTISTOCK[stock_name]['10MIN']['TICKS'], data_10min]).drop_duplicates()
                config.MULTISTOCK[stock_name]['10MIN']['TICKS'] = \
                    config.MULTISTOCK[stock_name]['10MIN']['TICKS'][
                        ~config.MULTISTOCK[stock_name]['10MIN']['TICKS'].index.duplicated(keep='last')]
                LOG.info("%s - 10Min TICKS\n%s\n", stock_name, config.MULTISTOCK[stock_name]['10MIN']['TICKS'].tail(3))

            if not config.MULTISTOCK[stock_name]['15MIN']['TICKS'].empty:
                config.MULTISTOCK[stock_name]['15MIN']['TICKS'] \
                    = pd.concat([config.MULTISTOCK[stock_name]['15MIN']['TICKS'], data_15min]).drop_duplicates()
                config.MULTISTOCK[stock_name]['15MIN']['TICKS'] = \
                    config.MULTISTOCK[stock_name]['15MIN']['TICKS'][
                        ~config.MULTISTOCK[stock_name]['15MIN']['TICKS'].index.duplicated(keep='last')]
                LOG.info("%s - 15Min TICKS\n%s\n", stock_name, config.MULTISTOCK[stock_name]['15MIN']['TICKS'].tail(3))

            if not config.MULTISTOCK[stock_name]['30MIN']['TICKS'].empty:
                config.MULTISTOCK[stock_name]['30MIN']['TICKS'] \
                    = pd.concat([config.MULTISTOCK[stock_name]['30MIN']['TICKS'], data_30min]).drop_duplicates()
                config.MULTISTOCK[stock_name]['30MIN']['TICKS'] = \
                    config.MULTISTOCK[stock_name]['30MIN']['TICKS'][
                        ~config.MULTISTOCK[stock_name]['30MIN']['TICKS'].index.duplicated(keep='last')]
                LOG.info("%s - 30Min TICKS\n%s\n", stock_name, config.MULTISTOCK[stock_name]['30MIN']['TICKS'].tail(3))

            if not config.MULTISTOCK[stock_name]['1HOUR']['TICKS'].empty:
                config.MULTISTOCK[stock_name]['1HOUR']['TICKS'] \
                    = pd.concat([config.MULTISTOCK[stock_name]['1HOUR']['TICKS'], data_1hour]).drop_duplicates()
                config.MULTISTOCK[stock_name]['1HOUR']['TICKS'] = \
                    config.MULTISTOCK[stock_name]['1HOUR']['TICKS'][
                        ~config.MULTISTOCK[stock_name]['1HOUR']['TICKS'].index.duplicated(keep='last')]
                LOG.info("%s - 1Hour TICKS\n%s\n", stock_name, config.MULTISTOCK[stock_name]['1HOUR']['TICKS'].tail(3))

        else:
            LOG.error("%s - Not handling quotes as Current timestamp beyond 3.30PM.", stock_name)
    else:
        LOG.error("%s - not available in instrument list", stock_name)
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
                for stock in config.TRADE_INSTRUMENT:
                    config.UPSTOX_SESSION.unsubscribe(config.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                                                      LiveFeedType.LTP)
                    LOG.info("%s - NSE_EQ -Live feed unsubscription successful.", stock)
                break

            if exchange == 'MCX_FO':
                for stock in config.TRADE_INSTRUMENT:
                    config.UPSTOX_SESSION.unsubscribe(config.UPSTOX_SESSION.get_instrument_by_symbol('MCX_FO', "CRUDEOIL18DECFUT"),
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
                for stock in config.TRADE_INSTRUMENT:
                    config.UPSTOX_SESSION.subscribe(config.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                                                      LiveFeedType.LTP)
                    LOG.info("%s - NSE_EQ - Live feed subscription successful.", stock)
                break

            if exchange=='MCX_FO':
                for stock in config.TRADE_INSTRUMENT:
                    config.UPSTOX_SESSION.subscribe(config.UPSTOX_SESSION.get_instrument_by_symbol('MCX_FO', stock),
                                                    LiveFeedType.LTP)
                    LOG.info("%s - MCX_FO -Live feed subscription successful.", stock)
                break

        except Exception as e:
            LOG.error("Exception during subscription. Attempt:%d", retry)
            retry = retry + 1
            if retry == 5:
                LOG.error("All retries failed for live feed subscription. Exiting Datafetcher")
                return -1



def main_upstox():
    config.UPSTOX_SESSION.set_on_quote_update(event_handler_quote_update)
    config.UPSTOX_SESSION.get_master_contract('NSE_EQ')
    #config.UPSTOX_SESSION.get_master_contract('NSE_FO')
    #config.UPSTOX_SESSION.get_master_contract('MCX_FO')

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
    config.UPSTOX_SESSION.start_websocket(True)

    condition = threading.Condition()
    condition.acquire()
    condition.wait()

    return


def main():
    global LOG
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=config.STD_PATH + 'logs/DataFetcher-' + timestr + '.log', mode='w')
    handler.setFormatter(formatter)
    LOG = logging.getLogger("DataFetcher")
    LOG.setLevel(logging.INFO)
    LOG.addHandler(handler)

    credentials_dict = json.load(open(config.STD_PATH + "configfiles/credentials_upstox.txt"))
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

"""
if __name__ == '__main__':
    import config as CONFIG
    from authentication import Authenticate

    CONFIG.init()

    auth = Authenticate()
    login_status = auth.login()
    if not login_status:
        print("Authentication failed. Exiting...")
        os._exit(1)

    main()
"""

