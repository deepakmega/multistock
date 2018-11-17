import pdb
import re
import datetime, time
import threading
from threading import Timer
import DataFetcher
import config
import logging
import json
import save2file


'''
    Intially starttime is the start time receiving the ticks and endtime is starttime + time interval (Defined in config)
    temp_tick is the list to store ltp received during time interval and after the time interval it is placed in the global tick variable
'''
"""
global endtime , endtime_secondary ,LOG1
LOG1=None
endtime = None
endtime_secondary = None
DEBUG_LEVELV_NUM = 9
global temp_tick, temp_tick_secondary
temp_tick = []
temp_tick_secondary = []
dick_key =""
"""
MKT_TICK=[]
MKT_IDX=0

global endtime , LOG, temp_tick, dict_key
LOG = {}
endtime = {}
DEBUG_LEVELV_NUM = 9
dict_key = {}
temp_tick = {}

'''
    on_tick is a callback function for every tick received
    
    global endtime
    global temp_tick
    global endtime_secondary
    global temp_tick_secondary
    global LOG1
    global dict_key
    local_tick_dict = {}
    local_tick_dict_sec = {}
    tick_timestamp = datetime.datetime.now().replace(microsecond=0)
    tick_price = float(ticks)
    config.MARKET_LTP = tick_price
'''
def on_ticks(ticks):
    global endtime
    global temp_tick
    global endtime_secondary
    global temp_tick_secondary
    global LOG1
    global dict_key
    local_tick_dict = {}
    local_tick_dict_sec = {}
    tick_timestamp = datetime.datetime.now().replace(microsecond=0)
    tick_price = float(ticks)
    config.MARKET_LTP = tick_price


    if (config.trading_exchange == "MCX"):
        config.PIP = config.CRUDE_PIP
        LOG1.info("Trading exchange:%s, PIP selected is:%d", config.trading_exchange, config.PIP)
        if (tick_timestamp.time() >= datetime.time(23, 59, 0, 0) or tick_timestamp.time() < datetime.time(0, 0, 0, 0)):
            LOG1.info("No Candles available after MCX market hours!")
            return
        else:
            if (endtime == None):
                if (tick_timestamp.minute % config.time_interval != 0):
                    return

                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                dict_key = str(tick_timestamp.replace(second=0, microsecond=0))
                local_tick_dict[dict_key] = ohlc
                config.tick.update(local_tick_dict)
                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)
                if config.time_interval == 30:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 44, 59)
                    elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                elif config.time_interval == 60:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                    elif (tick_timestamp.minute < 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 14, 59)
                else:
                    if (tick_timestamp.minute + config.time_interval - (
                                tick_timestamp.minute % config.time_interval) - 1 >= 60):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, (
                                                        tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                    59)
                    else:
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour,
                                                    tick_timestamp.minute + config.time_interval - (
                                                        tick_timestamp.minute % config.time_interval) - 1, 59)
            elif (tick_timestamp <= endtime):
                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                local_tick_dict[dict_key] = ohlc
                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)
                config.tick.update(local_tick_dict)
            else:
                del temp_tick[:]
                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                dict_key = str(tick_timestamp.replace(second=0, microsecond=0))
                local_tick_dict[dict_key] = ohlc
                config.tick.update(local_tick_dict)
                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)

                # temp_tick.append(tick_price)

                if config.time_interval == 30:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 44, 59)
                    elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                elif config.time_interval == 60:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                    elif (tick_timestamp.minute < 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 14, 59)
                else:
                    if (tick_timestamp.minute + config.time_interval - (
                                tick_timestamp.minute % config.time_interval) - 1 >= 60):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, (
                                                        tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                    59)
                    else:
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour,
                                                    tick_timestamp.minute + config.time_interval - (
                                                        tick_timestamp.minute % config.time_interval) - 1, 59)

    elif (config.trading_exchange == "NSE" or config.trading_exchange == "NFO"):
        config.PIP = config.NIFTY_PIP
        LOG1.info("Trading exchange:%s, PIP selected is:%d", config.trading_exchange, config.PIP)
        if(tick_timestamp.time() >= datetime.time(23,59,0,0) or tick_timestamp.time() < datetime.time(0,0,0,0)):
            LOG1.info("No Candles available after NSE Market hours")
            return
        else:
            if (endtime == None):
                if(tick_timestamp.minute % config.time_interval !=0):
                    return

                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                dict_key = str(tick_timestamp.replace(second=0, microsecond=0))
                local_tick_dict[dict_key] = ohlc
                config.tick.update(local_tick_dict)


                fileData = dict_key + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                save2file.savecandletofile(fileData, dict_key)

                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)
                if config.time_interval ==30:
                    if (tick_timestamp.hour ==15 and tick_timestamp.minute >= 15 and tick_timestamp.minute<30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15 and tick_timestamp.minute<45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 44, 59)
                    elif (tick_timestamp.minute < 15 and tick_timestamp.minute>=45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour+1, 14, 59)
                elif config.time_interval ==60:
                    if (tick_timestamp.hour ==15 and tick_timestamp.minute >= 15 and tick_timestamp.minute<30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour+1, 14, 59)
                    elif (tick_timestamp.minute < 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 14, 59)
                else:
                    if (tick_timestamp.minute + config.time_interval - (
                        tick_timestamp.minute % config.time_interval) - 1 >= 60):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, (
                                                    tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                    59)
                    else:
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, tick_timestamp.minute + config.time_interval - (
                                                    tick_timestamp.minute % config.time_interval) - 1, 59)
            elif (tick_timestamp <= endtime):
                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                local_tick_dict[dict_key] = ohlc
                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)
                config.tick.update(local_tick_dict)

                fileData = dict_key + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                save2file.updatecandletofile(fileData, dict_key)
            else:

                # Add candle to file
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]
                fileData = dict_key + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                save2file.updatecandletofile(fileData, dict_key)

                del temp_tick[:]
                temp_tick.append(tick_price)
                open = temp_tick[0]
                close = temp_tick[len(temp_tick) - 1]
                temp_list = sorted(temp_tick, key=float)
                low = temp_list[0]
                high = temp_list[len(temp_list) - 1]

                ohlc = [open, high, low, close]
                dict_key = str(tick_timestamp.replace(second=0, microsecond=0))
                local_tick_dict[dict_key] = ohlc
                config.tick.update(local_tick_dict)
                LOG1.info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                          config.MARKET_LTP)

                fileData = dict_key + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                save2file.savecandletofile(fileData, dict_key)
                #temp_tick.append(tick_price)

                if config.time_interval == 30:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 44, 59)
                    elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                elif config.time_interval == 60:
                    if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 29, 59)
                    elif (tick_timestamp.minute >= 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, 14, 59)
                    elif (tick_timestamp.minute < 15):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, 14, 59)
                else:
                    if (tick_timestamp.minute + config.time_interval - (
                                tick_timestamp.minute % config.time_interval) - 1 >= 60):
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour + 1, (
                                                        tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                    59)
                    else:
                        endtime = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                    tick_timestamp.hour, tick_timestamp.minute + config.time_interval - (
                                                        tick_timestamp.minute % config.time_interval) - 1, 59)


def on_ticks_multistock(ticks):
    global endtime, temp_tick,dict_key, LOG

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
            if(False):
                """ if time is near to market openning time """
                std_openning_time = datetime.datetime.now().replace(hour=config.OPEN_HR, minute=0, second=0, microsecond=0)
                if (datetime.datetime.now() >= std_openning_time) and \
                        (datetime.datetime.now() < std_openning_time.replace(hour=config.OPEN_HR,
                                                                            minute=config.OPEN_MIN,
                                                                            second=0, microsecond=0)):

                    print("Instrument_token=", eachscript['instrument_token']," Waiting for indian Market to open")
                    LOG[eachscript['instrument_token']].info("Waiting for indian Market to open")
                    return

                LOG[eachscript['instrument_token']].info("No Candles available after NSE Market hours")
                print("Instrument_token=",eachscript['instrument_token']," No Candles available after NSE Market hours")
                config.GlobalInstObj.exit_system()

            else:
                if (endtime[eachscript['instrument_token']] == None):
                    if(tick_timestamp.minute % config.time_interval !=0):
                        return

                    if tick_timestamp < config.SYSTEM_STARTED_TIME:
                        LOG[eachscript['instrument_token']].error("Found some spurious ticks. Waitting for correct start of time.")
                        return

                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]

                    ohlc = [open, high, low, close]
                    dict_key[eachscript['instrument_token']] = str(tick_timestamp.replace(second=0, microsecond=0))
                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval, str(local_tick_dict),
                              close)

                    fileData = dict_key[eachscript['instrument_token']] + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                    save2file.savecandletofile(fileData, dict_key[eachscript['instrument_token']])

                    if config.time_interval ==30:
                        if (tick_timestamp.hour ==15 and tick_timestamp.minute >= 15 and tick_timestamp.minute<30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15 and tick_timestamp.minute<45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 44, 59)
                        elif (tick_timestamp.minute < 15 and tick_timestamp.minute>=45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour+1, 14, 59)
                    elif config.time_interval ==60:
                        if (tick_timestamp.hour ==15 and tick_timestamp.minute >= 15 and tick_timestamp.minute<30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour+1, 14, 59)
                        elif (tick_timestamp.minute < 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 14, 59)
                    else:
                        if (tick_timestamp.minute + config.time_interval - (
                            tick_timestamp.minute % config.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour + 1, (
                                                        tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, tick_timestamp.minute + config.time_interval - (
                                                        tick_timestamp.minute % config.time_interval) - 1, 59)
                elif (tick_timestamp <= endtime[eachscript['instrument_token']]):
                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]

                    ohlc = [open, high, low, close]
                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval,
                             str(local_tick_dict),
                             close)

                    fileData = dict_key[eachscript['instrument_token']] + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                    save2file.updatecandletofile(fileData, dict_key[eachscript['instrument_token']])

                else:
                    #print(config.MULTISTOCK)
                    del temp_tick[eachscript['instrument_token']][:]
                    temp_tick[eachscript['instrument_token']].append(tick_price)
                    open = temp_tick[eachscript['instrument_token']][0]
                    close = temp_tick[eachscript['instrument_token']][len(temp_tick[eachscript['instrument_token']]) - 1]
                    temp_list = sorted(temp_tick[eachscript['instrument_token']], key=float)
                    low = temp_list[0]
                    high = temp_list[len(temp_list) - 1]
                    #fileData = dict_key[eachscript['instrument_token']] + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                    #save2file.updatecandletofile(fileData, dict_key[eachscript['instrument_token']])

                    ohlc = [open, high, low, close]
                    dict_key[eachscript['instrument_token']] = str(tick_timestamp.replace(second=0, microsecond=0))
                    if (tick_timestamp.minute % config.time_interval != 0):  # A case when socket disconnection happens
                        cur_min = tick_timestamp.minute - (tick_timestamp.minute % config.time_interval)
                        cur_mkt_idx = tick_timestamp.replace(minute=cur_min, second=0, microsecond=0)
                        tick_timestamp = cur_mkt_idx
                        dict_key[eachscript['instrument_token']] = str(cur_mkt_idx)

                    local_tick_dict[dict_key[eachscript['instrument_token']]] = ohlc
                    config.MULTISTOCK[eachscript['instrument_token']]['ticks'].update(local_tick_dict)
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval,
                             str(local_tick_dict),
                             close)

                    fileData = dict_key[eachscript['instrument_token']] + "," + str(open) + "," + str(high) + "," + str(low) + "," + str(close)
                    save2file.savecandletofile(fileData, dict_key[eachscript['instrument_token']])

                    if config.time_interval == 30:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15 and tick_timestamp.minute < 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 44, 59)
                        elif (tick_timestamp.minute < 15 and tick_timestamp.minute >= 45):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour + 1, 14, 59)
                    elif config.time_interval == 60:
                        if (tick_timestamp.hour == 15 and tick_timestamp.minute >= 15 and tick_timestamp.minute < 30):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 29, 59)
                        elif (tick_timestamp.minute >= 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour + 1, 14, 59)
                        elif (tick_timestamp.minute < 15):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, 14, 59)
                    else:
                        if (tick_timestamp.minute + config.time_interval - (
                                    tick_timestamp.minute % config.time_interval) - 1 >= 60):
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour + 1, (
                                                            tick_timestamp.minute + config.time_interval - tick_timestamp.minute % config.time_interval - 1) % 60,
                                                        59)
                        else:
                            endtime[eachscript['instrument_token']] = datetime.datetime(tick_timestamp.year, tick_timestamp.month, tick_timestamp.day,
                                                        tick_timestamp.hour, tick_timestamp.minute + config.time_interval - (
                                                            tick_timestamp.minute % config.time_interval) - 1, 59)


def create_tick(idx):
    if idx >= len(MKT_TICK)-1:
        print("No more ticks. Market has closed")
        LOG1.info("No more ticks. Market has closed")
        exit(0)

    multistock = {}
    multistock['instrument_token'] = config.TRADE_INSTRUMENT[0]
    multistock['timestamp'] = datetime.datetime.now().replace(microsecond=0)
    multistock['last_price'] = MKT_TICK[idx]
    on_ticks_multistock([multistock])

def main():
    with open(config.STD_PATH+'configfiles/simulation-738561-2018-08-14.091400.txt','r') as file:
        for line in file:
            out = re.search('(\d+)(\.)(\d+)', line)
            if out:
                MKT_TICK.append(out.group(0))
    return


def simulator():
    """
    global LOG1
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=config.STD_PATH + 'logs/Simulation/Simulator-' + timestr + '.log', mode='w')
    handler.setFormatter(formatter)
    LOG1 = logging.getLogger("Simulation")
    LOG1.setLevel(logging.INFO)
    LOG1.addHandler(handler)
    """
    global temp_tick, dict_key, endtime, LOG
    handler = {}
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #config.TRADE_INSTRUMENT = [12345]
    for i in config.TRADE_INSTRUMENT:
        config.MULTISTOCK[i] = {}
        config.MULTISTOCK[i]['ticks'] = {}

    for i in config.TRADE_INSTRUMENT:
        LOG[i] = None
        endtime[i] = None
        temp_tick[i] = []
        dict_key[i] = ""
        handler[i] = logging.FileHandler(
            filename=config.STD_PATH + 'logs/Simulation/Simulator-' + str(i) + '-' + timestr + '.log', mode='w')
        handler[i].setFormatter(formatter)
        LOG[i] = logging.getLogger("Simulator-" + str(i))
        LOG[i].setLevel(logging.INFO)
        LOG[i].addHandler(handler[i])

    main()

    while (True):
        tick_timestamp = datetime.datetime.now().replace(microsecond=0)
        if (tick_timestamp.minute % config.time_interval == 0):
            break

    print("Market simulation started.")

    idx = 0
    create_tick(idx)

    market_end_time = datetime.datetime.now().replace(hour=config.CLOSE_HR, minute=config.CLOSE_MIN, second=0,
                                                      microsecond=0)

    while True:
        idx = idx + 1
        Timer(1, create_tick,[idx]).run()
    return

if __name__=='__main__':
    simulator()