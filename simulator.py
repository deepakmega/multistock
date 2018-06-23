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
global endtime , endtime_secondary ,LOG1
LOG1=None
endtime = None
endtime_secondary = None
DEBUG_LEVELV_NUM = 9
global temp_tick, temp_tick_secondary
temp_tick = []
temp_tick_secondary = []
dick_key =""
MKT_TICK=[]
MKT_IDX=0

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

def create_tick(idx):
    if idx > len(MKT_TICK):
        print("No more ticks. Market has closed")
        LOG1.info("No more ticks. Market has closed")
        exit(0)

    on_ticks(MKT_TICK[idx])

def main():
    with open(config.STD_PATH+'configfiles/simulation-2018-06-07.091400.txt','r') as file:
        for line in file:
            out = re.search('(\d+)(\.)(\d+)', line)
            if out:
                MKT_TICK.append(out.group(0))
    return

def simulator():
    global LOG1
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    handler = logging.FileHandler(filename=config.STD_PATH + 'logs/Simulation/Simulator-' + timestr + '.log', mode='w')
    handler.setFormatter(formatter)
    LOG1 = logging.getLogger("Simulation")
    LOG1.setLevel(logging.INFO)
    LOG1.addHandler(handler)
    main()
    idx = 0
    create_tick(idx)

    market_end_time = datetime.datetime.now().replace(hour=config.CLOSE_HR, minute=config.CLOSE_MIN, second=0,
                                                      microsecond=0)
    while True:
        if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
            print("The market has closed... ")
            return
        idx = idx + 1
        Timer(1, create_tick,[idx]).run()
    return

if __name__=='__main__':
    simulator()