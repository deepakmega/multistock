'''
Created on 04-Aug-2017

@author: kailash Rudra
Kite APIs to fetch the stocks
'''


import config
import datetime
from kiteconnect import KiteTicker
import logging
import  time
import save2file



'''
    Intially starttime is the start time receiving the ticks and endtime is starttime + time interval (Defined in config)
    temp_tick is the list to store ltp received during time interval and after the time interval it is placed in the global tick variable
'''
global endtime , LOG, temp_tick, dict_key
LOG = {}
endtime = {}
DEBUG_LEVELV_NUM = 9
dict_key = {}
temp_tick = {}


'''
    on_tick is a callback function for every tick received
'''

def on_ticks(ws, ticks):
    global endtime, temp_tick,dict_key,LOG

    for eachscript in ticks:
        tick_timestamp = eachscript['timestamp']
        tick_price = float(eachscript['last_price'])
        local_tick_dict = {}
        if (config.trading_exchange == "NSE" or config.trading_exchange == "NFO"):
            if(tick_timestamp.time() >= datetime.time(config.CLOSE_HR,config.CLOSE_MIN,0,0) \
                       or tick_timestamp.time() < datetime.time(config.OPEN_HR,config.OPEN_MIN,0,0)):
                """ if time is near to market openning time """
                if (datetime.datetime.now() >= datetime.datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
                    and datetime.datetime.now() < datetime.datetime.now().replace(hour=config.OPEN_HR,
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
                else:
                    #print(config.MULTISTOCK)
                    del temp_tick[eachscript['instrument_token']][:]
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
                    LOG[eachscript['instrument_token']].info("Primary candle:interval=%d ,ohlc=%s, ltp=%f", config.time_interval,
                             str(local_tick_dict),
                             close)
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


def on_connect(ws, response):
    ws.subscribe(config.TRADE_INSTRUMENT)
    ws.set_mode(ws.MODE_FULL, config.TRADE_INSTRUMENT)

def main():
    global temp_tick, dict_key,endtime,LOG
    handler = {}
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for i in config.TRADE_INSTRUMENT:
        LOG[i] = None
        endtime[i] = None
        temp_tick[i] = []
        dict_key[i] = ""
        config.MULTISTOCK[i]={}
        config.MULTISTOCK[i]['ticks'] = {}
        handler[i] = logging.FileHandler(filename=config.STD_PATH+'logs/DataFetcher/DataFetcher-'+str(i)+'-'+timestr+'.log', mode='w')
        handler[i].setFormatter(formatter)
        LOG[i] = logging.getLogger("DataFetcher-"+str(i))
        LOG[i].setLevel(logging.INFO)
        LOG[i].addHandler(handler[i])

    kws = KiteTicker(config.API_KEY, config.ACCESS_TOKEN, config.CLIENT_ID)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect()

"""
if __name__ == '__main__':
    main()
    pass
"""
