'''
Created on 17-Sep-2017

@author: suhaheer
'''

import pdb

import config as CONFIG
import time
import datetime
import threading
import ExchangeInterface
import threading
from threading import Timer
import logging
import math


class SuperTrend():
    """
    Supertrend calculation:

    ATR (Average True Range)
    ```````````````````````````````````````````````````````````````````````````
    TR = Max{
            (Current_high - Current_low),
            (Current_high - Previous_close),
            (Current_low - Previous_close)
            }

    ATR_Period = 7;

    ATR[t] = (ATR[t-1]*(n-1)x(TR[t]))/n
    ```````````````````````````````````````````````````````````````````````````
    Multiplier = 3;

    BASIC UPPERBAND  =  ((HIGH + LOW) / 2) + (Multiplier * ATR)
    BASIC LOWERBAND =  ((HIGH + LOW) / 2) - (Multiplier * ATR)

    FINAL UPPERBAND = IF( (Current BASICUPPERBAND  < Previous FINAL UPPERBAND) and (Previous Close > Previous FINAL UPPERBAND)) THEN (Current BASIC UPPERBAND) ELSE Previous FINALUPPERBAND)

    FINAL LOWERBAND = IF( (Current BASIC LOWERBAND  > Previous FINAL LOWERBAND) and (Previous Close < Previous FINAL LOWERBAND)) THEN (Current BASIC LOWERBAND) ELSE Previous FINAL LOWERBAND)

    SUPERTREND = IF(Current Close <= Current FINAL UPPERBAND ) THEN Current FINAL UPPERBAND ELSE Current  FINAL LOWERBAND
    """

    """
    We have data in the format of prices [High, Low, Close]
    """
    LOG = None
    TREND = {}
    temp_avg = {}

    true_range = []
    average_TR = []

    BUY_LIST = {}
    SELL_LIST = {}

    upper = {}
    lower = {}

    """
    Immediate next candle in uptrend and immediate next candle in downtrend. 
    timestamp as key and a flag:TRUE/FALSE, to signify if next candle is set?
    
    when there is huge volatility in the market at the time of candle closing time, then although proper supertrend is set
    for next candle, at the beginnig of the next candle due to local and cur upper/lower bound differences, supertrend may be 
    immediately set to a location which has high tendency of cancelling the trade.
    
    The vars are unset right after passing the already set timestamp.
    """
    up_nxt_candle = {}
    down_nxt_candle = {}

    mutex_lock = False
    prev_supertrend_list = {}

    def __init__(self):
        self.tick_1min = CONFIG.tick
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Supertrend-' + timestr + '.log', mode='w')
        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("supertrend")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        pass

    def maximum(self, a, b, c):
        local_max = None

        if (a > b):
            local_max = a
        else:
            local_max = b

        if (c > local_max):
            return c
        else:
            return local_max

        pass

    """
    TR = Max{abs(High-Low), abs(High-Close_prev), abs(Low-Close_prev)}
    """
    def TrueRange(self, cur_time):
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        """
        This is the case where tick is yet to form at this second but TR has advanced in time.
        Hence keep the TR[cur_idx]=0 for this second. for every second in future, TR[cur_idx]
        will be updated to the correct value.
        """
        if (cur_idx) not in CONFIG.tick.keys():
            CONFIG.TRUE_RANGE[cur_idx] = 0
            return

        if len(CONFIG.tick)<=1:
            temp = (CONFIG.tick[cur_idx][1] - CONFIG.tick[cur_idx][2])
            CONFIG.TRUE_RANGE[cur_idx]=(float(format(temp, '.2f')))
            self.LOG.info("The First TR:%s, at:%s",CONFIG.TRUE_RANGE,cur_idx)

        else:
            prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
            prev_idx = str(prev_mkt_idx)
            self.LOG.info("Prev:%s, Cur:%s",prev_idx, cur_idx)

            if (prev_idx) not in CONFIG.tick.keys():
                self.LOG.info("Prev_idx:%s, Not found in tick. Update cur TR to prev TR.",prev_idx)
                if (prev_idx) in CONFIG.TRUE_RANGE.keys():
                    CONFIG.TRUE_RANGE[cur_idx] = CONFIG.TRUE_RANGE[prev_idx]
                return

            temp = self.maximum(abs(CONFIG.tick[cur_idx][1] - CONFIG.tick[cur_idx][2]),
                               abs(CONFIG.tick[cur_idx][1] - CONFIG.tick[prev_idx][3]),
                               abs(CONFIG.tick[cur_idx][2] - CONFIG.tick[prev_idx][3])
                               )
            CONFIG.TRUE_RANGE[cur_idx]=(float(format(temp, '.2f')))
            self.LOG.info("TrueRange:%s, at time:%s",CONFIG.TRUE_RANGE[cur_idx],cur_idx)
        return

    def Average_TR(self, cur_time):
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        """
        This is the case where tick is yet to form at this second but TR/ATR has advanced in time.
        Hence keep the TR[cur_idx]=0 for this second. for every second in future, TR[cur_idx]
        will be updated to the correct value.
        """
        if cur_idx not in CONFIG.TRUE_RANGE.keys():
            CONFIG.TRUE_RANGE[cur_idx] = 0
            return

        if len(CONFIG.tick) < CONFIG.SUPER_PERIOD:
            CONFIG.AVG_TRUE_RANGE[cur_idx]=0
            self.LOG.info("AVG_TRUE_RANGE:%s, len:%d",CONFIG.AVG_TRUE_RANGE[cur_idx],len(CONFIG.TRUE_RANGE))
            return

        """
        1st ATR is Arithematic mean.
        """
        if len(CONFIG.tick) == CONFIG.SUPER_PERIOD:
            idx = 0
            loc_sum = 0
            mkt_idx = cur_mkt_idx
            while (idx < CONFIG.SUPER_PERIOD):
                cur = str(mkt_idx)
                loc_sum += CONFIG.TRUE_RANGE[cur]
                mkt_idx = mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
                idx = idx + 1
            temp = float(loc_sum) / float(CONFIG.SUPER_PERIOD)
            self.average_TR.append(float(format(temp, '.2f')))
            CONFIG.AVG_TRUE_RANGE[cur_idx] = float(format(temp, '.2f'))
            self.LOG.info("The first ATR:%s, at time:%s, len:%d", CONFIG.AVG_TRUE_RANGE[cur_idx],
                          cur_idx,len(CONFIG.TRUE_RANGE))
        else:
            if prev_idx not in CONFIG.AVG_TRUE_RANGE.keys():
                self.LOG.info("Prev_idx:%s, Not found in AVG_TRUE_RANGE.", prev_idx)
                self.LOG.info("TR:%s, ATR:%s",CONFIG.TRUE_RANGE, CONFIG.AVG_TRUE_RANGE)
                CONFIG.AVG_TRUE_RANGE[cur_idx] = (CONFIG.TRUE_RANGE[cur_idx]) / float(CONFIG.SUPER_PERIOD)
                return
            self.LOG.info("ATR ticks timing:Cur:%s, Prev:%s", cur_idx, prev_idx)
            cur_atr = float(CONFIG.AVG_TRUE_RANGE[prev_idx] * (CONFIG.SUPER_PERIOD - 1)
                       + CONFIG.TRUE_RANGE[cur_idx]) / float(CONFIG.SUPER_PERIOD)

            CONFIG.AVG_TRUE_RANGE[cur_idx] = (float(format(cur_atr, '.2f')))
            self.LOG.info("ATR:%s, len:%d", CONFIG.AVG_TRUE_RANGE[cur_idx],len(CONFIG.AVG_TRUE_RANGE))
            self.average_TR.append(float(format(cur_atr, '.2f')))

        return

    """
    Supertrend
    
    Upper = ((high + low / 2) + Multiplier * ATR Only if price < upper

    Lower = ((high + low / 2) - Multiplier * ATR Only if price > lower
    """
    def supertrend_util(self, cur_time):
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        next_mkt_idx = cur_mkt_idx + datetime.timedelta(minutes=CONFIG.time_interval)
        next_idx = str(next_mkt_idx)

        candle_beginning_time = cur_mkt_idx.replace(second=5)
        candle_closing_time = cur_mkt_idx.replace(minute=cur_min+(CONFIG.time_interval-1), second=57)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        """unset previously set up/down immediate next candles, because they are prev candles now.
        Ref Var declaration for more understanding.
        """
        if prev_idx in self.up_nxt_candle.keys():
            self.up_nxt_candle = {}
        if prev_idx in self.down_nxt_candle.keys():
            self.down_nxt_candle = {}

        if (len(CONFIG.AVG_TRUE_RANGE) < CONFIG.SUPER_PERIOD):
            self.LOG.error("ATR:%d is less than super_period:%d",len(CONFIG.AVG_TRUE_RANGE), CONFIG.SUPER_PERIOD)
            return

        if (cur_idx not in CONFIG.tick.keys()):
            self.LOG.error("cur_idx:%s, tick may not have formed yet. Skipping supertrend. The prev_idx:%s.", cur_idx,
                           prev_idx)
            self.LOG.error("Tick at Supertrend:%s", str(CONFIG.tick))
            return

        if (cur_idx not in CONFIG.SUPERTREND.keys()):
            self.LOG.error("cur_idx:%s, supertrend may not have formed yet. Skipping supertrend. The prev_idx:%s.", cur_idx,
                           prev_idx)
            self.LOG.error("Tick at Supertrend:%s", str(CONFIG.tick))
            self.LOG.error("Supertrend list:%s", CONFIG.SUPERTREND)
            CONFIG.SUPERTREND[cur_idx]=CONFIG.SUPERTREND[prev_idx]

        if (cur_idx not in self.TREND.keys()):
            """
            This is the only case where trend is still continuing to be UP/DOWN. During change in trend, 
            we are already creating the dictionary entry with 'next_idx'.
            """
            self.TREND[cur_idx] = self.TREND[prev_idx]

        redLine = CONFIG.SUPERTREND[cur_idx] + CONFIG.AVG_TRUE_RANGE[cur_idx]
        cur_upper_band = (float(format(redLine, '.2f')))

        greenLine = CONFIG.SUPERTREND[cur_idx] - CONFIG.AVG_TRUE_RANGE[cur_idx]
        cur_lower_band = (float(format(greenLine, '.2f')))


        if (CONFIG.tick[cur_idx][3] >= CONFIG.SUPERTREND[cur_idx]):
            if (self.TREND[cur_idx] == "DOWN"):
                if (candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    CONFIG.SUPERTREND[next_idx] = cur_lower_band
                    self.TREND[next_idx] = "UP"
                    self.up_nxt_candle[next_idx]=True
                    self.LOG.info("Trend changed from DOWN to UP. cur_supertrend=%f, next_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f",
                                  CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx],
                                  cur_lower_band, cur_upper_band)

                    if (cur_idx) not in self.BUY_LIST.keys():
                        self.local_placeorder("BUY", CONFIG.MARKET_LTP, cur_lower_band, cur_idx)  # FIXME #price
                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                elif (candle_beginning_time >= datetime.datetime.now().replace(microsecond=0)):
                    CONFIG.SUPERTREND[cur_idx] = cur_lower_band
                    self.TREND[cur_idx] = "UP"
                    self.LOG.info("Trend changed from DOWN to UP. prev_supertrend=%f, cur_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f",
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if (cur_idx) not in self.BUY_LIST.keys():
                        self.local_placeorder("BUY", CONFIG.MARKET_LTP, cur_lower_band, cur_idx)  # FIXME #price
                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                else:
                    self.LOG.info("Candle not closed yet. It is still Trend:%s",self.TREND[cur_idx])
                    self.LOG.info("MKT_LTP:%f is >= supertrend:%f", CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx])
                    return

            local_lower_band = (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 - CONFIG.AVG_TRUE_RANGE[cur_idx])
            local_lower_band = (float(format(local_lower_band, '.2f')))

            if (local_lower_band >= cur_upper_band) and \
                    (CONFIG.tick[cur_idx][3] > cur_upper_band) and \
                        (cur_idx not in self.up_nxt_candle.keys()):
                CONFIG.SUPERTREND[cur_idx] = cur_upper_band

            if (CONFIG.SUPERTREND[cur_idx] >= CONFIG.SUPERTREND[prev_idx]) and \
                    (candle_closing_time != datetime.datetime.now().replace(microsecond=0)):
                self.LOG.info("Running Up trend, Mkt_LTP:%f, cur_close:%f, supertrend:%f",
                          CONFIG.MARKET_LTP,
                          CONFIG.tick[cur_idx][3],
                          CONFIG.SUPERTREND[cur_idx])
            else:
                self.LOG.error("Irregular trend...MKT_LTP:%f is >= supertrend:%f. But cur-supertrend:%f < prev-supertrend:%f",
                               CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[cur_idx],
                               CONFIG.SUPERTREND[prev_idx])

            self.TREND[cur_idx] = "UP"

            '''
            if (candle_closing_time == datetime.datetime.now().replace(microsecond=0)) and \
                    (0<=(CONFIG.tick[cur_idx][3] - CONFIG.SUPERTREND[cur_idx])<CONFIG.PIP) and \
                    self.identify_trend(cur_time)==CONFIG.UPTREND:
                self.LOG.info("BUY_SIGNAL cur close:%f, crossed above supertrend:%f, at:%s. stoploss:%f, ATR:%f, LTP:%f",
                              CONFIG.tick[cur_idx][3], CONFIG.SUPERTREND[cur_idx], datetime.datetime.now(),
                              cur_lower_band, CONFIG.AVG_TRUE_RANGE[cur_idx], CONFIG.MARKET_LTP)
                if (cur_idx) not in self.BUY_LIST.keys():
                    self.local_placeorder("BUY", CONFIG.MARKET_LTP, cur_lower_band, cur_idx) #FIXME #price
                else:
                    self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                """
                At the candle closing time, we see, new long supertrend has formed. Right now, the time is last 
                second of the candle, so update the cur_idx, so that, from next candle can start from the newly updated 
                value.
                """
                CONFIG.SUPERTREND[cur_idx] = cur_lower_band
            '''

        elif CONFIG.tick[cur_idx][3] < CONFIG.SUPERTREND[cur_idx]:
            if (self.TREND[cur_idx] == "UP"):
                if (candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    CONFIG.SUPERTREND[next_idx] = cur_upper_band
                    self.TREND[next_idx] = "DOWN"
                    self.down_nxt_candle[next_idx] = True
                    self.LOG.info("Trend changed from UP to DOWN. cur_supertrend=%f, next_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f",
                                  CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx],
                                  cur_lower_band, cur_upper_band)

                    if (cur_idx) not in self.SELL_LIST.keys():
                        self.local_placeorder("SELL", CONFIG.MARKET_LTP, cur_upper_band, cur_idx)  # FIXME #price
                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                elif (candle_beginning_time >= datetime.datetime.now().replace(microsecond=0)):
                    CONFIG.SUPERTREND[cur_idx] = cur_upper_band
                    self.TREND[cur_idx] = "DOWN"
                    self.LOG.info("Trend changed from UP to DOWN. prev_supertrend=%f, cur_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f",
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if (cur_idx) not in self.SELL_LIST.keys():
                        self.local_placeorder("SELL", CONFIG.MARKET_LTP, cur_upper_band, cur_idx)  # FIXME #price
                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                else:
                    self.LOG.info("Candle not closed yet. It is still Trend:%s",self.TREND[cur_idx])
                    self.LOG.info("MKT_LTP:%f is < supertrend:%f", CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx])
                    return

            local_upper_band = (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 + CONFIG.AVG_TRUE_RANGE[cur_idx])
            local_upper_band = (float(format(local_upper_band, '.2f')))

            if (local_upper_band <= cur_lower_band) and \
                    (CONFIG.tick[cur_idx][3] < cur_lower_band) and \
                        (cur_idx not in self.down_nxt_candle.keys()):
                CONFIG.SUPERTREND[cur_idx] = cur_lower_band

            if (CONFIG.SUPERTREND[cur_idx] <= CONFIG.SUPERTREND[prev_idx]) and \
                    (candle_closing_time != datetime.datetime.now().replace(microsecond=0)):
                self.LOG.info("Running Down trend, Mkt_LTP:%f, cur_close:%f, supertrend:%f",
                          CONFIG.MARKET_LTP,
                          CONFIG.tick[cur_idx][3],
                          CONFIG.SUPERTREND[cur_idx])
            else:
                self.LOG.error("Irregular trend...MKT_LTP:%f is <= supertrend:%f. But cur-supertrend:%f > prev-supertrend:%f",
                               CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[cur_idx],
                               CONFIG.SUPERTREND[prev_idx])

            self.TREND[cur_idx] = "DOWN"

            '''
            if candle_closing_time == datetime.datetime.now().replace(microsecond=0) and \
                    (-1*CONFIG.PIP<(CONFIG.tick[cur_idx][3] - CONFIG.SUPERTREND[cur_idx])<=0) and \
                    self.identify_trend(cur_time)==CONFIG.DOWNTREND:
                self.LOG.info("SELL_SIGNAL cur close:%f, crossed below supertrend:%f, at:%s. stoploss:%f, ATR:%f, LTP:%f",
                            CONFIG.tick[cur_idx][3], CONFIG.SUPERTREND[cur_idx], datetime.datetime.now(),
                            cur_upper_band, CONFIG.AVG_TRUE_RANGE[cur_idx], CONFIG.MARKET_LTP)
                if (cur_idx) not in self.SELL_LIST.keys():
                    self.local_placeorder("SELL", CONFIG.MARKET_LTP, cur_upper_band, cur_idx) #FIXME #price
                else:
                    self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)
                """
                At the candle closing time, we see, new short supertrend has formed. Right now, the time is last 
                second of the candle, so update the cur_idx, so that, from next candle can start from the newly updated 
                value.
                """
                CONFIG.SUPERTREND[cur_idx] = cur_upper_band
            '''

        return

    """
    Function to identify the trend.
    """
    def identify_trend(self, cur_time):
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        another_prev = prev_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        another_prev_idx = str(another_prev)

        if (CONFIG.tick[another_prev_idx][3] > CONFIG.SUPERTREND[another_prev_idx] and
            CONFIG.tick[prev_idx][3] > CONFIG.SUPERTREND[prev_idx] and
            CONFIG.tick[cur_idx][3] < CONFIG.SUPERTREND[cur_idx]):
            self.LOG.info("catching the crossing points. Its Downtrend")
            return CONFIG.DOWNTREND

        if (CONFIG.tick[another_prev_idx][3] < CONFIG.SUPERTREND[another_prev_idx] and
                    CONFIG.tick[prev_idx][3] < CONFIG.SUPERTREND[prev_idx] and
                    CONFIG.tick[cur_idx][3] > CONFIG.SUPERTREND[cur_idx]):
            self.LOG.info("catching the crossing points. Its Uptrend")
            return CONFIG.UPTREND

        return

    """ 
    Function to place order
    """
    def local_placeorder(self, ORDER_TYPE, mkt_price, stoploss, cur_idx):
        dt = datetime.datetime.now()
        order = {}
        #oid = int(dt.strftime("%s"))
        oid = int(time.mktime(dt.timetuple()))
        if ORDER_TYPE=="BUY":
            order["order_id"] = oid
            order["timestamp"] = str(dt)
            order["type"] = "BUY"
            order["variety"] = "BO"
            order["trading_sym"] = CONFIG.trading_symbol
            order["buy_price"] = mkt_price
            order["target"] = math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx])
            order["stoploss"] = math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx]*3)
            self.BUY_LIST[cur_idx] = order
            self.LOG.info("Buy order placed successfully order details: %s", str(order))
            ExchangeInterface.placebracketorder(CONFIG.trading_quantity, "BUY", order["buy_price"],
                                                order["target"], order["stoploss"])

        order = {}
        if ORDER_TYPE == "SELL":
            order["order_id"] = oid
            order["timestamp"] = str(dt)
            order["type"] = "SELL"
            order["variety"] = "BO"
            order["trading_sym"] = CONFIG.trading_symbol
            order["sell_price"] = mkt_price
            order["target"] = math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx])
            order["stoploss"] = math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx]*3)
            self.SELL_LIST[cur_idx] = order
            self.LOG.info("Sell order placed successfully order details: %s", str(order))
            ExchangeInterface.placebracketorder(CONFIG.trading_quantity, "SELL", order["sell_price"],
                                                order["target"], order["stoploss"])

        return

    def main_supertrend(self):

        if (not len(CONFIG.tick)):
            self.LOG.error("Tick is empty")
            return

        cur_time = datetime.datetime.now().replace(microsecond=0)

        self.TrueRange(cur_time)
        self.Average_TR(cur_time)

        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)
        if prev_idx not in self.temp_avg.keys():
            self.temp_avg[prev_idx]=0

        if (len(CONFIG.SUPERTREND) < CONFIG.SUPER_PERIOD):
            if (cur_idx not in CONFIG.tick.keys()):
                self.LOG.error("cur_idx:%s, may not have formed yet. Skipping for current second", cur_time)
                self.LOG.error("Tick list:%s", CONFIG.tick)
            else:
                self.LOG.info("Supertrend length=%d is less than SUPER_PERIOD=%d",len(CONFIG.SUPERTREND), CONFIG.SUPER_PERIOD)
                self.upper[cur_idx] = CONFIG.tick[cur_idx][3]
                self.lower[cur_idx] = CONFIG.tick[cur_idx][3]
                self.temp_avg[cur_idx] = float(CONFIG.tick[cur_idx][1]+CONFIG.tick[cur_idx][2])/2.0
                CONFIG.SUPERTREND[cur_idx] = self.temp_avg[cur_idx]
                if self.temp_avg[cur_idx] > self.temp_avg[prev_idx]:
                    self.TREND[cur_idx] = "UP"
                elif self.temp_avg[cur_idx] < self.temp_avg[prev_idx]:
                    self.TREND[cur_idx] = "DOWN"

                if CONFIG.SUPERTREND_STARTING_TIME==None:
                    CONFIG.SUPERTREND_STARTING_TIME = cur_idx

        else:
            self.supertrend_util(cur_time)
        return

def main():
    obj = SuperTrend()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                      microsecond=0)
    while True:
        Timer(1, obj.main_supertrend,[]).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (market_end_time == datetime.datetime.now().replace(microsecond=0)):
                print("The market has closed... ")
                break

    return
