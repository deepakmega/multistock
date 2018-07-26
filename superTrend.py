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
import os


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
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/Supertrend-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("supertrend")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

        self.TREND = CONFIG.SUPERTREND_TREND
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

        if len(CONFIG.tick) <= 1:
            temp = (CONFIG.tick[cur_idx][1] - CONFIG.tick[cur_idx][2])
            CONFIG.TRUE_RANGE[cur_idx] = (float(format(temp, '.2f')))
            self.LOG.info("The First TR:%s, at:%s", CONFIG.TRUE_RANGE, cur_idx)

        else:
            prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
            prev_idx = str(prev_mkt_idx)
            self.LOG.info("Prev:%s, Cur:%s", prev_idx, cur_idx)

            if (prev_idx) not in CONFIG.tick.keys():
                self.LOG.info("Prev_idx:%s, Not found in tick. Update cur TR to prev TR.", prev_idx)
                if (prev_idx) in CONFIG.TRUE_RANGE.keys():
                    CONFIG.TRUE_RANGE[cur_idx] = CONFIG.TRUE_RANGE[prev_idx]
                return

            temp = self.maximum(abs(CONFIG.tick[cur_idx][1] - CONFIG.tick[cur_idx][2]),
                                abs(CONFIG.tick[cur_idx][1] - CONFIG.tick[prev_idx][3]),
                                abs(CONFIG.tick[cur_idx][2] - CONFIG.tick[prev_idx][3])
                                )
            CONFIG.TRUE_RANGE[cur_idx] = (float(format(temp, '.2f')))
            self.LOG.info("TrueRange:%s, at time:%s", CONFIG.TRUE_RANGE[cur_idx], cur_idx)

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
            CONFIG.AVG_TRUE_RANGE[cur_idx] = 0
            self.LOG.info("AVG_TRUE_RANGE:%s, len:%d", CONFIG.AVG_TRUE_RANGE[cur_idx], len(CONFIG.TRUE_RANGE))
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
                          cur_idx, len(CONFIG.TRUE_RANGE))
        else:
            if prev_idx not in CONFIG.AVG_TRUE_RANGE.keys():
                self.LOG.info("Prev_idx:%s, Not found in AVG_TRUE_RANGE.", prev_idx)
                self.LOG.info("TR:%s, ATR:%s", CONFIG.TRUE_RANGE, CONFIG.AVG_TRUE_RANGE)
                CONFIG.AVG_TRUE_RANGE[cur_idx] = (CONFIG.TRUE_RANGE[cur_idx]) / float(CONFIG.SUPER_PERIOD)
                return
            self.LOG.info("ATR ticks timing:Cur:%s, Prev:%s", cur_idx, prev_idx)
            cur_atr = float(CONFIG.AVG_TRUE_RANGE[prev_idx] * (CONFIG.SUPER_PERIOD - 1)
                            + CONFIG.TRUE_RANGE[cur_idx]) / float(CONFIG.SUPER_PERIOD)

            CONFIG.AVG_TRUE_RANGE[cur_idx] = (float(format(cur_atr, '.2f')))
            if CONFIG.trading_exchange == "MCX":
                if CONFIG.AVG_TRUE_RANGE[cur_idx] > (2 * CONFIG.COMMODITY_PROFIT_MARGIN):
                    CONFIG.AVG_TRUE_RANGE[cur_idx] = (2 * CONFIG.COMMODITY_PROFIT_MARGIN - 1)

            self.LOG.info("ATR:%s, len:%d", CONFIG.AVG_TRUE_RANGE[cur_idx], len(CONFIG.AVG_TRUE_RANGE))
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

        candle_beginning_time = cur_mkt_idx.replace(second=10)
        candle_closing_time = cur_mkt_idx.replace(minute=cur_min + (CONFIG.time_interval - 1), second=50)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        prev_prev_mkt_idx = prev_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_prev_idx = str(prev_prev_mkt_idx)

        """unset previously set up/down immediate next candles, because they are prev candles now.
        Ref Var declaration for more understanding.
        """
        if prev_idx in self.up_nxt_candle.keys():
            self.up_nxt_candle = {}
        if prev_idx in self.down_nxt_candle.keys():
            self.down_nxt_candle = {}

        if (len(CONFIG.AVG_TRUE_RANGE) < CONFIG.SUPER_PERIOD):
            self.LOG.error("ATR:%d is less than super_period:%d", len(CONFIG.AVG_TRUE_RANGE), CONFIG.SUPER_PERIOD)
            return

        if (cur_idx not in CONFIG.tick.keys()):
            self.LOG.error("cur_idx:%s, tick may not have formed yet. Skipping supertrend. The prev_idx:%s.", cur_idx,
                           prev_idx)
            self.LOG.error("Tick at Supertrend:%s", str(CONFIG.tick))
            return

        if (cur_idx not in CONFIG.SUPERTREND.keys()):
            self.LOG.error("cur_idx:%s, supertrend may not have formed yet. Skipping supertrend. The prev_idx:%s.",
                           cur_idx, prev_idx)
            self.LOG.error("Tick at Supertrend:%s", str(CONFIG.tick))
            self.LOG.error("Supertrend list:%s", CONFIG.SUPERTREND)
            CONFIG.SUPERTREND[cur_idx] = CONFIG.SUPERTREND[prev_idx]

        optimal_supertrend_price = CONFIG.SUPERTREND[prev_idx]
        if (prev_idx not in self.TREND.keys()):
            """Sometimes its possible that Trend[prev] is missing."""
            self.TREND[prev_idx] = self.TREND.get(prev_prev_idx)

        if (cur_idx not in self.TREND.keys()):
            """Sometimes its possible that Trend[cur] is missing"""
            self.TREND[cur_idx] = self.TREND.get(prev_idx)

        if cur_idx not in CONFIG.LINEAR_REGRESSION_PREDICTOR.keys():
            CONFIG.LINEAR_REGRESSION_PREDICTOR[cur_idx] = CONFIG.LINEAR_REGRESSION.get(cur_idx)

        # redLine =  (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 + CONFIG.AVG_TRUE_RANGE[cur_idx]*CONFIG.SUPER_MULTIPLIER)
        redLine = (float(CONFIG.SUPERTREND[prev_idx]) + CONFIG.AVG_TRUE_RANGE[cur_idx])  # * CONFIG.SUPER_MULTIPLIER)
        cur_upper_band = (float(format(redLine, '.2f')))
        super_upper_band = cur_upper_band + CONFIG.AVG_TRUE_RANGE[cur_idx]/2.0
        super_upper_band = (float(format(super_upper_band, '.2f')))

        # greenLine = (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 - CONFIG.AVG_TRUE_RANGE[cur_idx]*CONFIG.SUPER_MULTIPLIER)
        greenLine = (float(CONFIG.SUPERTREND[prev_idx]) - CONFIG.AVG_TRUE_RANGE[cur_idx])  # * CONFIG.SUPER_MULTIPLIER)
        cur_lower_band = (float(format(greenLine, '.2f')))
        super_lower_band = cur_lower_band - CONFIG.AVG_TRUE_RANGE[cur_idx]/2.0
        super_lower_band = (float(format(super_lower_band, '.2f')))

        """
        The additional check of TREND[prev] being up/down, allows system to execute trade only during supertrend 
        cross over. For multiple entries during any particular trend needs complete different logic.
        """
        if (CONFIG.tick[cur_idx][3] > CONFIG.SUPERTREND[cur_idx]):
            if (self.TREND[prev_idx] == "DOWN"):
                if (candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    """7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                    price is above or below median value.
                    
                    During 7th candle closing time, its safe to assume uptrend as mkt_price/close > supertrend.
                    """
                    if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD) and self.TREND[cur_idx] == "DOWN":
                        CONFIG.SUPERTREND[cur_idx] = super_lower_band
                        self.TREND[cur_idx] = "UP"
                        self.LOG.info(
                            "Trend changed from DOWN to UP at candle:%d. close=%f, prev_supertrend=%f, cur_supertrend=%f, "
                            "cur_lower_band=%f, cur_upper_band=%f", len(CONFIG.SUPERTREND), CONFIG.tick[cur_idx][3],
                            CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                            cur_lower_band, cur_upper_band)

                        if ((cur_idx) not in self.BUY_LIST.keys()):
                            self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                                  cur_lower_band, cur_idx)
                            self.LOG.info("Buy order placed at LR:%f",
                                          CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))

                        else:
                            self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)

                        return

                    """
                    Candle[prev] was below supertrend[cur]. Candle[cur] is above supertrend at the closing time. The 
                    LTP/close is greater than supertrend[cur]. Hence, its a buy/long signal. Since it is closing time of
                    Candle[cur], update the supertrend at cur closing time and updation should reflect in next candle also.
                    """
                    CONFIG.SUPERTREND[next_idx] = CONFIG.SUPERTREND[cur_idx]
                    if (self.TREND.get(cur_idx, None) == "DOWN"):
                        CONFIG.SUPERTREND[cur_idx] = super_lower_band
                        if (super_lower_band > CONFIG.SUPERTREND[next_idx]):  #FIXME: not sure which case it is?
                            CONFIG.SUPERTREND[next_idx] = super_lower_band
                            self.LOG.info(
                                "At Uptrend, Updated from supertrend:%f, to supertrend:%f, cur_lower_bound:%f",
                                CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx], cur_lower_band)
                        else:
                            """most probably above case fails, so next candle will be update!
                            """
                            CONFIG.SUPERTREND[next_idx] = CONFIG.SUPERTREND[cur_idx]
                            self.LOG.info(
                                "At Uptrend, cur supertrend:%f, will be continued to next supertrend:%f, cur_lower_bound:%f",
                                CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx], cur_lower_band)

                    self.TREND[next_idx] = "UP"
                    self.up_nxt_candle[next_idx] = True
                    self.LOG.info("Trend changed from DOWN to UP. close:%f, cur_supertrend=%f, next_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.BUY_LIST.keys()):
                        self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_lower_band, cur_idx)
                        self.LOG.info("Buy order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))
                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed ", cur_idx)

                    return

                elif (candle_beginning_time >= datetime.datetime.now().replace(microsecond=0)):
                    """
                    Candle[prev] was below supertrend[cur]. The new candle[cur] has opened above supertrend[cur]. Its
                    buy/long signal.
                    """
                    if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD):
                        """
                        7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                        price is above or below median value.
                    
                        Wait till close of 7th candle and then, its safe to assume uptrend as mkt_price/close > supertrend.
                        """
                        self.LOG.info("Close=%f > supertrend=%f at candle=%d. Wait till close of Candle. Continue with prev trend=%s",
                                      CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], len(CONFIG.SUPERTREND), self.TREND[prev_idx])
                        self.TREND[cur_idx] = self.TREND[prev_idx]
                        return

                    """
                    Candle[prev] was below supertrend[cur]. The new candle[cur] has opened above supertrend[cur]. Its
                    long/buy indication.
                    If the buy signal was already generated at the Candle[prev] close, then its better to 
                    continue with whatever supertrend[cur] has, cause in the prev candle the supertrend[next] was already
                    updated. The best way I could think to check if the long/buy already happened, check the buy_list
                    for prev idx and then decide if we need to update the supertrend[cur] to new super_upper_band.
                    """
                    if prev_idx not in self.BUY_LIST:
                        CONFIG.SUPERTREND[cur_idx] = super_lower_band
                    """else: continue with whatever supertrend[cur] has. Remember it was already update prev candle 
                    closing time.
                    """
                    self.TREND[cur_idx] = "UP"
                    self.LOG.info("Trend changed from DOWN to UP. close:%f, prev_supertrend=%f, cur_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.BUY_LIST.keys()):
                        self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_lower_band, cur_idx)
                        self.LOG.info("Buy order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))

                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                else:
                    self.LOG.info("Candle not closed yet. It is still Trend:%s", self.TREND[cur_idx])
                    self.LOG.info("MKT_LTP:%f is >= supertrend:%f", CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx])
                    return

            elif (self.TREND.get(prev_idx, None) == "UP"):
                """
                The Candle[prev] close is above supertrend[cur]. The market might have tried to reverse or still running
                uptrend. Apart from 7th candle, no other candle finds any significance here!
                """
                if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD and \
                                candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    """7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                    price is above or below median value.

                    During 7th candle closing time, its safe to assume uptrend as mkt_price/close > supertrend.
                    """
                    CONFIG.SUPERTREND[cur_idx] = super_lower_band
                    self.TREND[cur_idx] = "UP"
                    self.LOG.info("Trend changed from DOWN to UP at the 7th candle. Close:%f, prev_supertrend=%f, "
                                  "cur_supertrend=%f, cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.BUY_LIST.keys()):
                        self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_lower_band, cur_idx)
                        self.LOG.info("Buy order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))

                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                    return

                    """Trend[prev] is UP and the Trend[cur] was running DOWN. But by end of the candle, the LTP is more 
                    than Supertrend[cur]. If this condition is not handled, then Supertrend will still do up due to next
                    sequence of programming. In reality, it should have been long buy.
                    """
                elif (self.TREND.get(cur_idx, None) == "DOWN"):
                    CONFIG.SUPERTREND[cur_idx] = super_lower_band
                    self.TREND[cur_idx] = "UP"
                    self.LOG.info("Trend changed from DOWN to UP at the 7th candle. Close:%f, prev_supertrend=%f, "
                              "cur_supertrend=%f, cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                              CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                              cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.BUY_LIST.keys()):
                        self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                          CONFIG.AVG_TRUE_RANGE[cur_idx] * 10, cur_idx)
                        self.LOG.info("Buy order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))

                    else:
                        self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
                    return

            """
            local_lower_band = (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 - CONFIG.AVG_TRUE_RANGE[cur_idx])
            local_lower_band = (float(format(local_lower_band, '.2f')))
            """
            """ Either local_lower_band is sufficiently lower than cur_upper_band or there should be at least 1 ATR 
                distance of High and Low.
            """
            local_lower_band = (float(CONFIG.tick[cur_idx][2]) - CONFIG.AVG_TRUE_RANGE[cur_idx] )
            local_lower_band = (float(format(local_lower_band, '.2f')))
            if ((local_lower_band >= cur_upper_band) and \
                    (local_lower_band - cur_upper_band) > (1) and \
                        (CONFIG.tick[cur_idx][3] > cur_upper_band) and \
                            (cur_idx not in self.up_nxt_candle.keys())):
                CONFIG.SUPERTREND[cur_idx] = cur_upper_band
                self.LOG.info("At Uptrend, Updated the supertrend to:%f, cur_lower_bound:%f, local_upper_bound:%f",
                              CONFIG.SUPERTREND[cur_idx], cur_upper_band, local_lower_band)

                if ((cur_idx) not in self.BUY_LIST.keys()):
                    self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                          cur_lower_band, cur_idx)
                    self.LOG.info("Buy order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))
                else:
                    self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
            """
            elif ((abs(CONFIG.tick[cur_idx][3] - CONFIG.tick[cur_idx][2])) > CONFIG.AVG_TRUE_RANGE[cur_idx]):
                self.LOG.info("At Uptrend, Close - low is 1ATR far distance. Place 1 buy order")
                if ((cur_idx) not in self.BUY_LIST.keys()):
                    self.local_placeorder("BUY", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                          cur_lower_band, cur_idx)
                    self.LOG.info("Buy order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))
                else:
                    self.LOG.error("Buy Order for this timestamp:%s might have been already placed", cur_idx)
            """
            if (CONFIG.SUPERTREND[cur_idx] >= CONFIG.SUPERTREND[prev_idx]) and \
                    (candle_closing_time != datetime.datetime.now().replace(microsecond=0)):
                self.LOG.info("Running Up trend, Mkt_LTP:%f, cur_close:%f, supertrend:%f",
                              CONFIG.MARKET_LTP,
                              CONFIG.tick[cur_idx][3],
                              CONFIG.SUPERTREND[cur_idx])
            else:
                self.LOG.error(
                    "Irregular trend...MKT_LTP:%f is >= supertrend:%f. But cur-supertrend:%f < prev-supertrend:%f",
                    CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[cur_idx],
                    CONFIG.SUPERTREND[prev_idx])

            self.TREND[cur_idx] = "UP"
            CONFIG.SUPERTREND_TREND[cur_idx] = self.TREND.get(cur_idx)

        elif CONFIG.tick[cur_idx][3] < CONFIG.SUPERTREND[cur_idx]:
            if (self.TREND[prev_idx] == "UP"):
                if (candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    """7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                    price is above or below median value.

                    During 7th candle closing time, its safe to assume downtrend as mkt_price/close < supertrend.
                    """
                    if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD) and self.TREND[cur_idx] == "UP":
                        CONFIG.SUPERTREND[cur_idx] = super_upper_band
                        self.TREND[cur_idx] = "DOWN"
                        self.LOG.info(
                            "Trend changed from UP to DOWN at candle:%d. close:%f, prev_supertrend=%f, cur_supertrend=%f, "
                            "cur_lower_band=%f, cur_upper_band=%f", len(CONFIG.SUPERTREND), CONFIG.tick[cur_idx][3],
                            CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                            cur_lower_band, cur_upper_band)

                        if ((cur_idx) not in self.SELL_LIST.keys()):
                            self.local_placeorder("SELL", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                                  cur_upper_band, cur_idx)
                            self.LOG.info("Sell order placed at LR:%f",
                                          CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))

                        else:
                            self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)

                        return
                    """
                    Candle[prev] was above supertrend[cur]. Candle[cur] is below supertrend at the closing time. The 
                    LTP/close is less than supertrend[cur]. Hence, its a short sell signal. Since it is closing time of
                    Candle[cur], update the supertrend at cur closing time and updation should reflect in next candle also.
                    """
                    CONFIG.SUPERTREND[next_idx] = CONFIG.SUPERTREND[cur_idx]
                    if (self.TREND.get(cur_idx, None) == "UP"):
                        CONFIG.SUPERTREND[cur_idx] = super_upper_band
                        if (super_upper_band < CONFIG.SUPERTREND[next_idx]):
                            CONFIG.SUPERTREND[next_idx] = super_upper_band
                            self.LOG.info(
                                "At Downtrend, Updated from supertrend:%f, to supertrend:%f, cur_upper_bound:%f",
                                CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx], cur_upper_band)
                        else:
                            CONFIG.SUPERTREND[next_idx] = CONFIG.SUPERTREND[cur_idx]
                            self.LOG.info(
                                "At Downtrend, cur supertrend:%f, will be continued to next supertrend:%f, cur_upper_bound:%f",
                                CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx], cur_upper_band)

                    self.TREND[next_idx] = "DOWN"
                    self.down_nxt_candle[next_idx] = True
                    self.LOG.info("Trend changed from UP to DOWN. close=%f, cur_supertrend=%f, next_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[next_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.SELL_LIST.keys()):
                        self.local_placeorder("SELL",CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_upper_band, cur_idx)
                        self.LOG.info("sell order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))
                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)

                    return
                elif (candle_beginning_time >= datetime.datetime.now().replace(microsecond=0)):

                    if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD):
                        """
                        7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                        price is above or below median value.

                        Wait till close of 7th candle and then, its safe to assume uptrend as mkt_price/close > supertrend.
                        """
                        self.LOG.info(
                            "Close=%f < supertrend=%f at candle=%d. Wait till close of Candle. Continue with prev trend=%s",
                            CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], len(CONFIG.SUPERTREND), self.TREND[prev_idx])
                        self.TREND[cur_idx] = self.TREND[prev_idx]
                        return

                    """
                    Candle[prev] was above supertrend[cur]. The new candle[cur] has opened below supertrend[cur]. Its
                    short sell indication.
                    If the short sell signal was already generated at the Candle[prev] close, then its better to 
                    continue with whatever supertrend[cur] has, cause in the prev candle the supertrend[next] was already
                    updated. The best way I could think to check if the shortsell already happened, check the sell_list
                    for prev idx and then decide if we need to update the supertrend[cur] to new super_upper_band.
                    """
                    if prev_idx not in self.SELL_LIST:
                        CONFIG.SUPERTREND[cur_idx] = super_upper_band
                    """else: continue with whatever supertrend[cur] has. Remember it was already update prev candle 
                        closing time.
                    """
                    self.TREND[cur_idx] = "DOWN"
                    self.LOG.info("Trend changed from UP to DOWN. close=%f, prev_supertrend=%f, cur_supertrend=%f, "
                                  "cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.SELL_LIST.keys()):
                        self.local_placeorder("SELL", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_upper_band, cur_idx)
                        self.LOG.info("sell order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))

                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed", cur_idx)
                    return
                else:
                    self.LOG.info("Candle not closed yet. It is still Trend:%s", self.TREND[cur_idx])
                    self.LOG.info("MKT_LTP:%f is < supertrend:%f", CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx])
                    return

            elif (self.TREND.get(prev_idx, None) == "DOWN"):
                """
                The Candle[prev] close is below supertrend[cur]. The market might have tried to reverse or still running
                downtrend. Apart from 7th candle, no other candle finds any significance here!
                """
                if (len(CONFIG.SUPERTREND) <= CONFIG.SUPER_PERIOD and \
                            candle_closing_time <= datetime.datetime.now().replace(microsecond=0)):
                    """
                    7th candle: last 6 candle's median value was taken as supertrend and called UP/DOWN trend if mkt 
                    price is above or below median value.

                    Wait till close of 7th candle and then, its safe to assume uptrend as mkt_price/close > supertrend.
                    """
                    CONFIG.SUPERTREND[cur_idx] = super_upper_band
                    self.TREND[cur_idx] = "DOWN"
                    self.LOG.info("Trend changed from UP to DOWN at 7th candle. close=%f, prev_supertrend=%f, "
                                  "cur_supertrend=%f, cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                                  CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                                  cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.SELL_LIST.keys()):
                        self.local_placeorder("SELL", CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                              cur_upper_band, cur_idx)
                        self.LOG.info("sell order placed at LR:%f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))
                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed",cur_idx)

                    return

                    """Trend[prev] is DOWN and the Trend[cur] was running UP. But by end of the candle, the LTP is less 
                    than Supertrend[cur]. If this condition is not handled, then Supertrend will still do down due to next
                    sequence of programming. In reality, it should have been shortsell.
                    """
                elif (self.TREND.get(cur_idx, None) == "UP"):
                    CONFIG.SUPERTREND[cur_idx] = super_upper_band
                    self.TREND[cur_idx] = "DOWN"
                    self.LOG.info("Trend changed from UP to DOWN at 7th candle. close=%f, prev_supertrend=%f, "
                              "cur_supertrend=%f, cur_lower_band=%f, cur_upper_band=%f", CONFIG.tick[cur_idx][3],
                              CONFIG.SUPERTREND[prev_idx], CONFIG.SUPERTREND[cur_idx],
                              cur_lower_band, cur_upper_band)

                    if ((cur_idx) not in self.SELL_LIST.keys()):
                        self.local_placeorder("SELL",
                                          CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP),
                                          CONFIG.AVG_TRUE_RANGE[cur_idx] * 10, cur_idx)
                        self.LOG.info("sell order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION_PREDICTOR.get(cur_idx, CONFIG.MARKET_LTP))

                    else:
                        self.LOG.error("Sell Order for this timestamp:%s might have been already placed",cur_idx)

                    return

            """
            local_upper_band = (float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0 + CONFIG.AVG_TRUE_RANGE[cur_idx])
            local_upper_band = (float(format(local_upper_band, '.2f')))
            """
            """ Either local_upper_band is sufficiently lower than cur_lower_band or there should be at least 1 ATR 
            distance of High and Low.
            """
            local_upper_band = (float(CONFIG.tick[cur_idx][1]) + CONFIG.AVG_TRUE_RANGE[cur_idx])
            local_upper_band = (float(format(local_upper_band, '.2f')))
            if ((local_upper_band <= cur_lower_band) and \
                    (cur_lower_band - local_upper_band) > (1) and \
                        (CONFIG.tick[cur_idx][3] < cur_lower_band) and \
                            (cur_idx not in self.down_nxt_candle.keys())):
                CONFIG.SUPERTREND[cur_idx] = cur_lower_band
                self.LOG.info("At Downtrend, Updated the supertrend to:%f. cur_lower_bound:%f, local_upper_bound:%f",
                              CONFIG.SUPERTREND[cur_idx], cur_lower_band, local_upper_band)
                if ((cur_idx) not in self.SELL_LIST.keys()):
                    self.local_placeorder("SELL", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                          cur_upper_band, cur_idx)
                    self.LOG.info("Sell order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))
                else:
                    self.LOG.error("Sell order might have been already placed for timestamp=%s", cur_idx)
            """
            elif((abs(CONFIG.tick[cur_idx][1] - CONFIG.tick[cur_idx][3])) > CONFIG.AVG_TRUE_RANGE[cur_idx]):
                self.LOG.info("At Downtrend, high - low is 1ATR away. Place an order ")
                if ((cur_idx) not in self.SELL_LIST.keys()):
                    self.local_placeorder("SELL", CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP),
                                          cur_upper_band, cur_idx)
                    self.LOG.info("Sell order placed at LR:%f",
                                  CONFIG.LINEAR_REGRESSION.get(cur_idx, CONFIG.MARKET_LTP))
                else:
                    self.LOG.error("Sell order might have been already placed for timestamp=%s", cur_idx)
            """


            if (CONFIG.SUPERTREND[cur_idx] <= CONFIG.SUPERTREND[prev_idx]) and \
                    (candle_closing_time != datetime.datetime.now().replace(microsecond=0)):
                self.LOG.info("Running Down trend, Mkt_LTP:%f, cur_close:%f, supertrend:%f",
                              CONFIG.MARKET_LTP,
                              CONFIG.tick[cur_idx][3],
                              CONFIG.SUPERTREND[cur_idx])
            else:
                self.LOG.error(
                    "Irregular trend...MKT_LTP:%f is <= supertrend:%f. But cur-supertrend:%f > prev-supertrend:%f",
                    CONFIG.MARKET_LTP, CONFIG.SUPERTREND[cur_idx], CONFIG.SUPERTREND[cur_idx],
                    CONFIG.SUPERTREND[prev_idx])

            self.TREND[cur_idx] = "DOWN"
            CONFIG.SUPERTREND_TREND[cur_idx] = self.TREND.get(cur_idx)

        return

    """
    Function to identify the trend.
    
    Not used code!
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
        cur_time = datetime.datetime.now()
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        # mkt_price = CONFIG.PRICE_FORMAT(mkt_price)
        mkt_price = CONFIG.MARKET_LTP
        if CONFIG.trading_exchange == "MCX":
            target = CONFIG.COMMODITY_PROFIT_MARGIN
        elif CONFIG.trading_exchange == "NFO":
            target = CONFIG.NIFTY_PROFIT_MARGIN

        '''
        if (False) and math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx]) > target:
            target = math.ceil(CONFIG.AVG_TRUE_RANGE[cur_idx])
            """There is possibility that, Market can move 3times the ATR value to just cut the trade.
            For safer side, keep the stoploss 2 unit more.
            """
            stoploss = target * (CONFIG.SUPER_MULTIPLIER + 2)
        else:
            """There is possibility that, Market can move 3times the ATR value to just cut the trade.
            For safer side, keep the stoploss 2 unit more.
            """
            stoploss = math.ceil(target) * (CONFIG.SUPER_MULTIPLIER + 2)
        '''

        stoploss = math.floor(abs(CONFIG.MARKET_LTP - CONFIG.SUPERTREND.get(cur_idx, prev_idx)))+2
        if stoploss > (CONFIG.COMMODITY_PROFIT_MARGIN*2):
            stoploss = (CONFIG.COMMODITY_PROFIT_MARGIN*2)
        if stoploss <= (CONFIG.COMMODITY_PROFIT_MARGIN):
            stoploss = (CONFIG.COMMODITY_PROFIT_MARGIN*1.5)

        dt = datetime.datetime.now()
        order = {}
        oid = int(dt.strftime("%s"))
        if ORDER_TYPE == "BUY":
            order["order_id"] = oid
            order["timestamp"] = str(dt)
            order["type"] = "BUY"
            order["variety"] = "BO"
            order["trading_sym"] = CONFIG.trading_symbol
            order["buy_price"] = mkt_price
            order["target"] = target
            order["stoploss"] = stoploss
            self.BUY_LIST[cur_idx] = order
            self.LOG.info("Buy order placed successfully order details: %s", str(order))
            if CONFIG.trading_exchange == "MCX":
                ExchangeInterface.placecoverorder(CONFIG.trading_quantity, "BUY", order["buy_price"],
                                                order["target"], order["stoploss"])
            else:
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
            order["target"] = target
            order["stoploss"] = stoploss
            self.SELL_LIST[cur_idx] = order
            self.LOG.info("Sell order placed successfully order details: %s", str(order))

            if CONFIG.trading_exchange == "MCX":
                ExchangeInterface.placecoverorder(CONFIG.trading_quantity, "SELL", order["sell_price"],
                                              order["target"], order["stoploss"])
            else:
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
            self.temp_avg[prev_idx] = 0

        if (len(CONFIG.SUPERTREND) < CONFIG.SUPER_PERIOD):
            if (cur_idx not in CONFIG.tick.keys()):
                self.LOG.error("cur_idx:%s, may not have formed yet. Skipping for current second", cur_time)
                self.LOG.error("Tick list:%s", CONFIG.tick)
            else:
                self.LOG.info("Supertrend length=%d is less than SUPER_PERIOD=%d", len(CONFIG.SUPERTREND),
                              CONFIG.SUPER_PERIOD)
                self.upper[cur_idx] = CONFIG.tick[cur_idx][3]
                self.lower[cur_idx] = CONFIG.tick[cur_idx][3]
                if (cur_time.second >55 and cur_time.second<=59):
                    self.temp_avg[cur_idx] = CONFIG.tick[cur_idx][3]#float(CONFIG.tick[cur_idx][1] + CONFIG.tick[cur_idx][2]) / 2.0
                else:
                    return
                self.temp_avg[cur_idx] = (float(format(self.temp_avg[cur_idx], '.2f')))
                CONFIG.SUPERTREND[cur_idx] = self.temp_avg[cur_idx]
                if self.temp_avg[cur_idx] > self.temp_avg[prev_idx]:
                    self.TREND[cur_idx] = "UP"
                    self.LOG.info("The cur_idx:%s, the trend is:%s", cur_idx, self.TREND[cur_idx])
                elif self.temp_avg[cur_idx] < self.temp_avg[prev_idx]:
                    self.TREND[cur_idx] = "DOWN"
                    self.LOG.info("The cur_idx:%s, the trend is:%s", cur_idx, self.TREND[cur_idx])

                self.LOG.info("Supertrend value=%f, at idx=%s", CONFIG.SUPERTREND[cur_idx], cur_idx)

                if CONFIG.SUPERTREND_STARTING_TIME == None:
                    CONFIG.SUPERTREND_STARTING_TIME = cur_idx

        else:
            self.LOG.info("Supertrend length=%d at actual supertrend calculation. ", len(CONFIG.SUPERTREND))
            self.supertrend_util(cur_time)
        return


def main():
    while (True):
        tick_timestamp = datetime.datetime.now().replace(microsecond=0)
        if (tick_timestamp.minute % CONFIG.time_interval == 0):
            break

    obj = SuperTrend()
    if CONFIG.trading_exchange == "MCX":
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR_COMMODITY,
                                                          minute=CONFIG.CLOSE_MIN_COMMODITY, second=0,
                                                          microsecond=0)
    else:
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                          minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)

    while True:
        Timer(1, obj.main_supertrend, []).run()

        """ This logic of exiting works only for Single stock system"""
        if CONFIG.trading_exchange == "MCX":
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exitint supertrend thread as MCX market has closed.")
                return

        elif (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exitint supertrend thread as NSE/NFO market has closed.")
                return

    return
