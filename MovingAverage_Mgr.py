import pdb
import time
import config as CONFIG
import logging
import datetime
import time
import ExchangeInterface
from threading import Timer
import os


class MA_Mgmt(object):
    """Moving averages varies from simple, exponential, weighted and many other things.
    This class contains definitions of Simple moving average.
    """

    """
    Tick holds data in OHLC[0,1,2,3] format for every min.
    """
    tick = {}
    tick_fast_mover = []
    tick_slow_mover = []
    cur_mkt = -1
    LOG = None

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH+'logs/MovingAverage_Mgr-' + timestr + '.log', mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/MovingAverage_Mgr-' + timestr + '.log',
                                          mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("MovingAverage Manager")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

        self.tick = CONFIG.tick
        return

    def preprocess_SMA_OHLC_by_4(self,tick, tick_idx):
        DIVIDER = 4.0
        sum = 0
        for idx in range(len(tick[tick_idx])):
            sum = sum + tick[tick_idx][idx]
        res = (float(sum)/(DIVIDER))
        res = float(format(res, '.2f'))
        self.LOG.info("Average:%f at tick:%d",res, tick_idx)
        return res

    def SMA_OHLC(self, Period, cur):
        self.LOG.info("Current_period:%d", Period)
        """MA of 5 Period candle using OHLC/4"""
        if (Period==CONFIG.SMA_5PERIOD):
            sum = 0
            idx = 0
            while (idx<Period):
                data = self.preprocess_SMA_OHLC_by_4(self.tick, idx-Period)
                sum = sum + data
                idx = idx+1
            sum = float(sum /float(Period))
            sum = float(format(sum, '.2f'))
            CONFIG.SMA_5PERIOD_VALUE = sum
            self.LOG.info("SMA OHLC/4:%f of Period:%d", sum, Period)
            return

        """MA of 34 Period candle using OHLC/4"""
        if (Period==CONFIG.SMA_34PERIOD):
            self.LOG.info("Performing SMA for:%d time.", Period)
            sum = 0
            idx = 0
            while (idx < Period):
                data = self.preprocess_SMA_OHLC_by_4(self.tick, idx-Period)
                sum = sum + data
                idx = idx + 1
            sum = float(sum / float(Period))
            sum = float(format(sum, '.2f'))
            CONFIG.SMA_34PERIOD_VALUE = sum
            self.LOG.info("SMA OHLC/4:%f of Period:%d", sum, Period)

        """MA of 1 Period candle using open"""
        if (Period == CONFIG.SMA_1PERIOD):
            self.LOG.info("Performing SMA for:%d time, calculated over Open price", Period)
            min_cur = cur.minute - (cur.minute%CONFIG.time_interval)
            temp_idx = str(cur.replace(minute=min_cur,second=0,microsecond=0))
            if not (temp_idx in self.tick.keys()):
                self.LOG.error("Cur time:%s not found in the tick:",str(temp_idx))
                self.LOG.info("Tick list at 1Min Period:%s",self.tick)
                return

            CONFIG.SMA_1PERIOD_VALUE = self.tick[temp_idx][0]
            self.tick_fast_mover.append(CONFIG.SMA_1PERIOD_VALUE)
            self.LOG.info("SMA using Open:%f of Period:%d", CONFIG.SMA_1PERIOD_VALUE, Period)


        """MA of 14 Period candle using High."""
        if (Period == CONFIG.SMA_14PERIOD):
            self.LOG.info("Performing SMA for:%d time, calculated over High price", Period)
            min_cur = cur.minute - (cur.minute % CONFIG.time_interval)
            cur_idx = (cur.replace(minute=min_cur,second=0,microsecond=0))
            temp_idx = str(cur_idx)
            if (temp_idx not in self.tick.keys()):
                self.LOG.error("Cur time:%s not found in the tick:", temp_idx)
                self.LOG.info("Tick list at 14Period:%s", self.tick)
                return

            """
            Since we are rolling over last 14 min candle, 
            make sure all of them do exist in the tick list
            """
            idx = 0
            temp_temp_idx = cur_idx
            while (idx < Period):
                temp = str(temp_temp_idx)
                if (temp not in self.tick.keys()):
                    self.LOG.error("The idx:%s, not in tick list:%s", temp, self.tick)
                    return
                idx = idx + 1
                temp_temp_idx = temp_temp_idx - datetime.timedelta(minutes=CONFIG.time_interval)

            sum = 0
            idx = 0
            self.LOG.info("Tick list at 14Period:%s", self.tick)
            while (idx < Period):
                temp_idx = str(cur_idx)
                data = self.tick[temp_idx][1]
                sum = sum + data
                idx = idx + 1
                cur_idx = cur_idx - datetime.timedelta(minutes=CONFIG.time_interval)

            sum = float(sum / float(Period))
            sum = float(format(sum, '.2f'))
            CONFIG.SMA_14PERIOD_VALUE = sum
            self.tick_slow_mover.append(CONFIG.SMA_14PERIOD_VALUE)
            self.LOG.info("SMA using High:%f of Period:%d", sum, Period)


    def wrapper_SMA_init(self):
        cur = datetime.datetime.now().replace(microsecond=0, second=0)

        if (not len(CONFIG.tick)):
            return

        if (len(self.tick) >= CONFIG.SMA_5PERIOD):
            pass
        else:
            CONFIG.SMA_5PERIOD_VALUE = None

        if (len(self.tick) >= CONFIG.SMA_34PERIOD):
            pass
        else:
            CONFIG.SMA_34PERIOD_VALUE = None

        if (len(self.tick) >= CONFIG.SMA_1PERIOD):
            delta = cur - datetime.timedelta(minutes=(CONFIG.SMA_1PERIOD * CONFIG.time_interval))
            first_min = delta.minute - (delta.minute % CONFIG.time_interval)
            first_candle = delta.replace(minute=first_min, second=0, microsecond=0)
            if (str(first_candle) in self.tick.keys()):
                self.SMA_OHLC(CONFIG.SMA_1PERIOD, cur)
        else:
            self.LOG.info("Tick len is:%d for 1Period",len(self.tick))
            self.tick_fast_mover = []

        if (len(self.tick) >= CONFIG.SMA_14PERIOD):
            delta = cur - datetime.timedelta(minutes=(CONFIG.SMA_14PERIOD * CONFIG.time_interval))
            first_min = delta.minute-(delta.minute%CONFIG.time_interval)
            first_candle = delta.replace(minute=first_min,second=0,microsecond=0)
            if(str(first_candle) in self.tick.keys()):
                self.SMA_OHLC(CONFIG.SMA_14PERIOD, cur)
        else:
            self.LOG.info("Tick len is:%d for 14Period", len(self.tick))
            self.tick_slow_mover = []

        return

    def __init_SMA_mgmt__(self):
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)
        while True:
            Timer(1,self.wrapper_SMA_init, []).run()

            if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
                if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                    print("The market has closed... ")
                    return


class Trade_finder(object):
    """
    Indexes and timings
    """
    cur_time = None
    cur_min = None
    cur_mkt_idx = None
    cur_idx = None
    prev_mkt_idx = None
    prev_idx = None

    LOG = None
    prev_5SMA_list = {}
    prev_34SMA_list = {}
    prev_1SMA_list = {}
    prev_14SMA_list = {}

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH+'logs/TradeFinder-' + timestr + '.log', mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/TradeFinder-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Trade Finder")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

        return

    def _get_current_time(self):
        self.cur_time = datetime.datetime.now()
        self.cur_min = self.cur_time.minute - (self.cur_time.minute % CONFIG.time_interval)
        self.cur_mkt_idx = self.cur_time.replace(minute=self.cur_min, second=0, microsecond=0)
        self.cur_idx = str(self.cur_mkt_idx)

        self.prev_mkt_idx = self.cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        self.prev_idx = str(self.prev_mkt_idx)

        return

    def _init_TradeFinder(self):
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)
        while True:
            self._get_current_time()
            Timer(1,self.is_time_to_trade, ()).run()

            if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
                if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                    print("The market has closed... ")
                    return

    def is_time_to_trade(self):
        self.LOG.info("Executing is time to trade")

        if (not(CONFIG.SMA_1PERIOD_VALUE) or not(CONFIG.SMA_14PERIOD_VALUE)):
            self.LOG.info("SMA Values are empty. Not going ahead to trade.")
            return

        if len(self.prev_1SMA_list) < 3 or len(self.prev_14SMA_list) < 3:
            self.prev_1SMA_list[self.cur_idx] = CONFIG.SMA_1PERIOD_VALUE
            self.prev_14SMA_list[self.cur_idx] = CONFIG.SMA_14PERIOD_VALUE
            self.LOG.error("SMA Prev list is empty!.")
            return

        if ((CONFIG.SMA_1PERIOD_VALUE) and (CONFIG.SMA_14PERIOD_VALUE)):
            self.LOG.info("1Period SMA:%f, 14Period SMA:%f", CONFIG.SMA_1PERIOD_VALUE, CONFIG.SMA_14PERIOD_VALUE)
            self.prev_1SMA_list[self.cur_idx] = CONFIG.SMA_1PERIOD_VALUE
            self.prev_14SMA_list[self.cur_idx] = CONFIG.SMA_14PERIOD_VALUE

        """
        ATR[cur_idx] or TR[cur_idx] may be empty at this second. Skip Tradefinder for now.
        """
        if ((self.cur_idx) not in CONFIG.AVG_TRUE_RANGE.keys()) or ((self.cur_idx) not in CONFIG.AVG_TRUE_RANGE.keys()):
            self.LOG.error("The cur_idx:%s, not found in ATR or TR. skipping TradeFinder for now.", self.cur_idx)
            return

        if (CONFIG.SMA_1PERIOD_VALUE > CONFIG.SMA_14PERIOD_VALUE):
            self.TREND = CONFIG.UPTREND
            self.LOG.info("Market is running Uptrend at cur_mkt_price:%f, 1Period:%f, 14Period:%f, ATR:%f, TR:%f",
                     CONFIG.MARKET_LTP, CONFIG.SMA_1PERIOD_VALUE, CONFIG.SMA_14PERIOD_VALUE,
                    CONFIG.AVG_TRUE_RANGE[self.cur_idx], CONFIG.TRUE_RANGE[self.cur_idx])


        if (CONFIG.SMA_1PERIOD_VALUE < CONFIG.SMA_14PERIOD_VALUE):
            self.TREND = CONFIG.DOWNTREND
            self.LOG.info("Market is running Downtrend at cur_mkt_price:%f, 1Period:%f, 14Period:%f, ATR:%f, TR:%f",
                     CONFIG.MARKET_LTP, CONFIG.SMA_1PERIOD_VALUE, CONFIG.SMA_14PERIOD_VALUE,
                    CONFIG.AVG_TRUE_RANGE[self.cur_idx], CONFIG.TRUE_RANGE[self.cur_idx])


        if (0<=(CONFIG.SMA_1PERIOD_VALUE - CONFIG.SMA_14PERIOD_VALUE) < float(CONFIG.PIP)) \
                and (CONFIG.SMA_1PERIOD_VALUE >= CONFIG.SMA_14PERIOD_VALUE) \
                and (CONFIG.UPTREND == self.identify_trend()):
            self.LOG.info("Catching exact spot: Running Uptrend at cur_mkt_price::%f, 1Period::%f, 14Period:%f, TR:%f, ATR:%f",
                     CONFIG.MARKET_LTP, CONFIG.SMA_1PERIOD_VALUE, CONFIG.SMA_14PERIOD_VALUE,
                          CONFIG.TRUE_RANGE[self.cur_idx], CONFIG.AVG_TRUE_RANGE[self.cur_idx])
            trading_price = min([CONFIG.SMA_1PERIOD_VALUE, CONFIG.MARKET_LTP])
            if (self.cur_idx) not in CONFIG.BUY_PRICE.keys():
                self.local_placeorder("BUY", trading_price, self.cur_idx)
            else:
                self.LOG.error("Buy Order for this timestamp:%s might have been already placed",self.cur_idx)


        if ((-1*float(CONFIG.PIP))<=(CONFIG.SMA_1PERIOD_VALUE - CONFIG.SMA_14PERIOD_VALUE) < 0) \
                and (CONFIG.SMA_1PERIOD_VALUE <= CONFIG.SMA_14PERIOD_VALUE) \
                and (CONFIG.DOWNTREND == self.identify_trend()):
            self.LOG.info("Catching exact spot: Running Downtrend at cur_mkt_price::%f, 1Period::%f, 14Period:%f, TR:%f, ATR:%f",
                     CONFIG.MARKET_LTP, CONFIG.SMA_1PERIOD_VALUE, CONFIG.SMA_14PERIOD_VALUE,
                          CONFIG.TRUE_RANGE[self.cur_idx], CONFIG.AVG_TRUE_RANGE[self.cur_idx])
            trading_price = max([CONFIG.SMA_1PERIOD_VALUE, CONFIG.MARKET_LTP])
            if (self.cur_idx) not in CONFIG.SELL_PRICE.keys():
                self.local_placeorder("SELL", trading_price, self.cur_idx)
            else:
                self.LOG.error("Sell Order for this timestamp:%s might have been already placed",self.cur_idx)

        return


    def identify_trend(self):
        self.prev_mkt_idx = self.cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        self.prev_idx = str(self.prev_mkt_idx)
        another_prev = self.prev_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        another_prev_idx = str(another_prev)

        """
        Make sure all timestamps are present in prev list.
        """
        temp_1sma_list = self.prev_1SMA_list.keys()
        temp_14sma_list = self.prev_14SMA_list.keys()
        temp_list = [another_prev_idx, self.prev_idx, self.cur_idx]
        if len(set(temp_list) & set(temp_1sma_list))!=len(temp_list):
            return None
        if len(set(temp_list) & set(temp_14sma_list))!=len(temp_list):
            return None

        if (self.prev_1SMA_list[another_prev_idx] > self.prev_14SMA_list[another_prev_idx] and
                    self.prev_1SMA_list[self.prev_idx] > self.prev_14SMA_list[self.prev_idx] and
                    self.prev_1SMA_list[self.cur_idx] < self.prev_14SMA_list[self.cur_idx]):
            self.LOG.info("catching the crossing points. Its Downtrend")
            return CONFIG.DOWNTREND

        if (self.prev_1SMA_list[another_prev_idx] < self.prev_14SMA_list[another_prev_idx] and
                    self.prev_1SMA_list[self.prev_idx] < self.prev_14SMA_list[self.prev_idx] and
                    self.prev_1SMA_list[self.cur_idx] > self.prev_14SMA_list[self.cur_idx]):
            self.LOG.info("catching the crossing points. Its Uptrend")
            return CONFIG.UPTREND

        return

    def local_placeorder(self, ORDER_TYPE, mkt_price, cur_idx):
        dt = datetime.datetime.now()
        order = {}
        oid = int(dt.strftime("%s"))
        if ORDER_TYPE=="BUY":
            order["order_id"] = oid
            order["timestamp"] = str(dt)
            order["type"] = "BUY"
            order["variety"] = "BO"
            order["trading_sym"] = CONFIG.trading_symbol
            order["mkt_price"] = mkt_price
            order["target"] = int(CONFIG.AVG_TRUE_RANGE[cur_idx])
            order["stoploass"] = 40
            CONFIG.BUY_PRICE[cur_idx] = order
            self.LOG.info("Buy order placed successfully order details: %s", str(order))
            #ExchangeInterface.placebracketorder(CONFIG.trading_quantity, "BUY", mkt_price,
            #                                    int(CONFIG.AVG_TRUE_RANGE[cur_idx]), 40)

        if ORDER_TYPE == "SELL":
            order["order_id"] = oid
            order["timestamp"] = str(dt)
            order["type"] = "SELL"
            order["variety"] = "BO"
            order["trading_sym"] = CONFIG.trading_symbol
            order["mkt_price"] = mkt_price
            order["target"] = int(CONFIG.AVG_TRUE_RANGE[cur_idx])
            order["stoploass"] = 40
            CONFIG.SELL_PRICE[cur_idx] = order
            self.LOG.info("Sell order placed successfully order details: %s", str(order))
            #ExchangeInterface.placebracketorder(CONFIG.trading_quantity, "SELL", mkt_price,
            #                                    int(CONFIG.AVG_TRUE_RANGE[cur_idx]), 40)

        return