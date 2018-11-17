import pdb
import time
import config as CONFIG
import logging
import datetime
import time
import numpy as np
import ExchangeInterface
from threading import Timer
import sklearn
from sklearn import linear_model
import talib
import os


class RSI_calculator(object):
    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/RSI_calculator-' + timestr + '.log', mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/RSI_calculator-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("RSI Calculator")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        self.tick = CONFIG.tick
        return

    def rsi_calculate(self,period):

        if len(self.tick) <= period:
            self.LOG.info("Required %d candles but available %d", period+1, len(self.tick))

        else:
            close_prices = []
            cur_time = datetime.datetime.now().replace(microsecond=0)
            cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
            cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
            cur_idx = str(cur_mkt_idx)
            final_idx = str(cur_mkt_idx)
            for i in range(1, len(self.tick)+1):
                while (cur_idx not in self.tick):
                    pass
                close_prices.append(self.tick[cur_idx][3])
                cur_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
                cur_idx = str(cur_mkt_idx)
            close_prices.reverse()
            a = np.asarray(close_prices)
            result = talib.RSI(a, timeperiod=5)
            CONFIG.RSI[final_idx] = (float(format(result[-1], '.2f'))) 
            self.LOG.info("RSI value for (%d period) at %s is %f", period , final_idx, CONFIG.RSI[final_idx])

        return


def main():
    while (True):
        tick_timestamp = datetime.datetime.now().replace(microsecond=0)
        if (tick_timestamp.minute % CONFIG.time_interval == 0):
            break

    obj = RSI_calculator()

    if CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO":
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)
    elif CONFIG.trading_exchange == "MCX":
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR_COMMODITY,
                                                          minute=CONFIG.CLOSE_MIN_COMMODITY, second=0,
                                                          microsecond=0)
    while True:
        Timer(CONFIG.TIMER_STD_VAL, obj.rsi_calculate,[CONFIG.RSI_PERIOD]).run()

        if CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO":
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exiting RSI thread as NSE/NFO market has closed now ")
                return

        elif CONFIG.trading_exchange == "MCX":
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exiting RSI thread as MCX market has closed now ")
                return

    return