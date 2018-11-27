'''
Created on 17-Sep-2018

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
import multiprocessing
from multiprocessing import Process
from threading import Thread
import numpy as np
import talib as ta



class Bollingerband:
    """Multi stock Data"""
    LOG = None
    uband = -3
    mband = -2
    lband = -1

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/BollingerBand-' + timestr + '.log',
                                      mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Bollinger Band")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return


    def main_bollingerband(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                                    and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:
                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(50).close.values),
                            timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation for 5min data.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(50).close.values),
                                   timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 10min data..", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(50).close.values),
                                   timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 15min data.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(50).close.values),
                                   timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 30min data.", stock)
                    self.LOG.error("\n%s\n",str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(50).close.values),
                                       timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 1Hour data.", stock)
                    self.LOG.error("\n%s\n",str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).close.values),
                                       timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 1Day data.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    BB_out = ta.BBANDS(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(50).close.values),
                                       timeperiod=50)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', 'BB_50_UP'] = round(BB_out[self.uband][-1], 2)
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', 'BB_50_DOWN'] = round(BB_out[self.lband][-1], 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during Bollinger Band calculation 1Week data.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

        return


    def utility(self):
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                          minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)

        while True:
            Timer(1, self.main_bollingerband, []).run()

            if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
                if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                    print("Exiting rangebound thread as NSE/NFO market has closed.")
                    return

        return



def main():
    obj = Bollingerband()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                      minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)

    while True:
        Timer(1, obj.main_bollingerband, []).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exiting rangebound thread as NSE/NFO market has closed.")
                return

    return