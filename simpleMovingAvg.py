
import config as CONFIG
from threading import Timer
from upstox_api.api import *
import pandas as pd
import talib as ta
import os
import threading
from threading import Thread
from threading import Timer
import logging
import time, datetime
import numpy as np


class SMA_:
    LOG = None

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/SimpleMovingAverage-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("SMA")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return


    def if_ticks_empty(self):
        return True

    def perform_sma(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                                    and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:

                try:
                    CONFIG.MULTISTOCK[stock]['5MIN']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    min5_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    min5_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 5min SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    CONFIG.MULTISTOCK[stock]['10MIN']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    min10_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    min10_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 10min SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    CONFIG.MULTISTOCK[stock]['15MIN']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    min15_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    min15_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 15min SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    CONFIG.MULTISTOCK[stock]['30MIN']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    min30_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    min30_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 30min SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    CONFIG.MULTISTOCK[stock]['1HOUR']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    min60_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    min60_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 1Hour SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue


                try:
                    CONFIG.MULTISTOCK[stock]['1DAY']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    day_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    day_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 1Day SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

                try:
                    CONFIG.MULTISTOCK[stock]['1WEEK']['10SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(10).close.values), timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['50SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(50).close.values), timeperiod=50)[-1]), 2)
                    week_50sma_l = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).low.values), timeperiod=50)[-1]), 2)
                    week_50sma_h = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).high.values), timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['100SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(100).close.values), timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['150SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(150).close.values), timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['200SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(200).close.values), timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['400SMA'] = round(
                        (ta.SMA(np.array(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(400).close.values), timeperiod=400)[-1]), 2)
                except Exception as e:
                    self.LOG.error("%s - Exception occured during 1Week SMA calculation.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue


                try:
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '50SMAL'] = min5_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '50SMAH'] = min5_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '50SMAL'] = min10_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '50SMAH'] = min10_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '50SMAL'] = min15_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '50SMAH'] = min15_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '50SMAL'] = min30_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '50SMAH'] = min30_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '10SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '50SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '50SMAL'] = min60_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '50SMAH'] = min60_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '100SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '150SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '200SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '400SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '10SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '50SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '50SMAL'] = day_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '50SMAH'] = day_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '100SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '150SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '200SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '400SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '10SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '50SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '50SMAL'] = week_50sma_l
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '50SMAH'] = week_50sma_h
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '100SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '150SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '200SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '400SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['400SMA']

                    """
                    if stock:
                        if CONFIG.MULTISTOCK[stock]['CMP']:
                            if not CONFIG.MULTISTOCK[stock]['DataFrame'].empty:
                                if not CONFIG.MULTISTOCK[stock]['Option_chain'].empty:
                                    self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n", stock,
                                                  CONFIG.MULTISTOCK[stock]['CMP'],
                                                  CONFIG.MULTISTOCK[stock]['DataFrame'],
                                                  CONFIG.MULTISTOCK[stock]['Option_chain'])
                                else:
                                    self.LOG.error("Option chain is None")
                            else:
                                self.LOG.error("Dataframe is None")
                        else:
                            self.LOG.error("CMP is None")
                    else:
                        self.LOG.error("Stock is None")
                    """

                    """
                    if not CONFIG.MULTISTOCK[stock]['DataFrame'].empty and \
                        not CONFIG.MULTISTOCK[stock]['DataFrame']['Option_chain'].empty:
                        self.LOG.info("\nStock:%s, LTP:%f \n%s\n %s\n", stock,
                                        CONFIG.MULTISTOCK[stock]['CMP'],
                                        CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(1),
                                        CONFIG.MULTISTOCK[stock]['DataFrame'])
                    """


                except Exception as e:
                    self.LOG.error("%s - Exception occured during Dataframe formation of SMA values", stock)
                    self.LOG.error("\n%s\n",str(e))
                    continue

        return


    def utility(self):
        while True:
            Timer(1, self.perform_sma, []).run()
            market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                     microsecond=0)

            if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
                if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                    print("The market has closed... ")
                    return
        return



def main():
    obj = SMA_()
    while True:
        Timer(1, obj.perform_sma, []).run()
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                print("The market has closed... ")
                return

    return
