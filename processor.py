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
import numpy
import talib as ta



class Processor:
    LOG = None

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Processor-' + timestr + '.log',
                                      mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Processor")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return

    def weekly_200sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(1).close.values[-1])
            week_200sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W','200SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: weekly 200 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if cur_close >= week_200sma_val:
            if not (self.weekly_100sma(stock, call="BUY")):
                self.LOG.error("stock - %s: weekly 100sma %s call processing has failed.", stock, call)
                return  False
        else:
            if not (self.weekly_100sma(stock, call="SELL")):
                self.LOG.error("stock - %s: weekly 100sma %s call processing has failed.", stock, call)
                return False
        return True

    def weekly_100sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(1).close.values[-1])
            week_100sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '100SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: weekly 100 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= week_100sma_val:
                if not (self.weekly_50sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: weekly 50sma %s call processing has failed.", stock, call)
                    return False
            """
            else:
                This can be wrong Buy signal. Ignore!
            """
        elif call=="SELL":
            if cur_close < week_100sma_val:
                if not (self.weekly_50sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: weekly 50sma %s call processing has failed.", stock, call)
                    return False
            """
            else:
                This can be wrong Sell signal. Ignore!
            """
        return True

    def weekly_50sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(1).close.values[-1])
            week_50sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '50SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: weekly 50 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= week_50sma_val:
                if not (self.weekly_10sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: weekly 10sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < week_50sma_val:
                if not (self.weekly_10sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: weekly 10sma %s call processing has failed.", stock, call)
                    return False
        return True

    def weekly_10sma(self, stock, call):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(1).close.values[-1])
            week_10sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '10SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: weekly 10 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= week_10sma_val:
                if not (self.daily_200sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: weekly 10sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < week_10sma_val:
                if not (self.weekly_50sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: weekly 10sma %s call processing has failed.", stock, call)
                    return False
        return True

    def daily_200sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(1).close.values[-1])
            daily_200sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '200SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: daily 200 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= daily_200sma_val:
                if not (self.daily_100sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: daily 200sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < daily_200sma_val:
                if not (self.daily_100sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: daily 200sma %s call processing has failed.", stock, call)
                    return False
        return True

    def daily_100sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(1).close.values[-1])
            daily_100sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '100SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: daily 100 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= daily_100sma_val:
                if not (self.daily_50sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: daily 100sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < daily_100sma_val:
                if not (self.daily_50sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: daily 100sma %s call processing has failed.", stock, call)
                    return False
        return True

    def daily_50sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(1).close.values[-1])
            daily_50sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '50SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: daily 50 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= daily_50sma_val:
                if not (self.daily_10sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: daily 50sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < daily_50sma_val:
                if not (self.daily_10sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: daily 50sma %s call processing has failed.", stock, call)
                    return False
        return True

    def daily_10sma(self, stock, call=None):
        try:
            cur_close = float(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(1).close.values[-1])
            daily_10sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '10SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: daily 10 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if cur_close >= daily_10sma_val:
                if not (self.min15_10sma(stock, call="BUY")):
                    self.LOG.error("stock - %s: daily 10sma %s call processing has failed.", stock, call)
                    return False

        elif call=="SELL":
            if cur_close < daily_10sma_val:
                if not (self.min15_10sma(stock, call="SELL")):
                    self.LOG.error("stock - %s: daily 10sma %s call processing has failed.", stock, call)
                    return False
        return True

    def min15_10sma(self, stock, call=None):
        try:
            open = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).open.values[-1])
            high = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).high.values[-1])
            low = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).low.values[-1])
            close = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).close.values[-1])
            cmp = float(CONFIG.MULTISTOCK[stock]['CMP'])
            min15_10sma_val = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '10SMA'])
        except Exception as err:
            self.LOG.error("stock - %s: 15min 10 SMA process.", stock)
            self.LOG.error("stock - %s: %s", stock, str(err))
            return False

        if call=="BUY":
            if open >= min15_10sma_val and high >= min15_10sma_val and close >= min15_10sma_val:
                if cmp >= min15_10sma_val:
                    self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['CMP'],
                                  CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1),
                                  CONFIG.MULTISTOCK[stock]['DataFrame'],
                                  CONFIG.MULTISTOCK[stock]['Option_chain'])

        elif call=="SELL":
            if open <= min15_10sma_val and low <= min15_10sma_val and close <= min15_10sma_val:
                if cmp <= min15_10sma_val:
                    self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['CMP'],
                                  CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1),
                                  CONFIG.MULTISTOCK[stock]['DataFrame'],
                                  CONFIG.MULTISTOCK[stock]['Option_chain'])
        return True

    def min15_50sma_high(self, stock, call=None):
        return True

    def min_50sma_low(self, stock, call=None):
        return True

    def process_sma(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            if not CONFIG.MULTISTOCK[stock]['DataFrame'].empty:
                if not (self.weekly_200sma(stock)):
                    self.LOG.error("stock - %s: weekly 200sma processing has failed.", stock)

        return

    def main_processor(self):
        for stock in CONFIG.TRADE_INSTRUMENT+CONFIG.TRADE_INDICES+CONFIG.TRADE_INSTRUMENT_MCX_FO:
            """
            if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                                    and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:
            """
            if stock:
                if CONFIG.MULTISTOCK[stock]['CMP']:
                    if not CONFIG.MULTISTOCK[stock]['DataFrame'].empty:
                        if not CONFIG.MULTISTOCK[stock]['Option_chain'].empty:
                            self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n%s\n\n", stock,
                                          CONFIG.MULTISTOCK[stock]['CMP'],
                                          CONFIG.MULTISTOCK[stock]['DataFrame'],
                                          CONFIG.MULTISTOCK[stock]['Option_chain'],
                                          CONFIG.MULTISTOCK[stock]['Fibonacci_level'])
                        else:
                            self.LOG.error("Option chain is None")
                    else:
                        self.LOG.error("Dataframe is None")
                else:
                    self.LOG.error("CMP is None")
            else:
                self.LOG.error("Stock is None")

        return

"""
    def track_mkt(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            if not CONFIG.MULTISTOCK[stock]['DataFrame'].empty \
                and not CONFIG.MULTISTOCK[stock]['Option_chain'].empty \
                    and not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                                    and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                                        and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty:
                try:
                    open = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).open.values[-1])
                    high = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).high.values[-1])
                    low = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).low.values[-1])
                    close = float(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1).close.values[-1])
                    day_10sma = float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D','10SMA'])
                    if close == None:
                        self.LOG.error("Close value is None for stock:", stock)
                        return

                    if CONFIG.MULTISTOCK[stock]['CMP'] == None:
                        self.LOG.error("CMP value is None for stock:", stock)
                        return

                    if open>0 and high>0 and low>0 and close>0 and day_10sma>0 and \
                        (CONFIG.MULTISTOCK[stock]['CMP'] > day_10sma):
                        if (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min','10SMA'])) and \
                            (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min','50SMAL'])) and \
                                (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min','50SMA'])) and \
                                    (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '50SMAH'])) and \
                                        (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '100SMA'])) and \
                                            (close > float(CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '200SMA'])):

                            self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n%s\n", stock,
                                            CONFIG.MULTISTOCK[stock]['CMP'],
                                            CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(1),
                                            CONFIG.MULTISTOCK[stock]['DataFrame'],
                                            CONFIG.MULTISTOCK[stock]['Option_chain'])

                except Exception as e:
                    self.LOG.error("%s - Exception occured during processing of 15min data.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

        return
"""


def main():
    obj = Processor()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                      minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)

    while True:
        Timer(1, obj.main_processor, []).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exiting rangebound thread as NSE/NFO market has closed.")
                return

    return
