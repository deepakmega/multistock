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


    def main_processor(self):
        self.track_mkt()
        return



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