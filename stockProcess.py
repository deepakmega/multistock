"""
Processing of stocks to qualify certain rules.

Rules are
1. Adhering to MACD
2. Adhering to TRIMA
3. Adhering to Fib levels.
"""

"""
MACD - Moving average Convergence and divergence.

"""

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
import pdb



class ProcessStock:
    LOG = None
    std_timeFrame = ['5MIN','10MIN','15MIN', '30MIN', '1HOUR', '1DAY', '1WEEK']

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/StockProcessor-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("ProcessStock")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return

    def is_ticks_empty(self, stock):
        if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
            and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:
            return  True
        else:
            return False

    def process_macd(self, stock):
        return 

    def perform_macd(self):
        for stock in (CONFIG.TRADE_INSTRUMENT + CONFIG.TRADE_INSTRUMENT_MCX_FO + CONFIG.TRADE_INDICES):
            if self.is_ticks_empty(stock):
                try:
                    self.process_macd(stock)

                except Exception as e:
                    self.LOG.error("%s - Exception occured during processing stock.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

        return

    def utility(self):
        while True:
            Timer(1, self.perform_stock, []).run()
            market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                     microsecond=0)

            if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
                if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                    print("The market has closed... ")
                    return
        return


def main():
    obj = ProcessStock()
    while True:
        Timer(1, obj.perform_stock, []).run()
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                          microsecond=0)

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                print("The market has closed... ")
                return

    return

if __name__ == '__main__':
    import config as CONFIG
    from authentication import Authenticate
    CONFIG.init()
    auth = Authenticate()
    login_status = auth.login()
    if not login_status:
        print("Authentication failed. Exiting...")
        os._exit(1)

    main()