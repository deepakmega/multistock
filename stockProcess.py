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
    TRADE = {}


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

    def process_trima(self, stock, transaction, message):
        if stock not in self.TRADE.keys():
            self.TRADE[stock] = {}

        if 'TRIMA' not in self.TRADE[stock].keys():
            self.TRADE[stock]['TRIMA'] = {}
            self.TRADE[stock]['TRIMA']['BUY'] = False
            self.TRADE[stock]['TRIMA']['SELL'] = False

        timeFrame = '15MIN'

        try:
            open = (float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).open.values[-1]))
            high = (float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).high.values[-1]))
            low = (float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).low.values[-1]))
            close = (float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).close.values[-1]))

            if not (CONFIG.MULTISTOCK[stock][timeFrame]['TRIMA'].size):
                return

            trima = float(CONFIG.MULTISTOCK[stock][timeFrame]['TRIMA'][-1:][-1])
        except Exception as e:
            self.LOG.error("%s - Exception occured during accessing TRIMA data.", stock)
            self.LOG.error("\n%s\n", str(e))
            return


        if transaction == "BUY":
            if (open > trima) and (high > trima) and (close > trima):
                if not self.TRADE[stock]['TRIMA']['BUY']:
                    #self.LOG.info("stock - %s , timeFrame-%s TRIMA - %f < O(%f), H(%f), C(%f) Signifies BUY",
                    #          stock, timeFrame, trima, open, high, close)
                    trimaMsg = "stock - " + str(stock) + " , timeFrame-" + str(timeFrame) + " TRIMA - " + str(trima) \
                               + " < O(" + str(open) + "), H(" + str(high) + "), C(" + str(close) + ") Signifies BUY"
                    final_msg = message+"\n"+trimaMsg
                    try:
                        if close >= CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at['0.618', '1DAY']:
                            self.LOG.info("stock - %s, timeFrame-%s, close(%f) > Fib-0.618 Signifies BUY",
                                          stock, timeFrame, close)
                            self.LOG.info(final_msg)
                            self.TRADE[stock]['TRIMA']['BUY'] = True
                            self.TRADE[stock]['TRIMA']['SELL'] = False
                    except Exception as e:
                        self.LOG.error("%s - Exception at FIB levels verification for BUY.", stock)
                        self.LOG.error("\n%s\n", str(e))

        if transaction == "SELL":
            if (open < trima) and (low < trima) and (close < trima):
                if not self.TRADE[stock]['TRIMA']['SELL']:
                    #self.LOG.info("stock - %s , timeFrame-%s TRIMA - %f > O(%f), H(%f), C(%f) Signifies SELL",
                    #          stock, timeFrame, trima, open, high, close)
                    trimaMsg = "stock - " + str(stock) + " , timeFrame-" + str(timeFrame) + " TRIMA - " + str(trima) + \
                               " > O(" + str(open) + "), H(" + str(high) + "), C(" + str(close) + ") Signifies SELL"
                    final_msg = message + "\n" + trimaMsg
                    try:
                        if close <= CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at['0.382', '1DAY']:
                            self.LOG.info("stock - %s, timeFrame-%s, close(%f) < Fib-0.382 Signifies SELL",
                                          stock, timeFrame, close)
                            self.LOG.info("\n"+final_msg)
                            self.TRADE[stock]['TRIMA']['BUY'] = False
                            self.TRADE[stock]['TRIMA']['SELL'] = True
                    except Exception as e:
                        self.LOG.error("%s - Exception at FIB levels verification for SELL.", stock)
                        self.LOG.error("\n%s\n", str(e))
        return

    def process_macd(self, stock):
        if stock not in self.TRADE.keys():
            self.TRADE[stock] = {}
            self.TRADE[stock]['MACD'] = {}
            self.TRADE[stock]['MACD']['BUY'] = False
            self.TRADE[stock]['MACD']['SELL'] = False

        timeFrame = '15MIN'

        try:
            """ Macd histogram cross over from -ve to positive """
            if not ((CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macdHist'].size) and \
                (CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macd'].size) and \
                    (CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macdSig'].size)):
                return

            macdHist = CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macdHist'][-1:][-1]
            macd = CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macd'][-1:][-1]
            macdSig = CONFIG.MULTISTOCK[stock][timeFrame]['MACD']['macdSig'][-1:][-1]
        except Exception as e:
            self.LOG.error("%s - Exception occured during accessing MACD data.", stock)
            self.LOG.error("\n%s\n", str(e))
            return

        try:
            if (float(macdHist) > 0.0):
                if (float(macd) > float(macdSig)):
                    if not self.TRADE[stock]['MACD']['BUY']:
                        #self.LOG.info("stock - %s timeFrame-%s Macd(%f) > MacdSig(%f) Signfies BUY",
                        #          stock, timeFrame, macd, macdSig)
                        macdMsg = "stock - " + str(stock) + " timeFrame-" + str(timeFrame) + \
                                  " Macd(" + str(macd) + ") > MacdSig(" + str(macdSig) + ") Signfies BUY"
                        self.process_trima(stock, "BUY", macdMsg)
                        self.TRADE[stock]['MACD']['BUY'] = True
                        self.TRADE[stock]['MACD']['SELL'] = False
        except Exception as e:
            self.LOG.error("%s - Exception occured during accessing MACD BUY processing.", stock)
            self.LOG.error("\n%s\n", str(e))
            return

        try:
            """ MACD histogram cross over from +ve to -ve signifies selling """
            if (float(macdHist) < 0.0):
                if (float(macd) < float(macdSig)):
                    if not self.TRADE[stock]['MACD']['SELL']:
                        #self.LOG.info("stock - %s timeFrame-%s Macd(%f) < MacdSig(%f) Signfies SELL",
                        #          stock, timeFrame, macd, macdSig)
                        macdMsg = "stock - " + str(stock) + " timeFrame-" + str(timeFrame) + \
                                  " Macd(" + str(macd) + ") < MacdSig(" + str(macdSig) + ") Signfies SELL"
                        self.process_trima(stock, "SELL", macdMsg)
                        self.TRADE[stock]['MACD']['BUY'] = False
                        self.TRADE[stock]['MACD']['SELL'] = True
        except Exception as e:
            self.LOG.error("%s - Exception occured during accessing MACD SELL processing.", stock)
            self.LOG.error("\n%s\n", str(e))
            return

        return


    def process_stocks(self):
        for stock in (CONFIG.TRADE_INSTRUMENT + CONFIG.TRADE_INSTRUMENT_MCX_FO + CONFIG.TRADE_INDICES):
            if self.is_ticks_empty(stock):
                try:
                    self.process_macd(stock)

                except Exception as e:
                    self.LOG.error("%s - Exception occured during processing of stock.", stock)
                    self.LOG.error("\n%s\n", str(e))
                    continue

        return

    def utility(self):
        while True:
            Timer(1, self.process_stocks, []).run()
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
        Timer(1, obj.process_stocks, []).run()
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
