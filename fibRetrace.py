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
import pandas as pd

class FibRetrace:
    LOG = None
    FibLevels = {}
    FibLevels['15MIN'] = {}
    FibLevels['30MIN'] = {}
    FibLevels['1HOUR'] = {}
    FibLevels['1DAY'] = {}
    FibLevels['1WEEK'] = {}
    FibLevels['1MONTH'] = {}
    std_timeFrame = ['15MIN','30MIN','1HOUR','1DAY','1WEEK','1MONTH']

    fiblevels = ['-3.764', '-3.618', '-3.5', '-3.382', '-3.236', '-3',
                 '-2.764', '-2.618', '-2.5', '-2.382', '-2.236', '-2',
                 '-1.764', '-1.618', '-1.5', '-1.382', '-1.236', '-1',
                 '0', '0.236', '0.382', '0.5', '0.618', '0.764',
                 '1', '1.236', '1.382', '1.5', '1.618', '1.764',
                 '2', '2.236', '2.382', '2.5', '2.618', '2.764',
                 '3', '3.236', '3.382', '3.5', '3.618', '3.764',
                 '4', '4.236', '4.382', '4.5', '4.618', '4.764']

    pd_fib = pd.DataFrame(dtype=(float), index=fiblevels, columns=std_timeFrame)


    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Fib_Retracement-' + timestr + '.log',
                                      mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Fib_Retracement")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

        self.reset_fib_level()

        return

    def reset_fib_level(self):
        for i in self.fiblevels:
            for timeFrame in self.std_timeFrame:
                self.pd_fib.at[i,timeFrame] = 0.0

        return

    def set_fib_level(self, stock):
        CONFIG.MUTEX.acquire()
        for timeFrame in self.std_timeFrame:
            high_low_range = (float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).high.values[-1]) -
                            float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).low.values[-1]))
            high_low_range = (float(format(high_low_range, '.2f')))

            ref_candle_lo = float(CONFIG.MULTISTOCK[stock][timeFrame]['TICKS'].tail(1).low.values[-1])

            for i in self.fiblevels:
                self.pd_fib.at[i,timeFrame] = float(format(ref_candle_lo + (high_low_range * float(i)), '.2f'))

        CONFIG.MULTISTOCK[stock]['Fibonacci_level'] = self.pd_fib
        self.pd_fib = pd.DataFrame(dtype=(float), index=self.fiblevels, columns=self.std_timeFrame)

        """
        self.LOG.info("\n")
        self.LOG.info("The Timeframes and OHLC\n")
        for i in self.std_timeFrame:
            self.LOG.info("TimeFrame: %s\n", i)
            self.LOG.info("%s\n",CONFIG.MULTISTOCK[stock][i]['TICKS'].tail(1))
        """

        CONFIG.MUTEX.release()

        return

    def main_processor(self):
        for stock in (CONFIG.TRADE_INSTRUMENT + CONFIG.TRADE_INDICES + CONFIG.TRADE_INSTRUMENT_MCX_FO):
            if not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1MONTH']['TICKS'].empty:
                self.set_fib_level(stock)

        self.track_fib_levels()

        return

    def track_fib_levels(self):
        for stock in (CONFIG.TRADE_INSTRUMENT + CONFIG.TRADE_INDICES + CONFIG.TRADE_INSTRUMENT_MCX_FO):
            if not CONFIG.MULTISTOCK[stock]['Fibonacci_level'].empty:
                cur_close = float(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(1).close.values[-1])
                self.LOG.info("Tracking Fib levels for stock - %s", stock)
                """ Check the CMP at month level"""
                for fib in range(len(self.fiblevels)):
                    if (fib + 1 < len(self.fiblevels)):
                        cur_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib], '1MONTH']
                        next_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib + 1], '1MONTH']
                        if(cur_fib < cur_close) and ( next_fib > cur_close):
                            self.LOG.info("Montly levels: close=%f  > %s(%s) and < %s(%s)",
                                      cur_close, cur_fib, self.fiblevels[fib], next_fib, self.fiblevels[fib + 1])

                for fib in range(len(self.fiblevels)):
                    if (fib + 1 < len(self.fiblevels)):
                        cur_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib], '1WEEK']
                        next_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib + 1], '1WEEK']
                        if (cur_fib < cur_close) and (next_fib > cur_close):
                            self.LOG.info("Weekly levels: close=%f  > %s(%s) and < %s(%s)",
                                      cur_close, cur_fib, self.fiblevels[fib], next_fib, self.fiblevels[fib + 1])

                for fib in range(len(self.fiblevels)):
                    if (fib + 1 < len(self.fiblevels)):
                        cur_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib], '1DAY']
                        next_fib = CONFIG.MULTISTOCK[stock]['Fibonacci_level'].at[self.fiblevels[fib + 1], '1DAY']
                        if (cur_fib < cur_close) and (next_fib > cur_close):
                            self.LOG.info("Daily levels: close=%f  > %s(%s) and < %s(%s)",
                                      cur_close, cur_fib, self.fiblevels[fib], next_fib, self.fiblevels[fib + 1])

                self.LOG.info("%s\n", CONFIG.MULTISTOCK[stock]['Fibonacci_level'])

        return


def main():
    obj = FibRetrace()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR,
                                                      minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)

    while True:
        Timer(1, obj.main_processor, []).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time and not CONFIG.SIMULATION_MODE):
                print("Exiting rangebound thread as NSE/NFO market has closed.")
                return

    return


import historicalDataMgmt
def std_init():
    historical_t = Thread(target=historicalDataMgmt.main)
    historical_t.start()
    time.sleep(10)

    fibretrace_t = Thread(target=main)
    fibretrace_t.start()
    return

"""
if __name__=='__main__':
    import config as CONFIG
    from authentication import Authenticate

    CONFIG.init()
    auth = Authenticate()
    login_status = auth.login()
    if not login_status:
        print("Authentication failed. Exiting...")
        os._exit(1)
    std_init()
    
"""

def fib_level_offline(pd_fib, fiblevels, timeFrame, ref_candle_hi, ref_candle_lo):
    high_low_range = ref_candle_hi - ref_candle_lo
    high_low_range = (float(format(high_low_range, '.2f')))
    for i in fiblevels:
        pd_fib.at[i, timeFrame] = float(format(ref_candle_lo + (high_low_range * float(i)), '.2f'))

    return


if __name__=='__main__':
    """
                        timeframe:[ High , low]
    """
    std_timeFrame = {'15MIN':[11797.95,11780.60],
                     '30MIN':[11797.95,11780.60],
                     '1HOUR':[11810.70,11776.75],
                     '1DAY' :[11810.95,11731.55],
                     '1WEEK':[11710.30,11549.10],
                     #'1MONTH':[11810.95,11549.10]}
                     '1MONTH':[11630.35, 10817.00]}

    fiblevels = ['-3.764', '-3.618', '-3.5', '-3.382', '-3.236', '-3',
                 '-2.764', '-2.618', '-2.5', '-2.382', '-2.236', '-2',
                 '-1.764', '-1.618', '-1.5', '-1.382', '-1.236', '-1',
                 '0', '0.236', '0.382', '0.5', '0.618', '0.764',
                 '1', '1.236', '1.382', '1.5', '1.618', '1.764',
                 '2', '2.236', '2.382', '2.5', '2.618', '2.764',
                 '3', '3.236', '3.382', '3.5', '3.618', '3.764',
                 '4', '4.236', '4.382', '4.5', '4.618', '4.764']

    pd_fib = pd.DataFrame(dtype=(float), index=fiblevels, columns=std_timeFrame)
    for i in std_timeFrame.keys():
        fib_level_offline(pd_fib, fiblevels, i, std_timeFrame[i][0], std_timeFrame[i][1])

    cur_close = 11752.80
    for fib in range(len(fiblevels)):
        if (fib+1 < len(fiblevels)) and (pd_fib.at[fiblevels[fib], '1MONTH'] < cur_close) and \
                (pd_fib.at[fiblevels[fib+1], '1MONTH'] > cur_close):
           print("Cur close", cur_close," is above Monthly Fib:", fiblevels[fib]," and less than monthly Fib:", fiblevels[fib+1])

    for fib in range(len(fiblevels)):
        if (fib+1 < len(fiblevels)) and (pd_fib.at[fiblevels[fib], '1WEEK'] < cur_close) and \
                (pd_fib.at[fiblevels[fib+1], '1WEEK'] > cur_close):
            print("Cur close", cur_close, " is above Weekly Fib:", fiblevels[fib], " and less than weekly Fib:", fiblevels[fib+1])

    for fib in range(len(fiblevels)):
        if (fib+1 < len(fiblevels)) and (pd_fib.at[fiblevels[fib], '1DAY'] < cur_close) and \
                (pd_fib.at[fiblevels[fib+1], '1DAY'] > cur_close):
            print("Cur close", cur_close, " is above daily Fib:", fiblevels[fib], " and less than daily Fib:", fiblevels[fib+1])

    print(pd_fib)
