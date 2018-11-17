
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


    def perform_sma(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty \
                and not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty \
                    and not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty \
                        and not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty \
                            and not CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty \
                                and not CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:
                try:
                    CONFIG.MULTISTOCK[stock]['5MIN']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['5MIN']['400SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(400).close.values, timeperiod=400)[-1]), 2)


                    CONFIG.MULTISTOCK[stock]['10MIN']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['10MIN']['400SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(400).close.values, timeperiod=400)[-1]), 2)

                    CONFIG.MULTISTOCK[stock]['15MIN']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['15MIN']['400SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(400).close.values, timeperiod=400)[-1]), 2)


                    CONFIG.MULTISTOCK[stock]['30MIN']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['30MIN']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)

                    CONFIG.MULTISTOCK[stock]['1HOUR']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)

                    CONFIG.MULTISTOCK[stock]['1DAY']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1DAY']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)

                    CONFIG.MULTISTOCK[stock]['1WEEK']['10SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(10).close.values, timeperiod=10)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['50SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(50).close.values, timeperiod=50)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['100SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(100).close.values, timeperiod=100)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['150SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(150).close.values, timeperiod=150)[-1]), 2)
                    CONFIG.MULTISTOCK[stock]['1WEEK']['200SMA'] = round(
                        (ta.SMA(CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(200).close.values, timeperiod=200)[-1]), 2)



                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['5Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['5MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['10Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['10MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['200SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['15Min', '400SMA'] = CONFIG.MULTISTOCK[stock]['15MIN']['400SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '10SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '50SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '100SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '150SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['30Min', '200SMA'] = CONFIG.MULTISTOCK[stock]['30MIN']['200SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '10SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '50SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '100SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '150SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1H', '200SMA'] = CONFIG.MULTISTOCK[stock]['1HOUR']['200SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '10SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '50SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '100SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '150SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1D', '200SMA'] = CONFIG.MULTISTOCK[stock]['1DAY']['200SMA']

                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '10SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['10SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '50SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['50SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '100SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['100SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '150SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['150SMA']
                    CONFIG.MULTISTOCK[stock]['DataFrame'].loc['1W', '200SMA'] = CONFIG.MULTISTOCK[stock]['1WEEK']['200SMA']

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
                                    print("Option chain is None")
                            else:
                                print("Dataframe is None")
                        else:
                            print("CMP is None")
                    else:
                        print("Stock is None")
                    """

                    self.LOG.info("\nStock:%s, LTP:%f \n %s\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['CMP'],
                                  CONFIG.MULTISTOCK[stock]['DataFrame'],
                                  CONFIG.MULTISTOCK[stock]['Option_chain'])


                except Exception as e:
                    print("Exception occured during SMA calculation for stock:", stock)
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
