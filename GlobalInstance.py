'''
Created on 30-Jul-2017

@author: kailah rudra
'''

import  pdb

from threading import Thread
import config as CONFIG
import DataFetcher
from Authentication import  Authenticate
import ExchangeInterface
from OrderMonitoring import Orderm
from MovingAverage_Mgr import MA_Mgmt, Trade_finder
import superTrend
from superTrend import SuperTrend
import save2file
import datetime, time
import Controller
from simulator import simulator
from Controller import Controller
import config
import LinearRegression, RSI
import os


class GlobalInst(object):
    '''
    Global instance class to create and hold pointers to all other objects.
    '''
    superTrendThread = None
    SMA_5_56_period = None
    TradeFinder = None
    LRThread = None
    fetcherThread = None
    orderMonitorThread = None
    simulation = None

    def __init__(self):
        CONFIG.init()
        pass

    def MktAnalyzer(self):
        self.superTrendThread = Thread(target=superTrend.main)
        self.superTrendThread.start()

        ma_mgmt = MA_Mgmt()
        self.SMA_5_56_period = Thread(target=ma_mgmt.__init_SMA_mgmt__)

        self.LRThread = Thread(target=LinearRegression.main)
        self.LRThread.start()

        self.RSIThread = Thread(target=RSI.main)
        self.RSIThread.start()

        tradefinder = Trade_finder()
        self.TradeFinder = Thread(target=tradefinder._init_TradeFinder)

        self.SMA_5_56_period.start()
        self.TradeFinder.start()

        return

    def main(self):
        CONFIG.SIMULATION_MODE = False

        if not CONFIG.SIMULATION_MODE:
            auth = Authenticate()
            login_status = auth.login()
            if not login_status:
                print("Exiting due to authentication failure")
                os._exit(1)

            if (datetime.datetime.now().time() < datetime.time(CONFIG.OPEN_HR, CONFIG.OPEN_MIN-1, 0, 0)):
                print("Waiting for Indian market to open...")
                starttime = datetime.datetime.now().replace(microsecond=0)
                endtime = datetime.datetime(starttime.year, starttime.month, starttime.day, CONFIG.OPEN_HR, CONFIG.OPEN_MIN-1, 0, 0)
                sec = int((endtime - starttime).total_seconds())
                if (sec > 0):
                    time.sleep(sec)

        controller = Controller()
        controller.init__controller()
        controller.import_from_storage()
        CONFIG.tick = {}
        CONFIG.TRUE_RANGE = {}
        CONFIG.AVG_TRUE_RANGE = {}
        CONFIG.SUPERTREND_TREND = {}
        CONFIG.RSI = {}
        CONFIG.LINEAR_REGRESSION = {}
        CONFIG.SUPERTREND = {}
        print("True range:", CONFIG.TRUE_RANGE)
        print("Avg True Range:", CONFIG.AVG_TRUE_RANGE)
        print("Supertrend", CONFIG.SUPERTREND)
        print("Supertrend_trend", CONFIG.SUPERTREND_TREND)
        print("RSI",CONFIG.RSI)
        print("Linear Regression:",CONFIG.LINEAR_REGRESSION)


        print("Starting the system at a proper whole time...")
        while (datetime.datetime.now().time().second%60) != 0:
            pass

        CONFIG.SYSTEM_STARTED_TIME = datetime.datetime.now().replace(second=0, microsecond=0)

        self.MktAnalyzer()

        ordermObj = Orderm()
        self.orderMonitorThread = Thread(target=ordermObj.pending_order_manager)
        self.orderMonitorThread.start()
        ExchangeInterface.main()

        """
        Market Simulator: We have past 1 day Nifty candle information. Simulator feeds the data 
        like kite feeder.
        """
        if CONFIG.SIMULATION_MODE:
            CONFIG.SIMULATION_START_HR = datetime.datetime.now().time().hour
            CONFIG.SIMULATION_START_MIN = datetime.datetime.now().time().minute
            if CONFIG.SIMULATION_START_HR > 17:
                CONFIG.CLOSE_HR = 6 - (24-CONFIG.SIMULATION_START_HR)

            self.simulation = Thread(target=simulator())
            self.simulation.start()
        else:
            self.fetcherThread = Thread(target=DataFetcher.main())
            self.fetcherThread.start()

        if not CONFIG.SIMULATION_MODE:
            self.exit_system()

        return

    def exit_system(self):
        time.sleep(5)
        print("Closed all market for the day. I'll resume next trading day @9AM. Signing off now...")
        if (datetime.datetime.now() >= datetime.datetime.now().replace(hour=CONFIG.SYSTEM_CLOSE_HR,
                                                    minute=CONFIG.SYSTEM_CLOSE_MIN, second=0,
                                                    microsecond=0)):
            os._exit(0)

        else:
            return

        return

if __name__ == '__main__':
    mainObj = GlobalInst()
    CONFIG.GlobalInstObj = mainObj
    mainObj.main()

