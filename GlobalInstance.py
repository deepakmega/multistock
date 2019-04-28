'''
Created on 30-Jul-2017

@author: suhaheer
'''

import  pdb

from threading import Thread
import config as CONFIG
import dataFetcher
from authentication import Authenticate
import ExchangeInterface
from OrderMonitoring import Orderm
import save2file
import Controller
from simulator import simulator
from Controller import Controller
import bollingerBand
from bollingerBand import *
import os
from upstox_api.api import *
import datetime, time
import historicalDataMgmt
import simpleMovingAvg
import option_chain
import processor
import fibRetrace
import macd_calculation
import triangularMA
import relative_strength_index

class GlobalInst(object):
    '''
    Global instance class to create and hold pointers to all other objects.
    '''

    def __init__(self):
        CONFIG.init()
        pass


    def main(self):
        CONFIG.SIMULATION_MODE = False

        if not CONFIG.SIMULATION_MODE:
            auth = Authenticate()
            login_status = auth.login()
            if not login_status:
                print("Authentication failed. Exiting...")
                os._exit(1)

        if (datetime.datetime.now().time() < datetime.time(CONFIG.OPEN_HR, CONFIG.OPEN_MIN-1, 0, 0)):
            print("Waiting for Indian market to open...")
            starttime = datetime.datetime.now().replace(microsecond=0)
            endtime = datetime.datetime.now(year=starttime.year, month=starttime.month, day=starttime.day,
                                            hour=CONFIG.OPEN_HR, minute=CONFIG.OPEN_MIN-1, second=0, microsecond=0)
            sec = int((endtime - starttime).total_seconds())
            if (sec > 0):
                time.sleep(sec)


        controller = Controller()
        controller.init__controller()
        controller.import_from_storage()

        while (True):
            tick_timestamp = datetime.datetime.now().replace(microsecond=0)
            if (tick_timestamp.minute % CONFIG.time_interval == CONFIG.time_interval-1) and\
                    (tick_timestamp.second > 20):
                break

        print("Starting the system at a proper whole time...")
        while (datetime.datetime.now().time().second%60) != 0:
            pass

        CONFIG.SYSTEM_STARTED_TIME = datetime.datetime.now().replace(second=0, microsecond=0)
        """
        fetcher_t = Thread(target=dataFetcher.main)
        fetcher_t.start()
        time.sleep(90)
        """
        historical_t = Thread(target=historicalDataMgmt.main)
        historical_t.start()
        time.sleep(10)

        optchain_t = Thread(target=option_chain.main)
        optchain_t.start()
        time.sleep(30)

        sma_t = Thread(target=simpleMovingAvg.main)
        sma_t.start()

        bollinger_t = Thread(target=bollingerBand.main)
        bollinger_t.start()

        fibretrace_t = Thread(target=fibRetrace.main)
        fibretrace_t.start()

        macd_t = Thread(target=macd_calculation.main)
        macd_t.start()

        trima_t = Thread(target=triangularMA.main)
        trima_t.start()

        rsi_t = Thread(target=relative_strength_index.main)
        rsi_t.start()

        processor_t = Thread(target=processor.main)
        processor_t.start()

        return

    def exit_system(self):
        time.sleep(5)
        print("Closed all market for the day...")
        if (datetime.datetime.now() >= datetime.datetime.now().replace(hour=CONFIG.SYSTEM_CLOSE_HR,
                                                                       minute=CONFIG.SYSTEM_CLOSE_MIN,
                                                                       second=0, microsecond=0)):
            os._exit(0)

        else:
            return

        return

if __name__ == '__main__':
    mainObj = GlobalInst()
    CONFIG.GlobalInstObj = mainObj
    mainObj.main()


