'''
Created on 30-Jul-2017

@author: kailah rudra
'''

import  pdb

from threading import Thread
import config as CONFIG
import DataFetcher
from Authentication import Authenticate
import ExchangeInterface
from OrderMonitoring import Orderm
import save2file
import Controller
from simulator import simulator
from Controller import Controller
import BollingerBand
from BollingerBand import *
import os
from upstox_api.api import *
import datetime, time
import Historical_Data_Mgmt
import SimpleMovingAvg
import option_chain


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
            endtime = datetime.datetime(starttime.year, starttime.month, starttime.day, CONFIG.OPEN_HR, CONFIG.OPEN_MIN-1, 0, 0)
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

        fetcherThread = Thread(target=DataFetcher.main)
        fetcherThread.start()

        """
        Unsubscription and subscription of stocks takes significant amount of time.
        """
        time.sleep(120)

        historical_data = Thread(target=Historical_Data_Mgmt.main)
        historical_data.start()

        opt_chain = Thread(target=option_chain.main)
        opt_chain.start()

        sma_calculator = Thread(target=SimpleMovingAvg.main)
        sma_calculator.start()

        bollinger_band = Thread(target=BollingerBand.main)
        bollinger_band.start()

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

