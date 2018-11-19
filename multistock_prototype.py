import  pdb

from threading import Thread
import config as CONFIG
import dataFetcher
from authentication import  Authenticate
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
from threading import Timer
from multiprocessing import Process

class testing_concurrency:
    INSTRUMENT = None

    def __init__(self, instrument):
        self.INSTRUMENT = instrument

    def testing(self):
        print("The current instrument:",self.INSTRUMENT, " timing:", datetime.datetime.now())

    def testing_utility(self):
        while True:
            Timer(1, self.testing, []).run()


#multistocks = [1,2,3,4,5]
objects = {}
def main():
    """
    First create multiple objects constructs
    :return:
    """
    for stock in range(1,100):
        objects[stock] = testing_concurrency(stock)

    """
    Call each objects's starting pointer(in this case 'testing_utility')
    """
    for stock in range(1,100):
        Process(target=objects[stock].testing_utility).start()

    return

if __name__ == '__main__':
    main()
