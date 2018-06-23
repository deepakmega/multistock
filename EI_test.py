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

if __name__ == '__main__':
    CONFIG.init()

    auth = Authenticate()
    auth.login()

    ExchangeInterface.main()
    CONFIG.trading_exchange = "NFO"
    ExchangeInterface.placebracketorder(75, "BUY", 10465, 6, 35)