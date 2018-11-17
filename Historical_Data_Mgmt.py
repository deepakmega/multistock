
import config as CONFIG
import time, datetime
from datetime import timedelta
from threading import Timer
from upstox_api.api import *
import pandas as pd
import threading
from threading import Thread



class Hist_Data:
    LOG = None

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/HistoricalData-' + timestr + '.log',
                                      mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Historical Data")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return

    def generate_15min_candles(self, stock, df_type_data):
        high_columns = df_type_data.max(axis=1)
        low_columns = df_type_data.min(axis=1)
        open_columns = df_type_data[df_type_data.columns[0]]
        close_columns = df_type_data[df_type_data.columns[-1]]

        open_columns = open_columns.to_frame()
        high_columns = high_columns.to_frame()
        low_columns = low_columns.to_frame()
        close_columns = close_columns.to_frame()

        CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = pd.concat([open_columns, high_columns, low_columns, close_columns],
                                                               axis=1)
        CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].columns = ['open','high','low','close']
        CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].rename_axis('Date', axis=1)
        CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].dropna()
        return


    def main_hist_data(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            try:
                res_5 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Minute_5, datetime.strptime(((datetime.now() - timedelta(days=30)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                res_10 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Minute_10, datetime.strptime(((datetime.now() - timedelta(days=50)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                res_30 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Minute_30,datetime.strptime(((datetime.now() - timedelta(days=200)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                res_1h = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Minute_60, datetime.strptime(((datetime.now() - timedelta(days=400)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())
                res_1D = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Day_1, datetime.strptime(((datetime.now() - timedelta(days=1000)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())
                res_1W = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol('NSE_EQ', stock),
                        OHLCInterval.Week_1, datetime.strptime(((datetime.now() - timedelta(days=1600)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())

            except Exception as e:
                self.LOG.error("%s - Failed to fetch historical data using API.", stock)
                self.LOG.error("%s",str(e))
                continue

            """
            Generate candles for 5min
            """
            try:
                temp_5 = {}
                for x in res_5:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    if not(time.hour > CONFIG.CLOSE_HR and time.minute>=CONFIG.CLOSE_MIN and (x['open'] == x['high']) and (x['high'] == x['close']) and (x['low'] == x['close'])):
                        temp_5[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_5 = pd.DataFrame.from_dict(temp_5, orient='index', columns = ['open', 'high', 'low', 'close'])
                temp_df_5 = temp_df_5.rename_axis('Date', axis =1)
                CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] = temp_df_5
            except Exception as e:
                self.LOG.error("%s - Exception occured during 5min historical data formation.", stock)
                self.LOG.error("%s", str(e))

            """
            Generate candles for 10min
            """
            try:
                temp_10 = {}
                for x in res_10:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    if not (time.hour > CONFIG.CLOSE_HR and time.minute >= CONFIG.CLOSE_MIN and (x['open'] == x['high']) and (x['high'] == x['close']) and (x['low'] == x['close'])):
                        temp_10[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_10 = pd.DataFrame.from_dict(temp_10, orient='index', columns=['open', 'high', 'low', 'close'])
                temp_df_10 = temp_df_10.rename_axis('Date', axis=1)
                CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] = temp_df_10
            except Exception as e:
                self.LOG.error("%s - Exception occured during 10min historical data formation.", stock)
                self.LOG.error("%s", str(e))


            """
            Generate candles for 15min based on 5min.
            """
            try:
                data_15min = temp_df_5.resample('15min').ohlc()
                self.generate_15min_candles(stock, data_15min)
            except Exception as e:
                self.LOG.error("%s - Exception occured during 15min historical data formation.", stock)
                self.LOG.error("%s", str(e))


            """
            Generate candles for 30min.
            """
            try:
                temp_30 = {}
                for x in res_30:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    if not (time.hour > CONFIG.CLOSE_HR and time.minute >= CONFIG.CLOSE_MIN and (x['open'] == x['high']) and (x['high'] == x['close']) and (x['low'] == x['close'])):
                        temp_30[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_30 = pd.DataFrame.from_dict(temp_30, orient='index', columns=['open', 'high', 'low', 'close'])
                temp_df_30 = temp_df_30.rename_axis('Date', axis=1)
                CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] = temp_df_30
            except Exception as e:
                self.LOG.error("%s - Exception occured during 30min historical data formation.", stock)
                self.LOG.error("%s", str(e))

            """
            Generate candles for 1hour.
            """
            try:
                temp_1h = {}
                for x in res_1h:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    if not (time.hour > CONFIG.CLOSE_HR and time.minute >= CONFIG.CLOSE_MIN and (
                            x['open'] == x['high']) and (x['high'] == x['close']) and (x['low'] == x['close'])):
                        temp_1h[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_1h = pd.DataFrame.from_dict(temp_1h, orient='index', columns=['open', 'high', 'low', 'close'])
                temp_df_1h = temp_df_1h.rename_axis('Date', axis=1)
                CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] = temp_df_1h
            except Exception as e:
                self.LOG.error("%s - Exception occured during Hour historical data formation.", stock)
                self.LOG.error("%s", str(e))

            """
            Generate candles for 1Day.
            """
            try:
                temp_1D = {}
                for x in res_1D:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    temp_1D[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_1D = pd.DataFrame.from_dict(temp_1D, orient='index', columns=['open', 'high', 'low', 'close'])
                temp_df_1D = temp_df_1D.rename_axis('Date', axis=1)
                CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'] = temp_df_1D
            except Exception as e:
                self.LOG.error("%s - Exception occured during Day historical data formation.", stock)
                self.LOG.error("%s", str(e))

            """
            Generate candles for 1Week.
            """
            try:
                temp_1W = {}
                for x in res_1W:
                    time = datetime.fromtimestamp(float(x['timestamp']) / 1000.0)
                    temp_1W[time] = [x['open'], x['high'], x['low'], x['close']]
                temp_df_1W = pd.DataFrame.from_dict(temp_1W, orient='index', columns=['open', 'high', 'low', 'close'])
                temp_df_1W = temp_df_1W.rename_axis('Date', axis=1)
                CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'] = temp_df_1W
            except Exception as e:
                self.LOG.error("%s - Exception occured during Week historical data formation.", stock)
                self.LOG.error("%s", str(e))

        return




def main():
    obj = Hist_Data()
    CONFIG.UPSTOX_SESSION.get_master_contract('NSE_EQ')
    market_end_time = datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                      microsecond=0)
    while True:
        Timer((30), obj.main_hist_data, []).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                print("The market has closed... ")
                return

    return
