
import config as CONFIG
import time
from time import sleep
import  datetime
from datetime import timedelta
from threading import Timer
from upstox_api.api import *
import pandas as pd
import threading
from threading import Thread
import requests
from requests import HTTPError

class Historical_Data:
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

        temp_df_15 = pd.concat([open_columns, high_columns, low_columns, close_columns], axis=1)
        temp_df_15.columns = ['open','high','low','close']
        temp_df_15 = temp_df_15.rename_axis('Date', axis=1)
        temp_df_15 = temp_df_15.dropna()
        if CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty:
            CONFIG.MUTEX.acquire()
            CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = temp_df_15
            CONFIG.MUTEX.release()
            self.LOG.info("%s - New historical data for 15min frame\n%s\n", stock,
                          CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(10))
        else:
            if not temp_df_15.empty:
                CONFIG.MUTEX.acquire()
                CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] \
                    = pd.concat([CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'], temp_df_15])
                CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].sort_index(inplace=True)
                CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = \
                    CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'][
                        ~CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].index.duplicated(keep='last')]
                CONFIG.MUTEX.release()
                self.LOG.info("%s - 15Min TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(10))

        return


    def fetch_hist_data(self, exchange):
        if exchange not in ['NSE_EQ', 'NSE_FO', 'BSE_EQ', 'BSE_FO', 'MCX_FO']:
            self.LOG.error("Invalid Exchange. Stock subscprition failed.")
            return False

        MAX_RETRIES = 1
        WAIT_TIME = 0.2
        for stock in CONFIG.TRADE_INSTRUMENT:
            res_5 = None
            retry = 0
            while res_5 is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_5 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Minute_5, datetime.strptime(((datetime.now() - timedelta(days=30)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                    if res_5:
                        break
                except HTTPError as er:
                    retry = retry + 1
                    self.LOG.error("%s - Failed to fetch 5Min historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 5Min historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))


            retry = 0
            res_10 = None
            while res_10 is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_10 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Minute_10, datetime.strptime(((datetime.now() - timedelta(days=50)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                    if res_10:
                        break
                except HTTPError as er:
                    self.LOG.error("%s - Failed to fetch 10Min historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    retry = retry + 1
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 10Min historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))


            retry = 0
            res_30  = None
            while res_30 is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_30 = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Minute_30,datetime.strptime(((datetime.now() - timedelta(days=200)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"), '%d/%m/%Y').date())
                    if res_30:
                        break
                except HTTPError as er:
                    self.LOG.error("%s - Failed to fetch 30Min historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    retry = retry + 1
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 30Min historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))


            retry = 0
            res_1h = None
            while res_1h is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_1h = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Minute_60, datetime.strptime(((datetime.now() - timedelta(days=400)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())
                    if res_1h:
                        break
                except HTTPError as er:
                    self.LOG.error("%s - Failed to fetch 1Hour historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    retry = retry + 1
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 1Hour historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))


            retry = 0
            res_1D = None
            while res_1D is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_1D = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Day_1, datetime.strptime(((datetime.now() - timedelta(days=1000)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())
                    if res_1D:
                        break
                except HTTPError as er:
                    self.LOG.error("%s - Failed to fetch 1Day historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    retry = retry + 1
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 1Day historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))


            retry = 0
            res_1W  = None
            while res_1W is None and retry < MAX_RETRIES:
                try:
                    sleep(0.1)
                    res_1W = CONFIG.UPSTOX_SESSION.get_ohlc(CONFIG.UPSTOX_SESSION.get_instrument_by_symbol(exchange, stock),
                            OHLCInterval.Week_1, datetime.strptime(((datetime.now() - timedelta(days=1600)).strftime("%d/%m/%Y")), '%d/%m/%Y').date(),datetime.strptime(datetime.now().strftime("%d/%m/%Y"),'%d/%m/%Y').date())
                    if res_1W:
                        break
                except HTTPError as er:
                    self.LOG.error("%s - Failed to fetch 1Week historical data using API. Will retry in 1s", stock)
                    self.LOG.error("%s", str(er))
                    retry = retry + 1
                    sleep(WAIT_TIME)
                except Exception as er:
                    self.LOG.error("%s - Failed to fetch 1Week historical data using API.Some other exceptin occured.", stock)
                    self.LOG.error("%s", str(er))



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
                temp_df_5 = temp_df_5.rename_axis('Date', axis=1)
                if CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] = temp_df_5
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 5min frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(10))
                else:
                    if not temp_df_5.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'], temp_df_5])
                        CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 5Min TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(10))
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
                if CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] = temp_df_10
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 10min frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(10))
                else:
                    if not temp_df_10.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'], temp_df_10])
                        CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 10Min TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(10))

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
                if CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] = temp_df_30
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 30min frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(10))
                else:
                    if not temp_df_30.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'], temp_df_30])
                        CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 30Min TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(10))

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
                if CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] = temp_df_1h
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 1Hour frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(10))
                else:
                    if not temp_df_1h.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'], temp_df_1h])
                        CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 1Hour TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(10))

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
                if CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'] = temp_df_1D
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 1Day frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(10))
                else:
                    if not temp_df_1D.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'], temp_df_1D])
                        CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 1Day TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['1DAY']['TICKS'].tail(10))

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
                if CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].empty:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'] = temp_df_1W
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - New historical data for 1Week frame\n%s\n", stock,
                                  CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(10))
                else:
                    if not temp_df_1W.empty:
                        CONFIG.MUTEX.acquire()
                        CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'] \
                            = pd.concat([CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'], temp_df_1W])
                        CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].sort_index(inplace=True)
                        CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'] = \
                            CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'][
                                ~CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].index.duplicated(keep='last')]
                        CONFIG.MUTEX.release()
                        self.LOG.info("%s - 1Week TICKS\n%s\n", stock,
                                      CONFIG.MULTISTOCK[stock]['1WEEK']['TICKS'].tail(10))

            except Exception as e:
                self.LOG.error("%s - Exception occured during Week historical data formation.", stock)
                self.LOG.error("%s", str(e))

        return


    def form_realtime_tick(self):
        for stock in CONFIG.TRADE_INSTRUMENT:
            try:
                df = pd.DataFrame(list(CONFIG.MULTISTOCK[stock]['LTP'].items()), columns=['Date', 'DateValue'])
                data = df.set_index(['Date'])
                """The LTP data after resample are upto the current timeframe accuracy.
                """
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during DF formation.")
                self.LOG.error("%s", str(e))
                return

            try:
                data_5min = data['DateValue'].resample('5min').ohlc()
                self.LOG.info("%s - 5Min resampled data\n%s\n", stock, data_5min.tail(10))
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during 5Min data resample")
                self.LOG.error("%s", str(e))

            try:
                data_10min = data['DateValue'].resample('10min').ohlc()
                self.LOG.info("%s - 10Min resampled data\n%s\n", stock, data_10min.tail(10))
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during 10Min data resample")
                self.LOG.error("%s", str(e))

            try:
                data_15min = data['DateValue'].resample('15min', base=15).ohlc()
                self.LOG.info("%s - 15Min resampled data\n%s\n", stock, data_15min.tail(10))
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during 15Min data resample")
                self.LOG.error("%s", str(e))

            try:
                data_30min = data['DateValue'].resample('30Min', base=15).ohlc()
                self.LOG.info("%s - 30Min resampled data\n%s\n", stock, data_30min.tail(10))
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during 30Min data resample")
                self.LOG.error("%s", str(e))

            try:
                data_1hour = data['DateValue'].resample('60Min', base=15).ohlc()
                self.LOG.info("%s - 1Hour resampled data\n%s\n", stock, data_1hour.tail(10))
            except Exception as e:
                self.LOG.error("Realtime tick formation: Exception during 1Hour data resample")
                self.LOG.error("%s", str(e))


            if not CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].empty:
                try:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] \
                        = pd.concat([CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'], data_5min])
                    CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].sort_index(inplace=True)
                    CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'] = \
                        CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'][
                            ~CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].index.duplicated(keep='last')]
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - 5Min TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['5MIN']['TICKS'].tail(10))
                except Exception as er:
                    self.LOG.error("Realtime tick formation: Exception during resampling of 5Min data.")
                    self.LOG.error("%s", str(er))
                    continue

            if not CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].empty:
                try:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] \
                        = pd.concat([CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'], data_10min])
                    CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].sort_index(inplace=True)
                    CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'] = \
                        CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'][
                            ~CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].index.duplicated(keep='last')]
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - 10Min TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['10MIN']['TICKS'].tail(10))
                except Exception as er:
                    self.LOG.error("Realtime tick formation: Exception during resampling of 10Min data.")
                    self.LOG.error("%s", str(er))
                    continue

            if not CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].empty:
                try:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] \
                        = pd.concat([CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'], data_15min])
                    CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].sort_index(inplace=True)
                    CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'] = \
                        CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'][
                            ~CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].index.duplicated(keep='last')]
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - 15Min TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['15MIN']['TICKS'].tail(10))
                except Exception as er:
                    self.LOG.error("Realtime tick formation: Exception during resampling of 15Min data.")
                    self.LOG.error("%s", str(er))
                    continue

            if not CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].empty:
                try:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] \
                        = pd.concat([CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'], data_30min])
                    CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].sort_index(inplace=True)
                    CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'] = \
                        CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'][
                            ~CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].index.duplicated(keep='last')]
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - 30Min TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['30MIN']['TICKS'].tail(10))
                except Exception as er:
                    self.LOG.error("Realtime tick formation: Exception during resampling of 30Min data.")
                    self.LOG.error("%s", str(er))
                    continue

            if not CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].empty:
                try:
                    CONFIG.MUTEX.acquire()
                    CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] \
                        = pd.concat([CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'], data_1hour])
                    CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].sort_index(inplace=True)
                    CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'] = \
                        CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'][
                            ~CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].index.duplicated(keep='last')]
                    CONFIG.MUTEX.release()
                    self.LOG.info("%s - 1Hour TICKS\n%s\n", stock, CONFIG.MULTISTOCK[stock]['1HOUR']['TICKS'].tail(10))
                except Exception as er:
                    self.LOG.error("Realtime tick formation: Exception during resampling of 1Hour data.")
                    self.LOG.error("%s", str(er))
                    continue

            
        return


    def main_hist_data(self):
        self.fetch_hist_data(exchange='NSE_EQ')
        self.form_realtime_tick()
        return





def main():
    obj = Historical_Data()
    market_end_time = datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)
    CONFIG.UPSTOX_SESSION.get_master_contract('NSE_EQ')
    #CONFIG.UPSTOX_SESSION.get_master_contract('NSE_FO')
    #CONFIG.UPSTOX_SESSION.get_master_contract('MCX_FO')

    while True:
        Timer((15), obj.main_hist_data, []).run()

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
