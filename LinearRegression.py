import pdb
import time
import config as CONFIG
import logging
import datetime
import time
import numpy as np
import ExchangeInterface
from threading import Timer
import sklearn
from sklearn import linear_model
import os


class LR_calculator(object):

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH+'logs/LR_calculator-' + timestr + '.log', mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/LR_calculator-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Linear Regression Manager")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

        self.tick = CONFIG.tick
        self.BUY_LIST = []
        self.SELL_LIST = []
        return

    def lr_predictor(self, cur_time):
        cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
        cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
        cur_idx = str(cur_mkt_idx)

        next_mkt_idx = cur_mkt_idx + datetime.timedelta(minutes=CONFIG.time_interval)
        next_idx = str(next_mkt_idx)

        candle_beginning_time = cur_mkt_idx.replace(second=10)
        candle_closing_time = cur_mkt_idx.replace(minute=cur_min + (CONFIG.time_interval - 1), second=55)

        prev_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
        prev_idx = str(prev_mkt_idx)

        cmt_time = datetime.datetime.now().replace(microsecond=0)
        if cur_idx in self.tick and cur_idx in CONFIG.SUPERTREND:
            if cmt_time >= candle_closing_time or cmt_time <= candle_beginning_time:
                if self.tick[cur_idx][3] < CONFIG.SUPERTREND[cur_idx] and \
                        CONFIG.LINEAR_REGRESSION[cur_idx] < CONFIG.SUPERTREND[cur_idx] and \
                            CONFIG.LINEAR_REGRESSION_PREDICTOR[cur_idx] > CONFIG.SUPERTREND[cur_idx]:
                    if cur_idx not in self.BUY_LIST:
                        self.BUY_LIST.append(cur_idx)
                        self.LOG.info("BUY Signal : LR Predicted value %f is above supertrend:%f at %s and cmp is %f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR[cur_idx], CONFIG.SUPERTREND[cur_idx], cur_idx , self.tick[cur_idx][3])

                if self.tick[cur_idx][3] > CONFIG.SUPERTREND[cur_idx] and \
                        CONFIG.LINEAR_REGRESSION[cur_idx] > CONFIG.SUPERTREND[cur_idx] and \
                            CONFIG.LINEAR_REGRESSION_PREDICTOR[cur_idx] < CONFIG.SUPERTREND[cur_idx]:
                    if cur_idx not in self.SELL_LIST:
                        self.SELL_LIST.append(cur_idx)
                        self.LOG.info("SELL Signal : LR Predicted value %f is below supertrend:%f at %s and cmp is %f",
                                      CONFIG.LINEAR_REGRESSION_PREDICTOR[cur_idx], CONFIG.SUPERTREND[cur_idx], cur_idx , self.tick[cur_idx][3])


        return

    def lr_calculate(self,period):
        if len(self.tick) < period:
            self.LOG.info("Required %d candles but available %d", period, len(self.tick))
        else:
            x = []
            temp_y = []
            cur_time = datetime.datetime.now().replace(microsecond=0)
            cur_min = cur_time.minute - (cur_time.minute % CONFIG.time_interval)
            cur_mkt_idx = cur_time.replace(minute=cur_min, second=0, microsecond=0)
            cur_idx = str(cur_mkt_idx)
            final_idx = str(cur_mkt_idx)

            for i in range(1, period+1):
                    x.append(i)
                    while(cur_idx not in self.tick):
                        pass
                    temp_y.append(self.tick[cur_idx][3])
                    cur_mkt_idx = cur_mkt_idx - datetime.timedelta(minutes=CONFIG.time_interval)
                    cur_idx = str(cur_mkt_idx)

            y = temp_y[::-1]
            pt = x[-1]

            linear_mod = linear_model.LinearRegression()
            x = np.reshape(x, (len(x), 1))  # converting to matrix of n X 1
            y = np.reshape(y, (len(y), 1))
            linear_mod.fit(x, y)  # fitting the data points in the model
            res = linear_mod.predict(pt)
            CONFIG.LINEAR_REGRESSION[final_idx] = (float(format(res[0][0],'.2f')))
            res_fut = linear_mod.predict(pt+1)
            CONFIG.LINEAR_REGRESSION_PREDICTOR[final_idx] = (float(format(res_fut[0][0], '.2f')))
            self.LOG.info("LR value for (%d period) at %s is %f", period , final_idx, CONFIG.LINEAR_REGRESSION[final_idx])
            self.LOG.info("LR Future value for (%d period) at %s is %f", period , final_idx, CONFIG.LINEAR_REGRESSION_PREDICTOR[final_idx])
            #print(CONFIG.LINEAR_REGRESSION)

            self.lr_predictor(cur_time)
            return


def main():
    obj = LR_calculator()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                      microsecond=0)
    while True:
        Timer(1, obj.lr_calculate,[5,]).run()

        if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
            print("The market has closed... ")
            return

    return