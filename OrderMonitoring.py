'''
Created on 30-Dec-2017

@author: kailah rudra
'''
import pdb
import config as CONFIG
import datetime
import requests
import json
import time
import ExchangeInterface
from Authentication import Authenticate
import logging
import threading
from threading import Timer

class Orderm(object):
    '''
    classdocs
    '''
    LOG = None
    def __init__(self):
        '''
        Constructor
        '''
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH+'logs/OrderMonitoring-'+timestr+'.log',mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/OrderMonitoring-' + timestr + '.log',
                                          mode='w')
        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("OrderMonitoring")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        pass

    def pending_order_manager(self):
        market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                        microsecond=0)

        if (CONFIG.SIMULATION_MODE):
            self.LOG.info("Running in Simulation mode. It will monitor the simulated trades for profit/loss.")


        while True:
            Timer(1, self.monitor, []).run()

            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                print("The market has closed... ")
                break

    def monitor(self):

        if (CONFIG.SIMULATION_MODE):
            self.monitor_simulation()
            return

        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
        url = 'https://api.kite.trade/orders/'
        var = "token " + CONFIG.API_KEY +":"+CONFIG.ACCESS_TOKEN
        headers = {
            "X-Kite-Version": '3',
            "Authorization": var
        }
        try:
            response = requests.get(url, headers=headers)
            json_data = json.loads(response.text)
        except Exception as e:
            self.LOG.error("Exception occured while retrieving the orders. Please check the connectivity..")
            self.LOG.error("Exception:%s", str(e))
            print("Exception at ordermonitoring:", str(e))
            return

        if(json_data['status'] == 'success'):
            #self.LOG.info("Orders retrieved")
            for x in json_data['data']:
                # if the order is complete then check whether the same is present in ORDER_MANAGER
                if (x['status'] == "COMPLETE" and x['order_id'] in CONFIG.ORDER_MANAGER):
                    temp_order = CONFIG.ORDER_MANAGER.get(x['order_id'])

                    if (temp_order['status'] != "COMPLETE" and temp_order['PARENTORDER'] == "YES"):
                        place_status = ExchangeInterface.placeorder(temp_order['QUANTITY'], temp_order['TARGET_TYPE'],
                                                         temp_order['TARGET_PRICE'], "NO", 0, temp_order['order_id']);
                        if(place_status == "success"):
                            self.LOG.info("Placed secondary order with type=%s, price=%s and order_id=%s",
                                          temp_order['TARGET_TYPE'],temp_order['TARGET_PRICE'],temp_order['order_id'])
                            temp_order['status'] = "COMPLETE"
                            CONFIG.ORDER_MANAGER[x['order_id']] = temp_order
                        elif(place_status == "fail"):
                            self.LOG.info("Placing secondary order failed and will be done in the next try")


                    # complete secondary order
                    if (temp_order['status'] != "COMPLETE" and temp_order['PARENTORDER'] == "NO"):
                        temp_order['status'] = "COMPLETE"
                        CONFIG.ORDER_MANAGER[x['order_id']] = temp_order
                        CONFIG.ORDER_MANAGER.pop(temp_order['REFERENCE'], None);
                        CONFIG.ORDER_MANAGER.pop(x['order_id'], None);

                # Check if any pending orders exist and they are not safe to keep open.
                if (x['status'] != "COMPLETE" and str(x['order_id']) in CONFIG.ORDER_MANAGER):
                    present_timestamp = datetime.datetime.now();
                    temp_order = CONFIG.ORDER_MANAGER.get(x['order_id'])
                    timestamp_diff = present_timestamp - temp_order['timestamp'];
                    minutes = (timestamp_diff.seconds + timestamp_diff.microseconds / 1000000) / 60
                    if (temp_order["PARENTORDER"] == "YES"):
                        if (minutes >= CONFIG.OrderCancelDeadline):
                            requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
                            url = "https://api.kite.trade/orders/" + CONFIG.order_variety + "/" + temp_order['order_id']
                            var = "token " + CONFIG.API_KEY + ":" + CONFIG.ACCESS_TOKEN
                            headers = {
                                "X-Kite-Version": '3',
                                "Authorization": var
                            }
                            response = requests.delete(url, headers=headers)
                            json_cancel_response = json.loads(response.text)
                            if(json_cancel_response['status'] == 'success'):
                                CONFIG.ORDER_MANAGER.pop(temp_order['order_id'], None)
                                self.LOG.info("Deleted order with order_id= %s", temp_order['order_id'])
                            else:
                                self.LOG.info("Order cancelled failed will be done in the next try")

        else:
            self.LOG.error("orders retrieval failed will try again...")

        return

    def monitor_simulation(self):
        for cur_idx in list(CONFIG.OPEN_ORDERS):
            order = CONFIG.OPEN_ORDERS.get(cur_idx)

            if order["transaction_type"] == "BUY":
                target_price = order['price'] + order['squareoff']
                stoploss = order['price'] - order['stoploss']
                if CONFIG.MARKET_LTP >= target_price:
                    CONFIG.OPEN_ORDERS.pop(cur_idx)
                    self.LOG.info("Target achieved. Order_id=%s, mkt_ltp=%f, price=%f, target=%f",
                                  order["order_id"], CONFIG.MARKET_LTP, order['price'], order['squareoff'])

                elif CONFIG.MARKET_LTP <= stoploss:
                    CONFIG.OPEN_ORDERS.pop(cur_idx)
                    self.LOG.error("Stoploss hit. Order_id=%s, mkt_ltp=%f, price=%f, target=%f",
                                   order["order_id"], CONFIG.MARKET_LTP, order['price'], order['squareoff'])

            elif order["transaction_type"] == "SELL":
                target_price = order['price'] - order['squareoff']
                stoploss = order['price'] + order['stoploss']
                if CONFIG.MARKET_LTP <= target_price:
                    CONFIG.OPEN_ORDERS.pop(cur_idx)
                    self.LOG.info("Target achieved. Order_id=%s, mkt_ltp=%f, price=%f, target=%f",
                                  order["order_id"], CONFIG.MARKET_LTP, order['price'], order['squareoff'])

                elif CONFIG.MARKET_LTP >= stoploss:
                    CONFIG.OPEN_ORDERS.pop(cur_idx)
                    self.LOG.error("Stoploss hit. Order_id=%s, mkt_ltp=%f, price=%f, target=%f",
                                   order["order_id"], CONFIG.MARKET_LTP, order['price'], order['squareoff'])



        return