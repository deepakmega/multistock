'''
Created on 27-Oct-2018

@author: suhaheer
'''

import pdb

import config as CONFIG
import time
import datetime
import threading
import ExchangeInterface
import threading
from threading import Timer
import logging
import math
import os
import sys
import multiprocessing
from multiprocessing import Process
from threading import Thread
import numpy as np
import talib
from talib import MA_Type
import requests
import pandas as pd
import bs4
from bs4 import BeautifulSoup
import re


pd.set_option('display.width', 320)
pd.set_option('display.max_columns', 100)


class Option_Chain:
    """
    Class: Option Chain, to analyse the current status of individual stock options.
    """
    LOG = None


    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/OptionChain-' + timestr + '.log',
                                      mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Option Chain")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)

    def parse_html(self,symbol):
        strike_diff = None
        Option_chain_main = None

        url = "https://www.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?symbol=" + symbol + ""

        while True:
            try:
                page = requests.get(url)
                if page:
                    break
            except:
                print("Exception: Failed to fetch the html page at url:", url)
                continue

        soup = BeautifulSoup(page.content, 'html.parser')

        ltp = soup.select('span b')
        symbol_ltp = float(re.sub("[^0-9.]", "", ltp[0].get_text()))

        main_table = soup.find_all(class_="opttbldata")
        tbl_cls_1 = soup.find_all(id="octable")

        col_list = []

        for tbl in tbl_cls_1:
            t_head = tbl.find("thead")

            try:
                rows = t_head.find_all("tr")
                for tr in rows:
                    cols = tr.find_all("th")
                    for th in cols:
                        er = th.text
                        col_list.append(er)
            except:
                self.LOG.error("Table do not have table head, for stock:%s", symbol)

        col_list_fnl = [e for e in col_list if e not in ("CALLS", "PUTS", "Chart", "\xc2", "\xa0")]

        tbl_cls_2 = soup.find(id="octable")
        all_trs = tbl_cls_2.find_all("tr")
        req_row = tbl_cls_2.find_all("tr")

        new_table = pd.DataFrame(index=range(0, len(req_row) - 3), columns=col_list_fnl)

        row_marker = 0

        for row_number, tr_nos in enumerate(req_row):
            # to enusure we use only rows with values
            if row_number <= 1 or row_number == len(req_row) - 1:
                continue

            td_columns = tr_nos.find_all('td')

            # this removes the graphs columns
            select_cols = td_columns[1:len(col_list_fnl) + 1]
            cols_horizontal = range(0, len(select_cols))

            for nu, column in enumerate(select_cols):
                str = column.get_text()
                str = str.strip('\n\r\t": ')
                tr = str.replace(',', '')
                if str in ['-']:
                    continue
                new_table.iloc[row_marker, [nu]] = tr
            row_marker += 1

        Option_chain = (new_table)

        # print(Option_chain)
        if Option_chain.empty:
            self.LOG.error("Option Chain for symbol=%s is empty!", symbol)
            return

        strike_diff = float(Option_chain['Strike Price'].get(1)) - float(Option_chain['Strike Price'].get(0))

        Option_chain.drop(['IV', 'LTP', 'Net Chng', 'BidQty', 'BidPrice', 'AskPrice', 'AskQty'], axis=1, inplace=True)

        Option_chain = Option_chain.fillna(0)

        Option_chain.columns = ['Call-OI', 'Call-chngInOi', 'Call-Vol', 'Strike',
                                'Put-Vol', 'Put-chngInOi', 'Put-OI']

        Option_chain = Option_chain.apply(pd.to_numeric, errors='coerce')

        Option_chain_main = (Option_chain)

        opt_chain = pd.DataFrame(index=range(0, len(req_row) - 3),
                                 columns=["Call-OI-today", "Strike", "Put-OI-today", "PCR"])
        opt_chain["Total-call"] = Option_chain["Call-OI"] + Option_chain["Call-chngInOi"]
        opt_chain["Total-put"] = Option_chain["Put-OI"] + Option_chain["Put-chngInOi"]
        opt_chain["Strike"] = Option_chain["Strike"]
        Option_chain["PCR"] = ((opt_chain["Total-put"]) / (opt_chain["Total-call"]))
        Option_chain["CPR"] = ((opt_chain["Total-call"]) / (opt_chain["Total-put"]))

        m_sup_idx = Option_chain['Put-OI'].idxmax()
        m_res_idx = Option_chain['Call-OI'].idxmax()
        d_sup_idx = Option_chain['Put-chngInOi'].idxmax()
        d_res_idx = Option_chain['Call-chngInOi'].idxmax()

        cur_strike = (int(int(symbol_ltp) / int(strike_diff)) * float(strike_diff))
        prev_strike = ((int(int(symbol_ltp) / int(strike_diff)) - 1) * float(strike_diff))
        next_strike = ((int(int(symbol_ltp) / int(strike_diff)) + 1) * float(strike_diff))

        Option_chain = Option_chain.loc[Option_chain["Strike"].isin([prev_strike, cur_strike, next_strike])]

        prev_strike_idx = Option_chain[Option_chain["Strike"] == prev_strike].index.values.astype(int)[0]
        cur_strike_idx = Option_chain[Option_chain["Strike"] == cur_strike].index.values.astype(int)[0]
        next_strike_idx = Option_chain[Option_chain["Strike"] == next_strike].index.values.astype(int)[0]


        Option_chain = Option_chain.assign(Direction="")
        for i in [prev_strike_idx, cur_strike_idx, next_strike_idx]:
            if (Option_chain.at[i, "Put-chngInOi"] > Option_chain.at[i, "Call-chngInOi"]):
                Option_chain.loc[i, "Direction"] = "UP"
            else:
                Option_chain.loc[i, "Direction"] = "DOWN"
        

        if Option_chain.empty:
            self.LOG.error("%s - Option is empty.")
            return -1

        CONFIG.MULTISTOCK[symbol]['Option_chain'] = Option_chain
        #self.LOG.info("\n%s\n", CONFIG.MULTISTOCK[symbol]['Option_chain'])

        return


    def option_chain_main(self):
        for id in CONFIG.TRADE_INSTRUMENT:
            self.parse_html(id)
            time.sleep(1)
        return



def main():
    obj = Option_Chain()
    market_end_time = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0,
                                                      microsecond=0)
    while True:
        Timer((1), obj.option_chain_main, []).run()

        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            if (datetime.datetime.now().replace(microsecond=0) >= market_end_time):
                print("The market has closed... ")
                return

    return


"""
if __name__ == '__main__':
    import config as CONFIG
    CONFIG.init()
    main()
"""