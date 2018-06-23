import time
import datetime
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import matplotlib.animation as animation
from mpl_finance import candlestick_ohlc
from threading import Thread
import config
from sklearn import linear_model
from adddata import chartsimulator

lin_x = []
lin_y = []
supertrend_x = []
supertrend_y = []
matplotlib.rcParams.update({'font.size' : 9})

def movingaverage(values, window):
    weights = np.repeat(1.0 , window)/window
    smas = np.convolve(values , weights , 'valid')
    return smas

def rsiFunc(prices, n=14):
    deltas = np.diff(prices)
    seed = deltas[:n+1]
    up =  seed[seed>=0].sum()/n
    down = -seed[seed<0].sum()/n
    rs = up /down
    rsi = np.zeros_like(prices)
    rsi[:n] = 100. - 100./(1.+rs)

    for i in range(n, len(prices)):
        delta = deltas[i-1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up*(n-1)+upval)/n
        down = (down*(n-1)+downval)/n
        rs = up/down
        rsi[i] = 100. - 100./(1.+rs)
    return rsi

def lr_calculator(window):
    x = []
    y = []
    x = list(range(len(config.ohlc_data) - (window -1), len(config.ohlc_data) + 1))
    pt = x[-1]
    temp_ohlc_data = config.ohlc_data[-window:]
    for num in temp_ohlc_data:
        y.append(num[4])

    linear_mod = linear_model.LinearRegression()
    x = np.reshape(x, (len(x), 1))  # converting to matrix of n X 1
    y = np.reshape(y, (len(y), 1))
    linear_mod.fit(x, y)  # fitting the data points in the model
    res = linear_mod.predict(pt)
    lin_x.append(pt)
    lin_y.append(res[0][0])


def ExpMovingAverage(values, window):
    weights = np.exp(np.linspace(-1., 0., window))
    weights /= weights.sum()
    a = np.convolve(values,weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a

def computeMACD(x, slow=26, fast=12):
    emaslow = ExpMovingAverage(x, slow)
    emafast = ExpMovingAverage(x, fast)
    return emaslow, emafast , emafast-emaslow

def supertrend_calculator(window):

    supertrend_x.append()
    supertrend_y.append()
    return
def graphData(ma1 , ma2):
    if len(config.ohlc_data) > 0 and config.plotted==False:
        fig.clf()
        date = []
        openp = []
        highp = []
        lowp = []
        closep = []
        j = 1
        for eachcandle in config.ohlc_data:
            date.append(j)
            openp.append(eachcandle[1])
            highp.append(eachcandle[2])
            lowp.append(eachcandle[3])
            closep.append(eachcandle[4])
            j=j+1


        ohlc_data = []
        i = 1
        for line in config.ohlc_data:
            ohlc_data.append((i,
                              np.float64(line[1]), np.float64(line[2]), np.float64(line[3]),
                              np.float64(line[4])))
            i = i +1


        # subplot for candlestick
        ax1 = plt.subplot2grid((16,6) , (3, 0), rowspan=10, colspan=6)
        plt.ylabel("Stock Price")
        plt.xlabel("Time")
        plt.title("Nifty")

        candlestick_ohlc(ax1, ohlc_data, width=0.5 , colorup='g', colordown='r', alpha=0.8)
        #ax1.grid(True)
        ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))



        # MA
        if len(config.ohlc_data) > ma2:
            Av1 = movingaverage(closep, ma1)
            Av2 = movingaverage(closep, ma2)
            SP = len(date[ma2 - 1:])
            label1 = str(ma1) + ' SMA'
            label2 = str(ma2) + ' SMA'
            ax1.plot(date[-SP:], Av1[-SP:], 'red', label=label1)
            ax1.plot(date[-SP:], Av2[-SP:], 'green', label=label2)
            plt.legend(loc=4, ncol=3, prop={'size': 7}, fancybox=True)
            #plt.setp(ax0.get_xticklabels(), visible=False)

        # RSI
        if len(config.ohlc_data) > 14 :
            # ax0 - subplot for RSI
            ax0 = plt.subplot2grid((16, 6), (0, 0), sharex=ax1, rowspan=3, colspan=6)
            rsi = rsiFunc(closep)
            SP = len(date[ma2 - 1:])
            ax0.plot(date[-SP:], rsi[-SP:])
            ax0.axhline(70)
            ax0.axhline(30)
            ax0.set_yticks([30, 70])
            ax0.tick_params(axis='x')
            ax0.tick_params(axis='y')
            # plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='lower'))
            plt.ylabel('RSI')
            plt.setp(ax0.get_xticklabels(), visible=False)
            plt.title("Nifty")


        # linear regression
        if len(config.ohlc_data) > 14 :
            lr_calculator(14)
            ax1.plot(lin_x, lin_y, color='blue', label='middle')

        # MACD
        if len(config.ohlc_data) > 26:
            # ax2 - subplot for MACD
            ax2 = plt.subplot2grid((16, 6), (13, 0), sharex=ax1, rowspan=4, colspan=6)

            nslow = 26
            nfast = 12
            nema = 9

            emaslow , emafast , macd = computeMACD(closep)
            ema9 = ExpMovingAverage(macd, nema)

            ax2.plot(date[-SP:],macd[-SP:])
            ax2.plot(date[-SP:],ema9[-SP:])
            #ax2.fill_between(date, macd[-SP:]-ema9[-SP:], 0 ,alpha =0.5)
            ax2.tick_params(axis='x')
            ax2.tick_params(axis='y')
            ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='upper'))
            #plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
            plt.setp(ax1.get_xticklabels(), visible=False)
            plt.ylabel('MACD')
            plt.xlabel("Time")

        #plot supertrend
        if len(config.ohlc_data) > 7:
           '''
           supertrend_calculator(7)
           ax1.plot(supertrend_x, supertrend_y, color='green', label='middle')
           '''
           pass


        plt.subplots_adjust(left=.05, bottom=.05, right=.98, top=.97)
        config.plotted=True

    return

fig = plt.figure(facecolor='white')

def animate(i):
        graphData(2,13)

def main():
    config.init()
    t1 = Thread(target=chartsimulator)
    t1.start()
    index = 0
    while True:
        if (len(config.ohlc_data) > index and config.plotted == False):
            ani = animation.FuncAnimation(fig, animate)
            plt.show()
            index = index + 1
            config.plotted = True

    t1.join()
    return

if __name__ == "__main__":
    main()












