import json
import config as CONFIG
import datetime
import time
import fileinput


def save2file():
    if(len(CONFIG.tick) <= 200):
        fileData = {"TRUE_RANGE": CONFIG.TRUE_RANGE, "SMA_5PERIOD_VALUE": CONFIG.SMA_5PERIOD_VALUE,
                    "SMA_34PERIOD_VALUE": CONFIG.SMA_34PERIOD_VALUE,"SMA_1PERIOD_VALUE": CONFIG.SMA_1PERIOD_VALUE,
                    "SMA_14PERIOD_VALUE": CONFIG.SMA_14PERIOD_VALUE, "tick":CONFIG.tick
                    }
    else:
        fileData = {}
        tick = {}
        dt = datetime.datetime.now()
        i=1
        while(i<= 200):
            temp_tick = {}
            dict_key = str(dt.replace(hour=15, minute=30, second=0, microsecond=0) - datetime.timedelta(minutes=i*CONFIG.time_interval))
            temp_tick[dict_key] = CONFIG.tick.get(dict_key)
            tick.update(temp_tick)
            i = i + 1
        fileData = {"TRUE_RANGE": CONFIG.TRUE_RANGE, "SMA_5PERIOD_VALUE": CONFIG.SMA_5PERIOD_VALUE,
                    "SMA_34PERIOD_VALUE": CONFIG.SMA_34PERIOD_VALUE, "SMA_1PERIOD_VALUE": CONFIG.SMA_1PERIOD_VALUE,
                    "SMA_14PERIOD_VALUE": CONFIG.SMA_14PERIOD_VALUE, "tick": tick
                    }
    json.dump(fileData, open(CONFIG.STD_PATH+"configfiles/Saved_Data.txt", 'a'))
    fhand_writer = open(CONFIG.STD_PATH+"configfiles/Saved_Data.txt", "a")
    fhand_writer.write("\n")
    fhand_writer.close()
    print("Write to file is done.")

    return

def readFromFile():
    fhand_reader = open(CONFIG.STD_PATH+"configfiles/Saved_Data.txt", "r")
    num_lines = sum(1 for line in open(CONFIG.STD_PATH+'configfiles/Saved_Data.txt'))

    if not num_lines:
        print("No data to read from the stored memory.")
        return None
    lines = fhand_reader.readlines()
    value = lines[num_lines - 1]
    fhand_reader.close()
    truerange_dict = json.loads(value)
    CONFIG.TRUE_RANGE = truerange_dict['TRUE_RANGE']
    CONFIG.SMA_5PERIOD_VALUE = truerange_dict['SMA_5PERIOD_VALUE']
    CONFIG.SMA_34PERIOD_VALUE = truerange_dict['SMA_34PERIOD_VALUE']
    CONFIG.SMA_1PERIOD_VALUE = truerange_dict['SMA_1PERIOD_VALUE']
    CONFIG.SMA_14PERIOD_VALUE = truerange_dict['SMA_14PERIOD_VALUE']
    parsetickdata(truerange_dict['tick'])
    print("Successfully read the data from file and stored in memory.")
    return

def parsetickdata(tick):
    temp_tick = {}
    for key in tick:
        dt = datetime.datetime.strptime(key, '%Y-%m-%d %H:%M:%S')
        currenttime =  datetime.datetime.now()
        dt = str(dt.replace(day=currenttime.day, month= currenttime.month, year=currenttime.year))
        temp_tick[dt] = tick[key]
    currenttime = datetime.datetime.now()
    no_of_candles = len(tick)
    if (CONFIG.time_interval == 30):
        basecandle = currenttime.replace(hour=15, minute=15, second=0, microsecond=0)
    elif (CONFIG.time_interval == 60):
        basecandle = currenttime.replace(hour=15, minute=15 , second=0, microsecond=0)
    else:
        basecandle = currenttime.replace(hour=15, minute=30 , second=0, microsecond=0) - datetime.timedelta(minutes=CONFIG.time_interval)

    currenttime = datetime.datetime.now()
    if(currenttime.minute % CONFIG.time_interval == 0):
        currenttime = currenttime.replace(second=0, microsecond=0)
    else:
        while(currenttime.minute % CONFIG.time_interval !=0):
            currenttime = currenttime - datetime.timedelta(minutes=1)
        currenttime = currenttime.replace(second=0, microsecond=0)

    i = 1
    final_tick = {}
    while (i <= no_of_candles):
        if (i == 1):
            final_tick[str(currenttime)] = temp_tick[str(basecandle)]
        else:
            currenttime = currenttime - datetime.timedelta(minutes=CONFIG.time_interval)
            basecandle = basecandle - datetime.timedelta(minutes=CONFIG.time_interval)
            final_tick[str(currenttime)] = temp_tick[str(basecandle)]
        i = i + 1
    CONFIG.tick = final_tick
    return

def savecandletofile(data, timestamp):
    temp_arr = data.split(',')
    supertrend_val = CONFIG.SUPERTREND.get(timestamp, None)
    atr_val = CONFIG.AVG_TRUE_RANGE.get(timestamp, None)
    lr_val = CONFIG.LINEAR_REGRESSION.get(timestamp, None)
    data = data + "," +str(supertrend_val)+","+str(atr_val) +"," + str(lr_val)
    fhand_writer = open(CONFIG.STD_PATH + "configfiles/Saved_Candles.txt", "a")
    fhand_writer.write(data)
    fhand_writer.write("\n")
    fhand_writer.close()
    return


def updatecandletofile(data,timestamp):
    for line in fileinput.input(CONFIG.STD_PATH + 'configfiles/Saved_Candles.txt', inplace=True):
        if timestamp in line:
            supertrend_val = CONFIG.SUPERTREND.get(timestamp, None)
            atr_val = CONFIG.AVG_TRUE_RANGE.get(timestamp, None)
            lr_val = CONFIG.LINEAR_REGRESSION.get(timestamp, None)
            line = data + "," +str(supertrend_val)+","+str(atr_val) +"," + str(lr_val)
        print(line.rstrip())
    return