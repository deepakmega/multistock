
import config as CONFIG
import re
import pdb
import json
import datetime, time


def main():
    count =0
    fileData = ""
    timestr = time.strftime("%Y-%m-%d.%H%M%S")
    timestr = "crude-2018-07-12.100000"
    fhand_writer = open(CONFIG.STD_PATH + "configfiles/simulation-"+timestr+".txt", "a")
    with open(CONFIG.STD_PATH+"../DataFetcher-crude-2018-07-12.100000.log",'r') as file:
        for line in file:
            out = re.search('ltp=(\d+)(\.)(\d+)', line)
            if out:
                if count < 1000000:
                    print(out.group(0))
                    count = count + 1
                    fhand_writer.write(str(out.group(0)))
                    fhand_writer.write("\n")

    fhand_writer.close()

if __name__ == '__main__':
    main()