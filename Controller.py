import datetime, time
import config as CONFIG
import save2file
import logging
from threading import Timer

class Controller(object):
    """
    This is system controller class. It controllers every action required by the system.
    It can save data at a particular time. It can retrieve data at a particular time.
    It can talk to any external entities to retrieve or export data. It do not seek data
    from the exchange.
    """
    LOG = None

    def __init__(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/controller-' + timestr + '.log', mode='w')
        if CONFIG.SIMULATION_MODE:
            handler = logging.FileHandler(filename=CONFIG.STD_PATH + 'logs/Simulation/controller-' + timestr + '.log', mode='w')

        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("System Controller")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        return

    """
    The controller object will run every second.
    """
    def init__controller(self):
        """
        Set data export timer:
        :return:
        """
        if (CONFIG.trading_exchange == "NSE" or CONFIG.trading_exchange == "NFO"):
            starttime = datetime.datetime.now().replace(microsecond=0)
            endtime = datetime.datetime.now().replace(hour=CONFIG.CLOSE_HR, minute=CONFIG.CLOSE_MIN, second=0, microsecond=0)
            sec = int((endtime-starttime).total_seconds())
            if(sec>0):
                timer= Timer(sec, self.export_to_storage, ())
                timer.start()
            else:
                print("You have started the system after 3.30 so wont save any data")
        return

    def export_to_storage(self):
        if (CONFIG.save2file_flag):
            CONFIG.save2file_flag = False
            self.LOG.info("Saved relevant trading data at the time:%s", str(datetime.datetime.now()))
            save2file.save2file()

        return

    def import_from_storage(self):
        CONFIG.save2file_flag = True
        save2file.readFromFile()
        return

    def export_out(self):
        """
        Rest call to export out the relevant data outside.
        """

        return