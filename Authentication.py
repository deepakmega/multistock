import config
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
import json
import hashlib
import platform
import logging
import time
from selenium.webdriver.support import expected_conditions as expect
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import selenium.webdriver.support.expected_conditions as EC
import selenium.webdriver.support.ui as ui
import furl


class Authenticate(object):
    '''
    Authentication module.
    '''

    kite = None
    LOG = None

    def __init__(self):
        '''
        Constructor
        '''
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        timestr = time.strftime("%Y-%m-%d.%H%M%S")
        handler = logging.FileHandler(filename=config.STD_PATH+'logs/Authentication-'+timestr+'.log',mode='w')
        handler.setFormatter(formatter)
        self.LOG = logging.getLogger("Authentication")
        self.LOG.setLevel(logging.INFO)
        self.LOG.addHandler(handler)
        pass


    def get_platform(self, credential_dict):
        system_platform = platform.system()
        self.LOG.info("System platform is %s", system_platform)
        if (system_platform=='Windows'):
            return str(credential_dict['chromedriver_windows'])
        elif (system_platform=='Linux'):
            return str(credential_dict['chromedriver_linux'])
        elif (system_platform=='Darwin'):
            return str(credential_dict['chromedriver_mac'])
        else:
            self.LOG.info("Unsupported platform %s please verify", system_platform)
            return;


    def login(self):
        for attempt in range(1, 6):
            try:
                credentials_dict = json.load(open(config.STD_PATH+"configfiles/credentials.txt"))
                self.LOG.info("Credentials loaded from the file %s", credentials_dict)
                chromedriver_path = self.get_platform(credentials_dict)
                self.LOG.info("Chromedriver used is %s", chromedriver_path)

                #Set chrome options for hiding the chrome browser while login
                chrome_options = Options()
                chrome_options.add_argument("--headless")
                driver = webdriver.Chrome(executable_path=chromedriver_path, chrome_options=chrome_options)
                login_url="https://kite.trade/connect/login?api_key="+config.API_KEY+"&v=3"
                self.LOG.info("kite Login url used is %s", login_url)
                driver.get(login_url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='container']/div/div/div[2]/form/div[1]/input")))
                username = driver.find_element_by_xpath("//*[@id='container']/div/div/div[2]/form/div[1]/input");

                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='container']/div/div/div[2]/form/div[2]/input")))
                password = driver.find_element_by_xpath("//*[@id='container']/div/div/div[2]/form/div[2]/input");

                username.send_keys(credentials_dict['login_cred']['username'])
                password.send_keys(credentials_dict['login_cred']['password'])

                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='container']/div/div/div[2]/form/div[4]/button")))
                driver.find_element_by_xpath("//*[@id='container']/div/div/div[2]/form/div[4]/button").click()

                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[2]/div/input')))
                firstq = driver.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[2]/div/input').get_attribute('label')

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[3]/div/input')))
                secondq = driver.find_element_by_xpath(
                    '//*[@id="container"]/div/div/div[2]/form/div[3]/div/input').get_attribute('label')

                first = str(firstq)
                second = str(secondq)

                """
                if "Shampoo" in first:
                    firsta = credentials_dict['2fa']['Shampoo']
                elif "married" in first:
                    firsta = credentials_dict['2fa']['married']
                elif "grandmother" in first:
                    firsta = credentials_dict['2fa']['grandmother']
                elif "your mother" in first:
                    firsta = credentials_dict['2fa']['mother']
                elif "grandfather" in first:
                    firsta = credentials_dict['2fa']['grandfather']
        
                if "Shampoo" in second:
                    seconda = credentials_dict['2fa']['Shampoo']
                elif "married" in second:
                    seconda = credentials_dict['2fa']['married']
                elif "grandmother" in second:
                    seconda = credentials_dict['2fa']['grandmother']
                elif "your mother" in second:
                    seconda = credentials_dict['2fa']['mother']
                elif "grandfather" in second:
                    seconda = credentials_dict['2fa']['grandfather']
                """

                """
                if "Which floor of the building" in first:
                    firsta = credentials_dict['2fa']['Which floor of the building']
                elif "How many floors" in first:
                    firsta = credentials_dict['2fa']['How many floors']
                elif "shoe size" in first:
                    firsta = credentials_dict['2fa']['shoe size']
                elif "Which vehicle" in first:
                    firsta = credentials_dict['2fa']['Which vehicle']
                elif "which company" in first:
                    firsta = credentials_dict['2fa']['which company']
                
                if "Which floor of the building" in second:
                    seconda = credentials_dict['2fa']['Which floor of the building']
                elif "How many floors" in second:
                    seconda = credentials_dict['2fa']['How many floors']
                elif "shoe size" in second:
                    seconda = credentials_dict['2fa']['shoe size']
                elif "Which vehicle" in second:
                    seconda = credentials_dict['2fa']['Which vehicle']
                elif "which company" in second:
                    seconda = credentials_dict['2fa']['which company']
                """

                if "name of the college from which you graduated" in first:
                    firsta = credentials_dict['2fa']['name of the college from which you graduated']
                elif "your birth place" in first:
                    firsta = credentials_dict['2fa']['your birth place']
                elif "previous company you worked for" in first:
                    firsta = credentials_dict['2fa']['previous company you worked for']
                elif "brand of your first mobile" in first:
                    firsta = credentials_dict['2fa']['brand of your first mobile']
                elif "email service provider" in first:
                    firsta = credentials_dict['2fa']['email service provider']

                if "name of the college from which you graduated" in second:
                    seconda = credentials_dict['2fa']['name of the college from which you graduated']
                elif "your birth place" in second:
                    seconda = credentials_dict['2fa']['your birth place']
                elif "previous company you worked for" in second:
                    seconda = credentials_dict['2fa']['previous company you worked for']
                elif "brand of your first mobile" in second:
                    seconda = credentials_dict['2fa']['brand of your first mobile']
                elif "email service provider" in second:
                    seconda = credentials_dict['2fa']['email service provider']

                #answer1 = driver.find_element_by_name("answer1")
                #answer2 = driver.find_element_by_name("answer2")

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[2]/div/input')))
                firstq = driver.find_element_by_xpath(
                    '//*[@id="container"]/div/div/div[2]/form/div[2]/div/input')


                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[3]/div/input')))
                secondq = driver.find_element_by_xpath(
                    '//*[@id="container"]/div/div/div[2]/form/div[3]/div/input')


                firstq.send_keys(firsta)
                secondq.send_keys(seconda)

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div/div/div[2]/form/div[4]/button')))
                driver.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[4]/button').click()


                # Get the request token from the url
                WebDriverWait(driver, 10).until(lambda driver: driver.current_url.find("request_token=") != -1)
                redirect_url = str(driver.current_url)
                driver.close()
                self.LOG.info("Chrome driver closed")
                config.REQUEST_TOKEN = furl.furl(redirect_url).args['request_token']
                h = hashlib.sha256(config.API_KEY.encode("utf-8") + config.REQUEST_TOKEN.encode("utf-8") + config.API_SECRET.encode("utf-8"))
                checksum = h.hexdigest()

                #url for creating session
                url = 'https://api.kite.trade/session/token/'
                self.LOG.info("kite session url used is %s", url)
                payload = {
                    "api_key": config.API_KEY,
                    "request_token": config.REQUEST_TOKEN,
                    "checksum": checksum
                }

                headers = {
                    "X-Kite-Version": '3'
                }

                # Add the ciphers supported by the kite server and create a session
                requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ECDHE-RSA-AES256-GCM-SHA384'
                response = requests.post(url, data=payload, headers=headers)
                json_data = json.loads(response.text)
                if(json_data['status'] == 'success'):
                    config.ACCESS_TOKEN = json_data['data']['access_token']
                    config.PUBLIC_TOKEN = json_data['data']['public_token']
                    config.REFRESH_TOKEN = json_data['data']['refresh_token']
                    self.LOG.info("Kite session created successfully at attempt=%d "
                                  "after getting Access, Public and Refresh token", attempt)
                    print("kite session established successfully at attempt=", attempt)
                    return True
                else:
                    self.LOG.error("Kite session creation failed")
                    continue
            except:
                if attempt == 5:
                    self.LOG.error("All attempts failed. Exiting...")
                    return False

                else:
                    self.LOG.error("Attempt %d Failed to connect", attempt)
                    print("Authentication failed. Retry attempt=", attempt)
                    continue
        return True


