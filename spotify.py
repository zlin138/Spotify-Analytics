import logging
import concurrent.futures
import json
import os
import csv
import re
import pandas as pd 
import numpy as np
from decouple import config
from datetime import timedelta
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from time import sleep

# --------------- Global Variables just for logging purposes------------------------
# Logging - Track information on data parsing status
def configure_logger(logger_name, log_file, level = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(filename)s - line: %(lineno)d - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_file, mode='w')
    # Modify this to filter the message displayed in log
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    # which messages are directed to log
    logger.setLevel(level)
    return logger

# Create handler for csv and webscraping process
csvLogger = configure_logger('csvLogger', 'log/csv.log')
scrapingLogger = configure_logger('spotifyLogger', 'log/spotify.log')

def createDriver(): 
    """ 
        Web-drivers are not thread safe -- they sort of resemable a thread themselve
        Therefore for each thread, we need to create a seperate webdriver
    """
    options = webdriver.ChromeOptions()
    # The headless option doesn't seem to be supported for webscraping spotify blocks it?
    # options.add_argument('--headless')
    # options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    return driver

def addInput(driver: webdriver, by: By, value: str, text: str):
    element = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((by, value))
    )
    element.send_keys(text)

#
def clickButton(driver: webdriver, by: By, value: str):
    button = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((by, value))
    )
    button.click()

def loginSpotify(driver: webdriver): 
    """ 
        Spotify will redirect all attempts to webscraping to default login page. 
        Use Selenium to dynamically login and continue webscraping all the Global charts INFO
        loginSpotify() sends by username and password and clicks submit when 
        fields are filled. 
    """
    username = config("SPOTIFY_USERNAME")
    password = config("SPOTIFY_PASSWORD")

    # Go to login page, wait for page to load, config and simulate login
    driver.get("https://accounts.spotify.com/en/login")
    addInput(driver, by=By.ID, value='login-username', text=username)
    addInput(driver, by=By.ID, value='login-password', text=password)
    clickButton(driver, by=By.ID, value='login-button')
    # Delay for button click to be processed and login processed
    sleep(3)

def createDate(startDate: str, endDate: str, dateFormat: str = '%Y-%m-%d') -> list:
    """ 
        Purpose: Returns a list of dates between the 
        specificed two inputs startDate and endDate 
        -- Used for webscraping to obtain data between during time interval
    """
    currentDate = datetime.strptime(startDate, dateFormat)
    endDate = datetime.strptime(endDate, dateFormat)

    dates = list()
    incrementTime  = timedelta(days=1)

    while(currentDate <= endDate):
        dates.append(currentDate.strftime(dateFormat))
        currentDate = currentDate + incrementTime

    return dates

def writeTop200Songs(input:str, filePath: str, mode: str, date: str, region: str):
    '''
        Takes a string input -- the elements from the webpage -- 
        parses the inputs and writes to CSV file
        
        Parameters: 
            filePath: specify name and location of file
            mode: "The mode of csv writer" 'a'(append) or 'w'(write)
    '''
    # Omit text up until 1 -- the first chart entry
    try:
        match = re.search("\n1\n", input)
        if(match):
            # Parsing format follows: position, position_change, track_title, artist_name, 
            # and one line string containing space seperated values peak, prev, streak, streams
            # Every 5 lines is a new song which should be an a new list
            csvLogger.info("Pattern found: data parsing begins")
            data = list()
            entryIndex = -1

            for i, entry in enumerate(input[match.start()+1:].split("\n")): 
                if i % 5 == 0: 
                    data.append(list())
                    entryIndex +=1 
                    data[entryIndex].extend([date, region])
                elif (i+1) % 5 == 0: 
                    data[entryIndex].extend(entry.split(' '))
                    continue
                data[entryIndex].append(entry)

            with open(filePath, mode, encoding='utf-8', newline='') as file: 
                csvLogger.info(f"Writing to file {filePath} with '{mode}' status for {date} - {region}")
                writer = csv.writer(file)
                if(mode == 'w'): 
                    header = ['date', 'region', 'position', 'change', 'track_title', 'artist_name',
                            'peak', 'prev', 'streak', 'streams']
                    writer.writerow(header)
                # Write the data rows
                writer.writerows(data)
        else: 
            csvLogger.warning("Pattern not matched: check webscraping status and inputs")
            return 
    except Exception as e: 
        csvLogger.fatal(f'An error occured when attempting to write to file {str(e)} for {date} {region}')

    
def getChartElement(driver) -> str: 
    '''
        Get the top 200 chart elements as a string 
        Regarding elements: There are various ways to parse the ouput 
        including using various html/css elements to break it down such as tr
        Here, using brute force string manipulating to avoid html jargon
    '''
    try: 
        # Wait for the charts table to be present
        charts_table_selector = '[data-testid="charts-table"]'
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, charts_table_selector))
        )
        elements = driver.find_elements(By.CSS_SELECTOR, charts_table_selector)
        stringInput = elements[0].text
        scrapingLogger.info("Top 200 songs found and returned")
        return stringInput
    except TimeoutException as e: 
        scrapingLogger.fatal("chart element not found -- page not loaded properly and or invalid navigation links" + 
                             f'error message {str(e)}')
        
def writeRegions():
    ''' 
        Write each region/country available on spotify charts to regions.json 
        Format example follows the example; Argentina: [ar, 2017/01/2017]
        First item in array is the country abbreviation and second line a date
    '''
    # Note the starting date here is used to check if the region had Spotify at that time
    # ie if it exists the date would match else the first available date is returned
    startDate = '2017-01-01'
    url = f'https://charts.spotify.com/charts/view/regional-global-daily/{startDate}'
    driver = createDriver()
    loginSpotify(driver)
    driver.get(url)
    try: 
        # Click the dropdown menu for Regions
        clickButton(driver, by=By.ID, value='entity-search')

        # Wait for popUp container to appear
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@data-popover-root="true"]'))
        )
        popUpContainer = driver.find_element(By.XPATH, '//*[@data-popover-root="true"]')
        scrapingLogger.info('Pop Up Container located')

        listBox = popUpContainer.find_elements(By.XPATH, '//ul[@role="listbox"]/li')
        #Create list to help populate the dictionary for json file later on
        regionName = list()
        regionLink = list()
        # Loop through each li element and extract text and data-key attribute
        for liElement in listBox:
            text = liElement.find_element(By.XPATH, './/*[contains(@class, "EntityOptionDisplay__TextContainer")]').text
            link = liElement.get_attribute('data-key')
            regionLink.append(link)
            regionName.append(text)
        scrapingLogger.info(f'All region Names and links retrieved \nregionNames:{regionName} \nregionLink{regionLink}')

        scrapingLogger.info("Begin parsing links for region.json")
        regionDict = dict()
        abbrPtrn = re.compile(r'(?<=-)\D+(?=-)')
        datePtrn = re.compile(r'\d{4}-\d{2}-\d{2}')
        for i, region in enumerate(regionName): 
            regionDict[region] = list()
            #Parse every link to get the region abbreviation and valid date
            abbrResult  = abbrPtrn.search(regionLink[i])
            dateResult = datePtrn.search(regionLink[i])
            regionDict[region].append(abbrResult.group(0))
            regionDict[region].append(dateResult.group(0))

        with open('region.json', 'w')as json_file:
            json.dump(regionDict, json_file, indent=2) 
        scrapingLogger.info('Wrote Dictionary to region.json')
    except TimeoutException as e: 
        scrapingLogger.fatal(f'Pop Up container not found {str(e)}')
    except NoSuchElementException as e :
        scrapingLogger.fatal(str(e))

def validRegions(regions:dict) -> list:
    pass


def chartHelper(driver: webdriver, dates:list):
    ''' 
        Helper function -- used to scrape a list of dates for multi-threading 
        Goes to spotify charts for a specific date and region and writes that to csv
        -- Currently aims to scrape the global charts starting from 2017
    '''
    # Fill in the actual url appended with the specified region and dates
    filePath = "spotifyChartsNew2.csv"
    region='global'
    startingDate = '2017-01-01'

    # Navigate to SpotifyCharts and grab the top 200 songs for that day and region
    for date in dates: 
        url = f'https://charts.spotify.com/charts/view/regional-{region}-daily/{date}'
        driver.get(url)
        scrapingLogger.info(f'Begin webscraping Daily Songs Chart for {date} {region}')
        top200Songs = getChartElement(driver)
        # Create file and write the first files with header -- append on subsequent iterations
        writeTop200Songs(top200Songs, filePath, 'w' if date == startingDate else 'a', date, region)
        scrapingLogger.info(f'Finished webscraping Daily Songs Chart for {date} {region}')

def spotifyDebut():
    pass 

def artistRank(): 
    pass


def main(): 

    jsonPath = 'region.json'
    if(not os.path.exists(jsonPath)): 
       writeRegions()
    with open(jsonPath, 'r') as json_file:
        regionDict = json.load(json_file)
    print(regionDict)

    # #Initialize needed variables and split dates into n-thread-partitions 
    # startDate = '2021-08-02'
    # endDate = '2023-12-13'
    # dates = createDate(startDate, endDate)
    # #dates = ['2020-06-11','2021-01-12', '2021-02-26', '2021-01-20', '2021-07-06', '2021-04-28', '2021-07-26', '2021-09-21','2021-11-18']
    # nThreads = 2
    # nDates = len(dates)//nThreads
    # dates = [dates[i * nDates:(i + 1) * nDates] for i in range(nThreads)]
    # drivers = [createDriver() for i in range(nThreads)]
    # [loginSpotify(driver) for driver in drivers]

    # with concurrent.futures.ThreadPoolExecutor(max_workers=nThreads) as executor:
    #     executor.map(chartHelper, drivers, dates)

    # [driver.quit() for driver in drivers]




if __name__ == "__main__":
        main()
