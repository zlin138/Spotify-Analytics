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
                    header = ['date', 'region', 'positon', 'change', 'track_title', 'artist_name',
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
        
def writeRegions(driver:webdriver):
    '''
        Write each region/country available on spotify charts to regions.json 
        Format example follows the example; Argentina: [ar, 2017/01/2017]
        First item in array is the country abbreviation and second line a date
    '''
    # Go to any spotify daily song chart page and specify a starting date
    # Note the starting date here is used to check if the region had Spotify at that time
    # For example, South Korea only had Spotify until 2020 thus Spotify would redirect to 
    # the first available date when clicking on it's dropdown menu
    startDate = '2017/01/01'
    url = 'https://charts.spotify.com/charts/view/regional-global-daily/{startDate}'
    driver.get(url)
    try: 
        # Click the dropdown menu for Regions
        clickButton(driver, by=By.ID, value='entity-search')

        # Wait for popUp container to appear
        popUpContainer = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'popover-container'))
        )
    except TimeoutException as e: 
        scrapingLogger.fatal(str(e))

    # Find all the items within the listbox
    try:
        listBox = popUpContainer.find_elements(By.XPATH, '//ul[@id="react-aria2894975124-2"]/li')

        regionName = list()
        regionLink = list()
        # Loop through each li element and extract text and data-key attribute
        for liElement in listBox:
            text = liElement.find_element(By.CLASS_NAME, 'EntityOptionDisplay__TextContainer-sc-1c8so1t-2').text
            link = liElement.get_attribute('data-key')
            regionLink.append(link)
            regionName.append(text)
    except NoSuchElementException as e :
        scrapingLogger.fatal(str(e))

    
def validRegions(regions:dict) -> list:
    pass


def chartHelper(driver, date, filePath, region='global', startingDate = '2017-01-01'):
    ''' 
        Helper function -- used in chartHistory()
        Goes to spotify charts for a specific date and region and writes that to csv
    '''
    # Fill in the actual url appended with the specified region and dates
    url = f'https://charts.spotify.com/charts/view/regional-{region}-daily/{date}'
    # Navigate to SpotifyCharts and grab the top 200 songs for that day and region
    driver.get(url)
    scrapingLogger.info(f'Begin webscraping Daily Songs Chart for {date} {region}')
    top200Songs = getChartElement(driver)
    # Create file and write the first files with header -- append on subsequent iterations
    writeTop200Songs(top200Songs, filePath, 'w' if date == startingDate else 'a', date, region)
    scrapingLogger.info(f'Finished webscraping Daily Songs Chart for {date} {region}')

def ChartHistory(driver, dates, filePath, region='global'):
    ''' 
        Multithreading to parallize the process each link is independent which 
        significantly benefits from this -- speeds up process by n folds depending on hardware
    '''
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Assuming you have a webdriver instance
        executor.map(lambda date: chartHelper(driver, date, filePath, region), dates)


def spotifyDebut():
    pass 

def artistRank(): 
    pass


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

def loginSpotify(driver: webdriver, username: str, password: str): 
    """ 
        Spotify will redirect all attempts to webscraping to default login page. 
        Use Selenium to dynamically login and continue webscraping all the Global charts INFO
        loginSpotify() sends by username and password and clicks submit when 
        fields are filled. 
    """
    addInput(driver, by=By.ID, value='login-username', text=username)
    addInput(driver, by=By.ID, value='login-password', text=password)
    clickButton(driver, by=By.ID, value='login-button')

def main(): 

    # Setup driver and configure username and password
    driver = webdriver.Chrome()
    username = config("SPOTIFY_USERNAME")
    password = config("SPOTIFY_PASSWORD")

    # Go to login page, wait for page to config and simulate login
    driver.get("https://accounts.spotify.com/en/login")
    sleep(2) 
    loginSpotify(driver, username, password)
    sleep(2)

    filePath = "spotifyChartsTest.csv"
    startDate = '2021-08-02'
    endDate = '2023-12-13'
    #dates = createDate(startDate, endDate)
    dates = ['2017-01-01', '2021-01-12', '2021-01-20', '2021-02-26', '2021-04-28', '2021-07-06', '2021-07-26']
    ChartHistory(driver, dates, filePath)

    # jsonPath = 'region.json'
    # if(~os.path.exists(jsonPath)): 
    #    writeRegions()
    # with open(jsonPath, 'r') as json_file:
    #     regionDict = json.load(json_file)
    

    
    # Add a delay before quitting to ensure webscraping was performed
    sleep(5)
    driver.quit()


if __name__ == "__main__":
        main()
