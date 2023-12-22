import logging
import concurrent.futures
from threading import Barrier
from functools import partial
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
    # headless option use with caution can have authentication issues when multithreading and can be blocked
    # options.add_argument('--headless')
    # options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options)
    return driver

def addInput(driver: webdriver, by: By, value: str, text: str):
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((by, value))
    )
    element.clear()
    element.send_keys(text)

#
def clickButton(driver: webdriver, by: By, value: str):
    button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((by, value))
    )
    button.click()

def loginSpotify(driver: webdriver, barrier, maxRetries = 1): 
    """ 
        Spotify will redirect all attempts to webscraping to default login page. 
        Use Selenium to dynamically login and continue webscraping all the Global charts INFO
        loginSpotify() sends by username and password and clicks submit when 
        fields are filled. 
    """
    username = config("SPOTIFY_USERNAME")
    password = config("SPOTIFY_PASSWORD")

    # Go to login page, wait for page to load, config and simulate login
    for iter in range(maxRetries+1):
        driver.get("https://accounts.spotify.com/en/login")
        try:
            addInput(driver, by=By.ID, value='login-username', text=username)
            addInput(driver, by=By.ID, value='login-password', text=password)
            clickButton(driver, by=By.ID, value='login-button')
            # Delay for button click to be processed and login processed - adjust as needed
            login = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="status-logged-in"]'))
            )
            if login: 
                barrier.wait()
                scrapingLogger.debug("Successfully Login to spotify")
                return 
        except TimeoutException as e:
            if iter < maxRetries:
                 scrapingLogger.warning(f"Attempt {iter + 1}: Login webpage did not load. Retrying...")
            else:
                scrapingLogger.fatal("Maximum retries reached. Could not authenticate login.")
                return  
            

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

def writeTop200Charts(input:str, filePath: str, mode: str, date: str, region: str, type:str):
    '''
        Takes a string input -- the elements from the webpage -- 
        parses the inputs and writes to CSV file
        specify which type of chart -- only interested in daily song and artist
        the table structure varies slightly across different charts modify accordingly

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
            # Every 5 lines is a new song which should be an a new list -- varies based on chart page
            csvLogger.info("Pattern found: data parsing begins")
            data = list()
            entryIndex = -1
            modBy = 4
            if type == 'song': 
                modBy = 5

            for i, entry in enumerate(input[match.start()+1:].split("\n")): 
                if i % modBy == 0: 
                    data.append(list())
                    entryIndex +=1 
                    data[entryIndex].extend([date, region])
                elif (i+1) % modBy == 0: 
                    data[entryIndex].extend(entry.split(' '))
                    continue
                data[entryIndex].append(entry)

            with open(filePath, mode, encoding='utf-8', newline='') as file: 
                csvLogger.info(f"Writing to file {filePath} with '{mode}' status for {date} - {region}")
                writer = csv.writer(file)
                if(mode == 'w'): 
                    # With multithreading it's unnecessary complex for write options but its here
                    # it's easier to just add the headers manually before writing to file
                    header = ['date', 'region', 'position', 'change', 'track_title', 'artist_name',
                            'peak', 'prev', 'streak', 'streams']
                    if type == 'artist': 
                        header = ['date', 'region', 'position', 'change', 'artist_name',
                            'peak', 'prev', 'streak']
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
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, charts_table_selector))
        )
        elements = driver.find_elements(By.CSS_SELECTOR, charts_table_selector)
        stringInput = elements[0].text
        scrapingLogger.info("Top 200 songs found and returned")
        return stringInput
    except TimeoutException as e: 
        scrapingLogger.fatal("chart element not found -- page not loaded properly and or invalid navigation links")
        
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

def validRegions(regions:dict, date:str) -> dict:
    ''' 
        Takes the regionDict and a date
        Returns the valid regions based on specified date as a dictionary of region name: region abbreviation
    '''
    validRegions = dict()
    
    for region in regions.keys(): 
        values = regions[region]
        if values[1] <= date: 
            validRegions[region] = values[0]
    return validRegions

def chartHelper(driver: webdriver, dates:list):
    ''' 
        Helper function -- used to scrape a list of dates for multi-threading 
        Goes to spotify charts for a specific date and region and writes that to csv
        -- Currently aims to scrape the global charts starting from 2017
    '''
    # Fill in the actual url appended with the specified region and dates
    filePath = "spotifyChartsTest.csv"
    region='global'
    startingDate = '2017-01-01'
    for date in dates: 
        # Navigate to SpotifyCharts and grab the top 200 songs for that day and region
        url = f'https://charts.spotify.com/charts/view/regional-{region}-daily/{date}'
        driver.get(url)
        scrapingLogger.info(f'Begin webscraping Daily Songs Chart for {date} {region}')
        top200Songs = getChartElement(driver)
        # Create file and write the first files with header -- append on subsequent iterations
        writeTop200Charts(top200Songs, filePath, 'w' if date == startingDate else 'a', date, region, 'song')
        scrapingLogger.info(f'Finished webscraping Daily Songs Chart for {date} {region}')

def spotifyDebut(driver:webdriver, dates:list, regionDict:dict):
    '''
        Get the performance of an album on Spotify Charts
        Do some data cleaning to map songs to albums 
        -- Full week performance across all avilable regions for release date
    '''
    dateFormat = '%Y-%m-%d'
    filePath = 'spotifyDebutTest.csv'
    for date in dates: 
        print(date)
        regions = validRegions(regionDict, date)
        for region, abbr in regions.items():
            # Get the whole week's data for album debut
            incrementTime  = timedelta(days=6)
            endDate = datetime.strftime(datetime.strptime(date, dateFormat) + incrementTime, dateFormat)
            week = createDate(date, endDate)
            for day in week: 
                url = f'https://charts.spotify.com/charts/view/regional-{abbr}-daily/{day}'
                driver.get(url)
                scrapingLogger.info(f'Spotify Debut: begin webscraping Daily Songs Chart for {day} {region}')
                top200Songs = getChartElement(driver)
                # Create file and write the first files with header -- append on subsequent iterations
                writeTop200Charts(top200Songs, filePath, 'a', day, region, 'song')
                scrapingLogger.info(f'Finished webscraping Daily Songs Chart for {day} {region}')


def artistRank(driver:webdriver, dates:list, regionDict:dict): 
    filePath = 'spotifyArtistRankNew.csv'
    for date in dates: 
        for region, abbr in regionDict.items(): 
            url = f'https://charts.spotify.com/charts/view/artist-{abbr}-daily/{date}'
            driver.get(url)
            scrapingLogger.info(f'Begin webscraping Daily Artist Chart for {date} {region}')
            top200Artist = getChartElement(driver)
            writeTop200Charts(top200Artist, filePath, 'a', date, region, 'artist')
            scrapingLogger.info(f'Finished webscraping Artist Songs Chart for {date} {region}')


def main(): 
    jsonPath = 'region.json'
    if(not os.path.exists(jsonPath)): 
       writeRegions()
    with open(jsonPath, 'r') as json_file:
        regionDict = json.load(json_file)
    # Initialize needed variables and split dates into n-thread-partitions 
    startDate = '2022-01-27'
    endDate = '2022-04-01'
    #albumDates = ['2019-08-23','2020-7-24', '2020-12-11', '2021-04-09','2021-11-12', '2022-10-21', '2023-07-07', '2023-10-27']
    dates = createDate(startDate, endDate)
    nThreads = 2
    # Not the best way but the most intuitive way to do this and avoid threading issues
    nDates = len(dates)//nThreads
    dates = [dates[i * nDates:(i + 1) * nDates] if i != nThreads-1 else dates[i * nDates:] for i in range(nThreads)]
    barrier = Barrier(nThreads)
    # Create n drivers for each thread
    drivers = [createDriver() for _ in range(nThreads)]

    # For brevity just omit entries without spotify since it's launch of artist chart
    artistRegions = validRegions(regionDict, '2021-10-21')
    partialSpotifyDebut = partial(spotifyDebut, regionDict = regionDict)
    partialArtistRank = partial(artistRank, regionDict = artistRegions)
    with concurrent.futures.ThreadPoolExecutor(max_workers=nThreads) as executor:
        executor.map(lambda driver: loginSpotify(driver, barrier), drivers)
        #executor.map(chartHelper, drivers, dates)
        #executor.map(partialSpotifyDebut, drivers, dates)
        executor.map(partialArtistRank, drivers, dates)

    [driver.quit() for driver in drivers] 

if __name__ == "__main__":
        main()
