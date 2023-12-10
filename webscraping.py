import logging
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
from time import sleep

# Logging - Track information on data parsing status
logging.basicConfig(level=logging.DEBUG, format='%(filename)s - line: %(lineno)d - %(levelname)s  - %(message)s', 
                    handlers=[logging.FileHandler('csv.log', mode='w')])
csvLogger = logging.getLogger()

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

def writeCSV(input:str, filePath: str, mode: str):
    '''
        Takes a string input -- the elements from the webpage -- 
        parses the inputs and writes to CSV file
        
        Parameters: 
            filePath: specify name and location of file
            mode: "The mode of csv writer" 'a'(append) or 'w'(write)
    '''
    # Omit text up until 1 -- the first chart entry
    match = re.search("\n1\n", input)
    if(match):
        # Parsing format follows: position, position_change, track_title, artist_name, 
        # and one line string containing space seperated values peak, prev, streak, streams
        # Every 5 lines is a new song which should be an a new list
        csvLogger.info("Pattern found: data parsing begins")

        data = list()
        entryIndex = -1

        for counter, entry in enumerate(input[match.start()+1:].split("\n")): 
            if counter % 5 == 0: 
                data.append(list())
                entryIndex +=1 
            elif (counter+1) % 5 == 0: 
                data[entryIndex].extend(entry.split(' '))
                continue
            data[entryIndex].append(entry)

        with open(filePath, mode, encoding='utf-8', newline='') as file: 
            csvLogger.info(f'Writing to file {filePath} with {mode} status')
            writer = csv.writer(file)
            if(mode == 'w'): 
                header = ['positon', 'change', 'track_title', 'artist_name'
                          'peak', 'prev', 'streak', 'streams']
                writer.writerow(header)
            # Write the data rows
            writer.writerows(data)
    else: 
        csvLogger.warning("Pattern not matched: check webscraping status and inputs")
        return 
        
def createDate(startDate: str, endDate: str, dateFormat: str = '%Y-%m-%d'):
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

    filePath = "spotify.csv"
    startDate = '2017-01-01'
    endDate = '2017-01-02'
    region = 'global'
    dates = createDate(startDate, endDate)
    
    for index, date in enumerate(dates): 
        # Fill in the actual url appended with the specifed region and dates
        url = f'https://charts.spotify.com/charts/view/regional-{region}-daily/{date}'
        # Navigate to SpotifyCharts and grab the top 200 songs for that day and region
        driver.get(url)

        # Wait for the charts table to be present
        charts_table_selector = '[data-testid="charts-table"]'
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, charts_table_selector))
        )
        # Regarding elements: There are various ways to parse the ouput 
        # including using various html/css elements to break it down
        # Here, using brute force string manipulating to avoid html jargon
        elements = driver.find_elements(By.CSS_SELECTOR, charts_table_selector)
        stringInput = elements[0].text

        # Create file and write the first entry -- append on subsequent iterations
        if index == 0: 
            writeCSV(stringInput, filePath, mode = 'w')
        else:
            writeCSV(stringInput, filePath, mode = 'a')

    # Add a delay before quitting to ensure webscraping was performed
    sleep(5)
    driver.quit()


if __name__ == "__main__":
        main()
