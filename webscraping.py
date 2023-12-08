from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from decouple import config
import pandas as pd 
import numpy as np
from time import sleep
import csv

""" 
    Spotify will redirect all attempts to webscraping to default login page. 
    Use Selenium to dynamically login and continue webscraping all the Global charts INFO
    loginSpotify() sends by username and password and clicks submit when 
    fields are filled. 
"""
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
    addInput(driver, by=By.ID, value='login-username', text=username)
    addInput(driver, by=By.ID, value='login-password', text=password)
    clickButton(driver, by=By.ID, value='login-button')
    

def main(): 

    # Setup driver and configure username and password
    driver = webdriver.Chrome()
    driver.get("https://accounts.spotify.com/en/login")
    username = config("SPOTIFY_USERNAME")
    password = config("SPOTIFY_PASSWORD")
    sleep(2) # wait a few seconds between each webpage to load 


    loginSpotify(driver, username, password)
    sleep(2)


    # Navigate to a test webpage and begin scraping
    driver.get("https://charts.spotify.com/charts/view/regional-global-daily/2017-08-25")

    # Wait for the charts table to be present
    charts_table_selector = '[data-testid="charts-table"]'
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, charts_table_selector))
    )

    # Find and print elementss
    elements = driver.find_elements(By.CSS_SELECTOR, charts_table_selector)

    """
        elements: list with one entry -- the charts_table_selector
        Use different attributes like .text .tag to fetch information 
        charts_table_selector can use the tr element but use simple string parsing
        instead of working with html
    """

    chartEntries = elements[0].text

    

    # Add a delay before quitting to ensure webscraping was performed
    sleep(5)
    driver.quit()


if __name__ == "__main__":
        main()
