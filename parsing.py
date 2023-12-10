import pandas as pd 
import numpy as np 
import logging
import re
import csv
import datetime as dt
from datetime import timedelta
from datetime import datetime

# Track information on data parsing status
logging.basicConfig(level=logging.DEBUG, format='%(filename)s - line: %(lineno)d - %(levelname)s  - %(message)s', 
                    handlers=[logging.FileHandler('csv.log', mode='w')])
csvLogger = logging.getLogger()

def writeCSV(input:str, filePath: str, mode: str):
    '''
        Takes a string input -- the elements from the webpage -- 
        parses the inputs and writes to CSV file
        
        Parameters: 
            filePath: specify name and location of file
            mode: "The mode of csv writer" 'a' or 'w' 
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
    startDate = '2017-01-01'
    endDate = '2017-01-02'
    print(createDate(startDate, endDate))
        

if __name__ == "__main__": 
    main()

