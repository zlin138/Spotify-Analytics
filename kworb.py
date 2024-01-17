import requests
from bs4 import BeautifulSoup
import csv
import re

def main(): 
    # Kworb stores an artist top tracks including their top daily songs and total stream count
    # This is a static webpage which works well with BeautifulSoup 
    url = 'https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html'
    response = requests.get(url)

    # Check status code HTML 200 = success else failed 
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # Want to find the date text thats surrounded between multiple <br> tags
        line_break = soup.find('br')
        parent_element = line_break.find_parent()

        # Webpage is quite bare bone there aren't any distinct tags 
        # Basically get all the values until the actual table data and parse it with regex
        # This will help document the file name
        text = ""
        for content in parent_element.contents:
            if content.name == 'table':
                break
            text += str(content)
        else:
            print('<br> tags not found -- check webpage/url link')
        
        # Find and extract the date
        date_pattern = re.compile(r'Last updated: (\d{4}/\d{1,2}/\d{1,2})')
        match = date_pattern.search(text)
        if match:
            extracted_date = match.group(1)
        else:
            print('Date not found/format mismatch')
        

        # Now actually parsing the data of interest - consist of table with three columns track name, streams, and daily gains
        table = soup.find('table', class_='addpos sortable')

        if table:
            # Use date to name files(webpage updates daily)
            filename = extracted_date.replace("/", "-")
            with open(f'dailyStream/{filename}.csv', 'w', newline='', encoding= 'utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Extract rows and parses individual elements and write to csv
                rows = table.find_all('tr')
                for row in rows:
                    csv_writer.writerow([cell.text.strip() for cell in row.find_all(['td', 'th'])])
            print(f"Data written for {filename}") # Do note this would be data for the day before
        else:
            print('Table not found-- inspect the tags')
    else:
        print(f'Failed to retrieve the webpage with status code {response.status_code}')

if __name__ == "__main__":
        main()
