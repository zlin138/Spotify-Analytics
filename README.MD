#### Create Webscraping script for Spotify Analytics Dashboards

##### Environment and Setup
Install Anaconda and run this in the command line 
> conda env create -f environments.yaml

Create a .env file with the following attributes set
> SPOTIFY_USERNAME = 
> SPOTIFY_PASSWORD =

##### Code 
Code is not meant to be robust rather it extracts the intended elements and tags from spotify and parses it into csv file. 
The code parses the top 200 global charts from 2017-2023 you can adjust dates and regions as needed. This parses the top 200 artist 
region since the it's launch across all(well I filled in the gaps later). Note: the amount of requests is quite intense 

##### Results
Created Dashboards for Tableau. Not all data came directly from Spotify's official chart(in fact many did not and the distinction is obvious). 

Check out the results [here](https://public.tableau.com/app/profile/zhang.lin2425/viz/SpotifySwift/SpotifyDebutDashboard)
