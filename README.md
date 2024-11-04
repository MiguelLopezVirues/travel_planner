# 🧳 Travel Planning and Flight Analysis
<div style="text-align: center;">
  <img src="assets/travel_scraping.png" alt="portada" />
</div>

## 📝 Project Overview

The purpose of this Travel Planner project is to lay out the foundation for a new travel app that will allow users to compare among different dates and cities, to select the flights, accommodations and activities for the best combination in dates and destinations. Collecting information from websites and API sources, the goal is to analyze one year data of possible weekend travel combinations to offer tailored travel options to customers, optimizing the selection of:

1. The best flight deals: Departure times, duration and price
2. The best accommodations: Price, distance to city centre and more.
3. The most convenient city and activities: Based on activities available in the city, weather and more.
4. The best dates for the trip: Based on a combination of the above factors.

Key questions we aim to address via data analysis to provide the above solutions include:

1. What are the price trends for flights and accommodations across different cities and weekends throughout the year?
2. How do flight durations compare between destinations and dates?
3. What is the cost of accommodation per city, week and type of accommodation?
4. What types of accommodation are more available across different cities?
5. What are the top-rated activities available in key destinations? What categories of activities does each city offer?
6. What is the weather forecast for a given city for the travel dates? What has been the weather history of the past few years for the envisioned date?



## 📝 Services and APIs used
The sources to obtain the data for this project feature 2 scraped websites and 4 API integrations

### Scraped services
- Booking: Accommodation information
- Civitatis: Activities available in each destination

### API integrations
- Air scrapper API: Airline itinerary information
- Google Geolocation API: Asynchronous fetching the addresses from latitude and longitude data (Freemium tier)
- Neonatim Geolocation API: Synchronous fecthing latitude and longitude (Free)
- Open Meteo API: Weather forecast/history

## 📁 Project Structure

```bash
Travel-Planner/
├── assets/
├── data/
│   ├── accommodations/
│   │   └── transformed/
│   ├── activities/
│   │   └── transformed/
│   │       └── activities.parquet
│   ├── airport_codes/
│   │   └── transformed/
│   │       └── countries_airports.csv
│   ├── flights/
│   │   └── transformed/
│   │       └── itineraries.parquet
│   └── weather/
│       └── transformed/
│           └── weather.parquet
├── notebooks/
│   ├── 1_data_extraction.ipynb
│   ├── 2_data_transformation.ipynb
│   └── 3_data_load.ipynb
├── src/
│   ├── data_etl.py
│   ├── data_extraction_support.py
│   ├── data_load_support.py
│   └── database_connection_support.py
├── .env
├── .gitignore
├── Pipfile
├── Pipfile.lock
└── README.md
```
## 🛠️ Installation and Requirements

This project requires the following tools and libraries:

- [pandas](https://pandas.pydata.org/docs/)  
- [numpy](https://numpy.org/doc/)  
- [selenium](https://selenium-python.readthedocs.io/)  
- [geopy](https://geopy.readthedocs.io/)  
- [requests](https://docs.python-requests.org/)  
- [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)  
- [webdriver-manager](https://github.com/SergeyPirogov/webdriver_manager)  
- [scipy](https://docs.scipy.org/doc/scipy/)  
- [matplotlib](https://matplotlib.org/stable/contents.html)  
- [seaborn](https://seaborn.pydata.org/)  
- [tqdm](https://tqdm.github.io/docs/tqdm/)  
- [ipykernel](https://ipython.readthedocs.io/en/stable/)   
- [asyncio](https://docs.python.org/3/library/asyncio.html)  
- [aiohttp](https://docs.aiohttp.org/)  
- [python-dotenv](https://saurabh-kumar.com/python-dotenv/)  
- [futures](https://docs.python.org/3/library/concurrent.futures.html)  
- [fastparquet](https://fastparquet.readthedocs.io/)  
- [pyarrow](https://arrow.apache.org/docs/python/)  
- [psycopg2](https://www.psycopg.org/docs/)  
- [psycopg2-binary](https://www.psycopg.org/docs/)  
- [unidecode](https://pypi.org/project/Unidecode/)  


### Setting up the Environment with Pipenv

Clone this repository by navigating to the desired folder with your command line and cloning the environment:

```bash
git clone https://github.com/MiguelLopezVirues/travel-planner
```

To replicate the project's environment, you can use Pipenv with the included `Pipfile.lock`:

```bash
pipenv install
pipenv shell  
```


## 🔄 Next Steps
1. Track multiple origins to be able to provide service for other cities as well as multi-destination travels
2. Program daily weather updates
3. Track flights, accommodations and activities prices over a month period of time to measure differences in data with respect to query date, time and day of the week
4. Implement streamlined ETL
5. Implement database updates
6. Create POC app in Streamlit


## 🤝 Contributions

Contributions are welcome. If you wish to improve the project, open a pull request or an issue.

## ✒️ Authors

Miguel López Virués - [GitHub Profile](https://github.com/MiguelLopezVirues)

## 📜 License

This project is licensed under the MIT License.


