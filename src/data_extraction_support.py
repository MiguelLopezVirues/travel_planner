# data processing
import pandas as pd
import numpy as np

## Scraping
# Webdriver automation
from selenium import webdriver 
from webdriver_manager.chrome import ChromeDriverManager  
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
# html parsing
from bs4 import BeautifulSoup
# make synchronous request
import requests

# math operations
import math

# work with dates and time
import time
import datetime

# # work with asynchronicity
import asyncio
import aiohttp

# work with concurrency
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import json

# environment variables
import dotenv
import os
dotenv.load_dotenv()
AIR_SCRAPPER_API_KEY = os.getenv("AIR_SCRAPPER_KEY")
GOOGLE_API = os.getenv("GOOGLE_API_KEY")
BASE_URL_FORECAST = os.getenv("BASE_URL_FORECAST")
BASE_URL_ARCHIVE = os.getenv("BASE_URL_ARCHIVE")

# import support functions
import sys 
sys.path.append("..")

# function typing
from typing import List, Optional

# regular expressions
import re



### Cities 
def create_country_airport_code_df(list_of_countries):
    
    list_of_countries_airports = []

    for country in list_of_countries:

        url = "https://sky-scrapper.p.rapidapi.com/api/v1/flights/searchAirport"

        querystring = {"query": country,"locale":"en-US"}

        headers = {
            "x-rapidapi-key": AIR_SCRAPPER_API_KEY,
            "x-rapidapi-host": "sky-scrapper.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)

        response_data = response.json()["data"]
        list_of_countries_airports.extend(get_country_airport_codes(response_data,country))
    
    countries_airports = pd.DataFrame(list_of_countries_airports)
    
    return countries_airports


def get_country_airport_codes(response_data,country):

    airport_data_filtered = list(filter(lambda dictionary: True if dictionary["navigation"]["entityType"] == "AIRPORT" else False,response_data))

    airport_codes_dict_list = list(map(lambda dictionary: map_airport_codes(dictionary, country), airport_data_filtered))

    return airport_codes_dict_list


def map_airport_codes(dictionary,country):

    navigation = dictionary["navigation"]

    result_dict = dict()
    result_dict["country"] = country
    
    result_dict_assigner = {
        "city": lambda nav: nav["relevantHotelParams"]["localizedName"],
        "city_entityId": lambda nav: nav["relevantHotelParams"]["entityId"],
        "airport_skyId": lambda nav: nav["relevantFlightParams"]["skyId"],
        "airport_entityId": lambda nav: nav["relevantFlightParams"]["entityId"],
        "airport_name": lambda nav: nav["relevantFlightParams"]["localizedName"]
    }

    for key, function in result_dict_assigner.items():
        try:
            result_dict[key] = function(navigation)
        except:
            result_dict[key] = np.nan
    return result_dict


async def get_lat_lon(city, country="Spain"):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "city": city,
        "country": country,
        "format": "json",
        "limit": 1
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 429:
                print(f"Rate limit hit for {city}. Retrying...")
                await asyncio.sleep(1)  # Wait for a second before retrying
                return await get_lat_lon(city)  # Retry the same request
            elif response.status == 200:
                data = await response.json()
                if data:
                    lat = data[0]["lat"]
                    lon = data[0]["lon"]
                    return city, lat, lon
            return city, None, None

async def get_cities_coordinates(cities_list): 

    tasks = [get_lat_lon(city) for city in cities_list]
    results = await asyncio.gather(*tasks)

    return results


### FLIGHTS - air scrapper - API
from typing import List, Dict, Union
import pandas as pd

async def get_flights(
    countries_airports: Dict[str, str],
    origin_city: str,
    destination_cities: List[str],
    start_date: str = "2024-11-01",
    n_steps: int = 3,
    step_length: int = 7,
    days_window: int = 2,
    n_adults: int = 1,
    n_children: int = 0,
    n_infants: int = 0,
    origin_airport_code: bool = True,
    destination_airport_code: bool = True,
    sort_by: str = "price_high",
    currency: str = "EUR"
) -> pd.DataFrame:
    """
    Asynchronously retrieves flight itineraries based on search criteria,
    organizes them in a DataFrame, and saves the data to a Parquet file.

    Parameters:
    - countries_airports (Dict[str, str]): Dictionary mapping country names to their airport codes.
    - origin_city (str): Name of the origin city.
    - destination_cities (List[str]): List of destination city names.
    - start_date (str): The start date of the itinerary search in 'YYYY-MM-DD' format.
    - n_steps (int): Number of date steps to search over.
    - step_length (int): Interval in days between each date step.
    - days_window (int): Window in days around each date step for flexibility in flight dates.
    - n_adults (int): Number of adult passengers.
    - n_children (int): Number of child passengers.
    - n_infants (int): Number of infant passengers.
    - origin_airport_code (bool): Whether to include the origin airport code in the query.
    - destination_airport_code (bool): Whether to include the destination airport code in the query.
    - sort_by (str): Criterion to sort the results by (e.g., 'price_high').
    - currency (str): Currency code for the results (e.g., 'EUR').

    Returns:
    - pd.DataFrame: DataFrame containing the flattened itinerary data, saved to a Parquet file.
    """
    
    querystrings_list = build_flight_request_querystring_list_single(
        countries_airports,
        origin_city,
        destination_cities,
        start_date,
        n_steps=n_steps,
        step_length=step_length,
        days_window=days_window,
        n_adults=n_adults,
        n_children=n_children,
        n_infants=n_infants,
        origin_airport_code=origin_airport_code,
        destination_airport_code=destination_airport_code,
        sort_by=sort_by,
        currency=currency
    )
    
    itineraries_dict_list = await request_flight_itineraries_async_multiple(querystrings_list)
    
    itineraries_dict_list_flat = [itinerary_dict for dict_list in itineraries_dict_list if dict_list for itinerary_dict in dict_list]
    itineraries_df = create_itineraries_dataframe(itineraries_dict_list_flat)
    
    itineraries_df.to_parquet("../data/flights/itineraries.parquet")

    return itineraries_df



# for double way - not used at the moment
def build_flight_request_querystring_double(countries_airports_df,origin_city,destination_cities_list, date_query_start, n_steps=52, step_length=7, days_window=2, n_adults= 1, n_children=0, n_infants=0, origin_airport_code=None, 
                                   destination_airport_code=None, cabin_class="economy",sort_by="best",currency="EUR"):
    """This function generates a list of querystrings from the input params, that is used to later generate a list of I/O taks to a flights API.
    It generates querystrings for go and return flights both on the same day, iterating from the date_query_start to the end of the days window.
    This allows to map all flights from the selected origin to the possible destinations, for the time window selected (defaulted to 1 year).

    Args:
        countries_airports_df (_type_): _description_
        origin_city (_type_): _description_
        destination_cities_list (_type_): _description_
        date_query_start (_type_): _description_
        date_query_end (_type_): _description_
        n_adults (int, optional): _description_. Defaults to 1.
        n_children (int, optional): _description_. Defaults to 0.
        n_infants (int, optional): _description_. Defaults to 0.
        origin_airport_code (_type_, optional): _description_. Defaults to None.
        destination_airport_code (_type_, optional): _description_. Defaults to None.
        cabin_class (str, optional): _description_. Defaults to "economy".
        sort_by (str, optional): _description_. Defaults to "best".
        currency (str, optional): _description_. Defaults to "EUR".
    """
    date_query_start_datetime = datetime.datetime.strptime(date_query_start, "%Y-%m-%d")

    querystring_list = []
    for destination_city in destination_cities_list:
        for step in range(n_steps):
            date_departure = (date_query_start_datetime + datetime.timedelta(days=step*step_length)).strftime("%Y-%m-%d")
            date_return = (date_query_start_datetime + datetime.timedelta(days=step*step_length+days_window)).strftime("%Y-%m-%d")
            
            querystring_double = build_flight_request_querystring(countries_airports_df,origin_city=destination_city,destination_city=origin_city, date_departure=date_departure,date_return=date_return, n_adults= n_adults, n_children=n_children, n_infants=n_infants, origin_airport_code=origin_airport_code, 
                                   destination_airport_code=destination_airport_code, cabin_class=cabin_class,sort_by=sort_by,currency=currency)
            
            querystring_list.append([querystring_double])
    
    return querystring_list


# for single way - used at the moment
def build_flight_request_querystring_list_single(countries_airports_df,origin_city,destination_cities_list, date_query_start, n_steps=52, step_length=7, days_window=2, n_adults= 1, n_children=0, n_infants=0, origin_airport_code=None, 
                                   destination_airport_code=None, cabin_class="economy",sort_by="best",currency="EUR"):
    """This function generates a list of querystrings from the input params, that is used to later generate a list of I/O taks to a flights API.
    It generates querystrings for go and return flights both on the same day, iterating from the date_query_start to the end of the days window.
    This allows to map all flights from the selected origin to the possible destinations, for the time window selected (defaulted to 1 year).

    Args:
        countries_airports_df (_type_): _description_
        origin_city (_type_): _description_
        destination_cities_list (_type_): _description_
        date_query_start (_type_): _description_
        date_query_end (_type_): _description_
        n_adults (int, optional): _description_. Defaults to 1.
        n_children (int, optional): _description_. Defaults to 0.
        n_infants (int, optional): _description_. Defaults to 0.
        origin_airport_code (_type_, optional): _description_. Defaults to None.
        destination_airport_code (_type_, optional): _description_. Defaults to None.
        cabin_class (str, optional): _description_. Defaults to "economy".
        sort_by (str, optional): _description_. Defaults to "best".
        currency (str, optional): _description_. Defaults to "EUR".
    """
    date_query_start_datetime = datetime.datetime.strptime(date_query_start, "%Y-%m-%d")

    querystring_list = []
    for destination_city in destination_cities_list:
        for step in range(n_steps):
            date_departure = (date_query_start_datetime + datetime.timedelta(days=step*step_length)).strftime("%Y-%m-%d")
            date_return = (date_query_start_datetime + datetime.timedelta(days=step*step_length+days_window)).strftime("%Y-%m-%d")

            querystring_departure = build_flight_request_querystring(countries_airports_df,origin_city=origin_city,destination_city=destination_city, date_departure=date_departure, n_adults= n_adults, n_children=n_children, n_infants=n_infants, origin_airport_code=origin_airport_code, 
                                   destination_airport_code=destination_airport_code, cabin_class=cabin_class,sort_by=sort_by,currency=currency)
            
            querystring_return = build_flight_request_querystring(countries_airports_df,origin_city=destination_city,destination_city=origin_city, date_departure=date_return, n_adults= n_adults, n_children=n_children, n_infants=n_infants, origin_airport_code=origin_airport_code, 
                                   destination_airport_code=destination_airport_code, cabin_class=cabin_class,sort_by=sort_by,currency=currency)
            
            querystring_list.extend([querystring_departure,querystring_return])
    
    return querystring_list


def build_flight_request_querystring(countries_airports_df,origin_city,destination_city, date_departure, n_adults= 1, n_children=0, n_infants=0, origin_airport_code=None, 
                                   destination_airport_code=None, cabin_class="economy",sort_by="price_high",currency="EUR"):
    
    ## create API query params based on user restrictions

    url = "https://sky-scrapper.p.rapidapi.com/api/v2/flights/searchFlightsComplete"


    # make sure cabin class exits. Not used at the moment as it filters out possibly available flights. Better to get all and filter by SQL query.
    cabin_class_list = ["economy","premium_economy","business","first"]
    cabin_class = cabin_class if cabin_class in cabin_class_list else "economy"

    # choose this to find more airports to choose by
    try:
        origin_city_id = countries_airports_df.loc[countries_airports_df["city"].str.lower() == origin_city.lower(), "city_entityId"]

        origin_city_id = str(int(origin_city_id.iloc[0]))

    except:
        pass

    try:
        destination_city_id =  countries_airports_df.loc[countries_airports_df["city"].str.lower() == destination_city.lower(), "city_entityId"]

        destination_city_id = str(int(destination_city_id.iloc[0]))

    except:
        pass
    
    # careful here as how to select the main airport is now mere coincidence and in the future it will need a method to be selected
    if origin_airport_code != None:
        try:
            origin_airport_id = str(int(countries_airports_df.loc[countries_airports_df["city"].str.lower() == origin_city,"airport_entityId"].unique()))
        except:
            pass
    if destination_airport_code != None:
        try:
            destination_airport_id = str(int(countries_airports_df.loc[countries_airports_df["city"].str.lower() == destination_city,"airport_entityId"].unique()))
        except:
            pass

    sort_by_dict = {
        "best": "best",
        "cheapest": "price_high",
        "fastest": "fastest",
        "outbound_take_off": "outbound_take_off_time",
        "outbound_landing": "outbound_landing_time",
        "return_take_off": "return_take_off_time",
        "return_landing": "return_landing_time"
    }

    sort_by = sort_by_dict.get(sort_by,"price_high")

    if origin_airport_code != None:
        querystring = {"originSkyId":origin_city,"destinationSkyId": destination_city,"originEntityId":origin_airport_id,
                    "destinationEntityId":destination_airport_id,"date": date_departure,
                    "adults":str(n_adults),"childrens":str(n_children),"infants": str(n_infants),"sortBy":sort_by,"currency":currency}
    else:
        querystring = {"originSkyId":origin_city,"destinationSkyId": destination_city,"originEntityId":origin_city_id,
            "destinationEntityId":destination_city_id,"date": date_departure,
            "adults":str(n_adults),"childrens":str(n_children),"infants": str(n_infants),"sortBy":sort_by,"currency":currency}
        
    if date_departure != None:
        querystring["date_return"] = date_departure

    return querystring


def build_flight_request_querystring_list_single(countries_airports_df,origin_city,destination_cities_list, date_query_start, n_steps=52, step_length=7, days_window=2, n_adults= 1, n_children=0, n_infants=0, origin_airport_code=None, 
                                   destination_airport_code=None, cabin_class="economy",sort_by="best",currency="EUR"):
    """This function generates a list of querystrings from the input params, that is used to later generate a list of I/O taks to a flights API.
    It generates querystrings for go and return flights both on the same day, iterating from the date_query_start to the end of the days window.
    This allows to map all flights from the selected origin to the possible destinations, for the time window selected (defaulted to 1 year).

    Args:
        countries_airports_df (_type_): _description_
        origin_city (_type_): _description_
        destination_cities_list (_type_): _description_
        date_query_start (_type_): _description_
        date_query_end (_type_): _description_
        n_adults (int, optional): _description_. Defaults to 1.
        n_children (int, optional): _description_. Defaults to 0.
        n_infants (int, optional): _description_. Defaults to 0.
        origin_airport_code (_type_, optional): _description_. Defaults to None.
        destination_airport_code (_type_, optional): _description_. Defaults to None.
        cabin_class (str, optional): _description_. Defaults to "economy".
        sort_by (str, optional): _description_. Defaults to "best".
        currency (str, optional): _description_. Defaults to "EUR".
    """
    date_query_start_datetime = datetime.datetime.strptime(date_query_start, "%Y-%m-%d")

    querystring_list = []
    for destination_city in destination_cities_list:
        for step in range(n_steps):
            date_departure = (date_query_start_datetime + datetime.timedelta(days=step*step_length)).strftime("%Y-%m-%d")
            date_return = (date_query_start_datetime + datetime.timedelta(days=step*step_length+days_window)).strftime("%Y-%m-%d")

            querystring_departure = build_flight_request_querystring(countries_airports_df,origin_city=origin_city,destination_city=destination_city, date_departure=date_departure, n_adults= n_adults, n_children=n_children, n_infants=n_infants, origin_airport_code=origin_airport_code, 
                                   destination_airport_code=destination_airport_code, cabin_class=cabin_class,sort_by=sort_by,currency=currency)
            
            querystring_return = build_flight_request_querystring(countries_airports_df,origin_city=destination_city,destination_city=origin_city, date_departure=date_return, n_adults= n_adults, n_children=n_children, n_infants=n_infants, origin_airport_code=origin_airport_code, 
                                   destination_airport_code=destination_airport_code, cabin_class=cabin_class,sort_by=sort_by,currency=currency)
            
            querystring_list.extend([querystring_departure,querystring_return])
    
    return querystring_list


def build_flight_request_querystring(countries_airports_df,origin_city,destination_city, date_departure, n_adults= 1, n_children=0, n_infants=0, origin_airport_code=None, 
                                   destination_airport_code=None, cabin_class="economy",sort_by="price_high",currency="EUR"):
    
    ## create API query params based on user restrictions

    url = "https://sky-scrapper.p.rapidapi.com/api/v2/flights/searchFlightsComplete"


    # make sure cabin class exits. Not used at the moment as it filters out possibly available flights. Better to get all and filter by SQL query.
    cabin_class_list = ["economy","premium_economy","business","first"]
    cabin_class = cabin_class if cabin_class in cabin_class_list else "economy"

    # choose this to find more airports to choose by
    try:
        origin_city_id = countries_airports_df.loc[countries_airports_df["city"].str.lower() == origin_city.lower(), "city_entityId"]

        origin_city_id = str(int(origin_city_id.iloc[0]))

    except:
        pass

    try:
        destination_city_id =  countries_airports_df.loc[countries_airports_df["city"].str.lower() == destination_city.lower(), "city_entityId"]

        destination_city_id = str(int(destination_city_id.iloc[0]))

    except:
        pass
    
    # careful here as how to select the main airport is now unique and in the future it will need a method 
    # for the correct airport in the city to be selected
    if origin_airport_code != None:
        try:
            origin_airport_id = str(int(countries_airports_df.loc[countries_airports_df["city"].str.lower() == origin_city,"airport_entityId"].unique()))
        except:
            pass
    if destination_airport_code != None:
        try:
            destination_airport_id = str(int(countries_airports_df.loc[countries_airports_df["city"].str.lower() == destination_city,"airport_entityId"].unique()))
        except:
            pass

    sort_by_dict = {
        "best": "best",
        "cheapest": "price_high",
        "fastest": "fastest",
        "outbound_take_off": "outbound_take_off_time",
        "outbound_landing": "outbound_landing_time",
        "return_take_off": "return_take_off_time",
        "return_landing": "return_landing_time"
    }

    sort_by = sort_by_dict.get(sort_by,"price_high")

    if origin_airport_code != None:
        querystring = {"originSkyId":origin_city,"destinationSkyId": destination_city,"originEntityId":origin_airport_id,
                    "destinationEntityId":destination_airport_id,"date": date_departure,
                    "adults":str(n_adults),"childrens":str(n_children),"infants": str(n_infants),"sortBy":sort_by,"currency":currency}
    else:
        querystring = {"originSkyId":origin_city,"destinationSkyId": destination_city,"originEntityId":origin_city_id,
            "destinationEntityId":destination_city_id,"date": date_departure,
            "adults":str(n_adults),"childrens":str(n_children),"infants": str(n_infants),"sortBy":sort_by,"currency":currency}

    return querystring


async def request_flight_itineraries_async(querystring):
    
    url = "https://sky-scrapper.p.rapidapi.com/api/v2/flights/searchFlightsComplete"

    headers = {
        "x-rapidapi-key": AIR_SCRAPPER_API_KEY,
        "x-rapidapi-host": "sky-scrapper.p.rapidapi.com"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=querystring) as response:
            if response.status == 200:
                try:
                    itineraries = await response.json()
                    itineraries = itineraries["data"]["itineraries"]
                except KeyError as e:
                    print(f"KeyError with querystring {querystring}: {e}")
                    return []  # returning an empty list for consistency
                except TypeError as e:
                    print(f"TypeError with querystring {querystring}: {e}")
                    return []
                except Exception as e:
                    print(f"Unexpected error with querystring {querystring}: {e}")
                    return []
            else:
                print(f"Request failed with status {response.status} for {querystring}")
                raise ValueError(f"HTTP Error: {response.status}")

    return itineraries


async def request_flight_itineraries_async_multiple(querystrings_list):
    request_itineraries_tasks = [request_flight_itineraries_async(querystring) for querystring in querystrings_list]

    itineraries_dict_list = await asyncio.gather(*request_itineraries_tasks)

    return itineraries_dict_list




def create_itineraries_dataframe(itineraries_dict_list):

    extracted_itinerary_info_list = list()

    for itinerary in itineraries_dict_list:
        extracted_itinerary_info_list.append(extract_flight_info(itinerary))
        
    return pd.DataFrame(extracted_itinerary_info_list)



def extract_flight_info(flight_dict):

    flight_result_dict = {}

    flight_result_dict_assigner = {
        'itinerary_id': lambda flight: flight['id'],
        'query_date': lambda _: datetime.datetime.now(),
        'score': lambda flight: float(flight['score']),
        'duration': lambda flight: int(flight['legs'][0]['durationInMinutes']),
        'price': lambda flight: int(flight['price']['formatted'].split()[0].replace(",","")),
        'price_currency': lambda flight: flight['price']['formatted'].split()[1],
        'stops': lambda flight: int(flight['legs'][0]['stopCount']),
        'departure': lambda flight: pd.to_datetime(flight['legs'][0]['departure']),
        'arrival': lambda flight: pd.to_datetime(flight['legs'][0]['arrival']),
        'company': lambda flight: flight['legs'][0]['carriers']['marketing'][0]['name'],
        'self_transfer': lambda flight: flight['isSelfTransfer'],
        'fare_is_change_allowed': lambda flight: flight['farePolicy']['isChangeAllowed'],
        'fare_is_partially_changeable': lambda flight: flight['farePolicy']['isPartiallyChangeable'],
        'fare_is_cancellation_allowed': lambda flight: flight['farePolicy']['isCancellationAllowed'],
        'fare_is_partially_refundable': lambda flight: flight['farePolicy']['isPartiallyRefundable'],
        'score': lambda flight: float(flight['score']),
        'origin_airport': lambda flight: flight['legs'][0]['origin']['name'],
        'destination_airport': lambda flight: flight['legs'][0]['destination']['name'],
        'origin_airport_code': lambda flight: flight['legs'][0]['origin']['displayCode'],
        'destination_airport_code': lambda flight: flight['legs'][0]['destination']['displayCode'],
        'origin_airport_entityid': lambda flight: flight['legs'][0]['origin']['entityId'],
        'destination_airport_entityid': lambda flight: flight['legs'][0]['destination']['entityId']
    }


    for key, function in flight_result_dict_assigner.items():
        try:
            flight_result_dict[key] = function(flight_dict)
        except KeyError:
            flight_result_dict[key] = np.nan  


    return flight_result_dict


### ACCOMMODATIONS - Booking - Scraping
def scrape_accommodations_from_page(page_soup, booking_url, verbose=False):
    accommodation_scraper_dict = {
        "query_date": lambda _: datetime.datetime.now(),
        "city": lambda _: re.findall(r"ss=([a-z]+)&", booking_url)[0],
        "checkin": lambda _: re.findall(r"checkin=(\d{4}-\d{2}-\d{2})", booking_url)[0],
        "checkout": lambda _: re.findall(r"checkout=(\d{4}-\d{2}-\d{2})", booking_url)[0],
        "n_adults_search": lambda _: re.findall(r"group_adults=(\d+)", booking_url)[0],
        "n_children_search":lambda _: re.findall(r"group_children=(\d+)", booking_url)[0],
        "n_rooms_search": lambda _: re.findall(r"no_rooms=(\d+)", booking_url)[0],
        "name": lambda card: card.find("div",{"data-testid":"title"}).text,
        "url": lambda card: card.find("a",{"data-testid":"title-link"})["href"],
        "price_currency": lambda card: card.find("span",{"data-testid":"price-and-discounted-price"}).text.split()[0],
        "total_price_amount": lambda card: card.find("span",{"data-testid":"price-and-discounted-price"}).text.split()[1].replace(".","").replace(",","."),
        "distance_city_center_km": lambda card: card.find("span",{"data-testid":"distance"}).text.split()[1].replace(".","").replace(",","."),
        "score": lambda card: card.find("div",{"data-testid": "review-score"}).find_all("div",recursive=False)[0].find("div").next_sibling.text.strip().replace(",","."),
        "n_comments": lambda card: card.find("div",{"data-testid": "review-score"}).find_all("div",recursive=False)[1].find("div").next_sibling.text.strip().split()[0].replace(".",""),
        "close_to_metro": lambda card: True if card.find("span",{"class":"f419a93f12"}) else False,
        "sustainability_cert": lambda card: True if card.find("span",{"class":"abf093bdfe e6208ee469 f68ecd98ea"}) else False,
        "room_type": lambda card: card.find("h4",{"class":"abf093bdfe e8f7c070a7"}).text,
        "double_bed": lambda card: True if any(["doble" in element.text for element in card.find_all("div",{"class":"abf093bdfe"})]) else False,
        "single_bed": lambda card: True if any(["individual" in element.text for element in card.find_all("div",{"class":"abf093bdfe"})]) else False,
        "free_cancellation": lambda card: True if any([element.text == "Cancelación gratis" for element in card.find_all("div",{"class":"abf093bdfe d068504c75"})]) else False,
        "breakfast_included": lambda card: True if any([element.text == "Cancelación gratis" for element in card.find_all("div",{"class":"abf093bdfe d068504c75"})]) else False,
        "pay_at_hotel": lambda card: True if any(['Sin pago por adelantado' in element.text for element in card.find_all("div",{"class":"abf093bdfe d068504c75"})]) else False,
        "location_score": lambda card: card.find("span",{"class":"a3332d346a"}).text.split()[1].replace(",","."),
        "free_taxi": lambda card: True if any(["taxi gratis" in element.text.lower() for element in card.find_all("div",{"span":"b30f8eb2d6"})]) else False
    }

    accommodation_data_dict = {key: [] for key in accommodation_scraper_dict}

    for accommodation_card in page_soup.findAll("div", {"aria-label":"Alojamiento"}):
            for key, accommodation_scraper_function in accommodation_scraper_dict.items():
                try:
                    accommodation_data_dict[key].append(accommodation_scraper_function(accommodation_card))
                except Exception as e:
                    if verbose == True:
                        print(f"Error filling {key} due to {e}")
                    accommodation_data_dict[key].append(np.nan)

    return accommodation_data_dict


# dynamic html loading functions
def scroll_to_bottom(driver,scroll_period):
    last_height = driver.execute_script("return window.pageYOffset")

    while True:

        driver.execute_script('window.scrollBy(0, 2000)')
        time.sleep(scroll_period)
        
        new_height =  driver.execute_script("return window.pageYOffset")
        if new_height == last_height:
            break
        last_height = new_height

def scroll_back_up(driver):
    driver.execute_script('window.scrollBy(0, -600)')
    time.sleep(0.2)

def click_load_more(driver):
    try:
        button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(("xpath",'//*[@id="bodyconstraint-inner"]/div[2]/div/div[2]/div[3]/div[2]/div[2]/div[3]/div[*]/button')))
        button.click()

        return True
    except:
        return False

def scroll_and_click_cycle(driver,scroll_period):
    while True:
        scroll_to_bottom(driver,scroll_period)
        scroll_back_up(driver)
        if not click_load_more(driver):
            break





def build_booking_urls(destinations_list: List[str], start_date: str, stay_duration: int = 2, step_length: int = 7, n_steps: int = 52, adults: int = 2, children: int = 0,
                           rooms: int = 1, max_price: int = 350, star_ratings: list = None, 
                           meal_plan: str = None, review_score: list = None, max_distance_meters: int = None):
    

    start_date_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    booking_url_list = list()
    for destination in destinations_list:
        for step in range(n_steps):
            checkin = (start_date_datetime + datetime.timedelta(days=step*step_length)).strftime("%Y-%m-%d")
            checkout = (start_date_datetime + datetime.timedelta(days=step*step_length + stay_duration)).strftime("%Y-%m-%d")

            booking_search_link = build_booking_url_full(
                destination=destination,
                checkin=checkin,
                checkout=checkout,
                adults=adults, 
                children=children, 
                rooms=rooms, 
                max_price=max_price, 
                star_ratings=star_ratings, 
                meal_plan=meal_plan,  
                review_score=review_score,  
                max_distance_meters=max_distance_meters 
            )

            booking_url_list.append(booking_search_link)

    return booking_url_list

def build_booking_url_full(destination: str, checkin: str, checkout: str, adults: int = 1, children: int = 0,
                           rooms: int = 1, min_price: int = 1, max_price: int = 1, star_ratings: list = None, 
                           meal_plan: str = None, review_score: list = None, max_distance_meters: int = None):
    """
    Build a Booking.com search URL by including all parameter filters, 
    ensuring proper formatting for all parameters.

    Parameters:
    - destination (str): Destination city.
    - checkin (str): Check-in date in YYYY-MM-DD format.
    - checkout (str): Check-out date in YYYY-MM-DD format.
    - adults (int): Number of adults.
    - children (int): Number of children.
    - rooms (int): Number of rooms.
    - min_price (int): Minimum price in Euros.
    - max_price (int): Maximum price in Euros.
    - star_ratings (list): List of star ratings (e.g., [3, 4, 5]).
    - meal_plan (int): Meal plan (0 for no meal, 1 for breakfast, etc.).
    - review_score (list): List of review scores (e.g., [80, 90] for 8.0+ and 9.0+).
    - max_distance_meters (int): Maximum distance from city center in meters (e.g., 500).

    Returns:
    - str: A Booking.com search URL based on the specified filters.
    """
    
    base_url = "https://www.booking.com/searchresults.es.html?"
    
    # Start with basic search parameters (ensure no tuple formatting)
    url = f"{base_url}ss={destination}&checkin={checkin}&checkout={checkout}&group_adults={adults}&group_children={children}"
    
    if rooms is not None:
       url += f"&no_rooms={rooms}"
    
    if min_price is not None and max_price is not None:
        price_filter = f"price%3DEUR-{min_price}-{max_price}-1"
    elif min_price is not None:
        price_filter = f"price%3DEUR-{min_price}-1-1"
    elif max_price is not None:
        price_filter = f"price%3DEUR-{max_price}-1"
    else:
        price_filter = None

    # Construct 'nflt' parameter to add other filters
    nflt_filters = []
    
    if price_filter:
        nflt_filters.append(price_filter)
    
    if star_ratings:
        star_filter = '%3B'.join([f"class%3D{star}" for star in star_ratings])
        nflt_filters.append(star_filter)
    
    meal_plan_options = {
            "breakfast": 1,
            "breakfast_dinner": 9,
            "kitchen": 999,
            "nothing": None
        }
    meal_plan_formatted = meal_plan_options.get(meal_plan, None)

    if meal_plan_formatted is not None:
        meal_plan_str = f"mealplan%3D{meal_plan_formatted}"
        nflt_filters.append(meal_plan_str)
    
    if review_score:
        review_filter = '%3B'.join([f"review_score%3D{score}" for score in review_score])
        nflt_filters.append(review_filter)
    
    if max_distance_meters is not None:
        distance_str = f"distance%3D{max_distance_meters}"
        nflt_filters.append(distance_str)
    
    # Add all 'nflt' filters to URL
    if nflt_filters:
        url += f"&nflt={'%3B'.join(nflt_filters)}"

    url += "&sr_view=list"
    
    return url

def accommodations_booking_selenium_fetch_all_html_contents_concurrent(booking_url_list,max_threads=5,scroll_period=0.2):
    # Determine optimal max_workers
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(fetch_booking_html_optimized, booking_url,scroll_period) for booking_url in booking_url_list]

        # Collect results as they complete
        html_contents_total = []
        for future in futures:
            html_contents_total.append(future.result())

    return html_contents_total, booking_url_list


def fetch_booking_html(booking_url, scroll_period):

    # open driver
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get(booking_url)

    # scroll and load more until bottom
    # css_selector = "#bodyconstraint-inner > div:nth-child(8) > div > div.af5895d4b2 > div.df7e6ba27d > div.bcbf33c5c3 > div.dcf496a7b9.bb2746aad9 > div.d4924c9e74 > div.c82435a4b8.f581fde0b8 > button"
    scroll_and_click_cycle(driver)

    # fetch booking url html
    html_page = driver.page_source

    return html_page

def fetch_booking_html_optimized(booking_url, scroll_period):

    # ADD OPTIMIZATION OPTIONS HERE
    # add optimization options
    options = Options()
    options.add_argument("--no-sandbox")      # Enables no-sandbox mode
    options.add_argument("--disable-gpu")     # Disables GPU usage
    # options.add_argument("--headless")        # Runs Chrome in headless mode

    # open driver
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.get(booking_url)

    # scroll and load more until bottom
    css_selector = "#bodyconstraint-inner > div:nth-child(8) > div > div.af5895d4b2 > div.df7e6ba27d > div.bcbf33c5c3 > div.dcf496a7b9.bb2746aad9 > div.d4924c9e74 > div.c82435a4b8.f581fde0b8 > button"
    scroll_and_click_cycle(driver, scroll_period)

    # fetch booking url html
    html_page = driver.page_source

    return html_page

def accommodations_booking_soup_from_all_html_contents_parallel(html_contents_total, booking_urls_list, verbose=False):
    start_time = time.time()
    with ThreadPoolExecutor() as executor:

        page_dfs = list(executor.map(accommodations_booking_parse_single_page_wrapper, html_contents_total, booking_urls_list, [verbose] * len(html_contents_total)))

    total_activities_df = pd.concat(page_dfs).reset_index(drop=True)
    end_time = time.time()
    print(f"The whole parallel Beautiful Soup process took {end_time-start_time}")
    return total_activities_df

def accommodations_booking_parse_single_page_wrapper(page_html, booking_url, verbose=False):
    return accommodations_booking_parse_single_page(page_html, booking_url,verbose=verbose)


def accommodations_booking_parse_single_page(page_html,booking_url, verbose=False):
    page_soup = BeautifulSoup(page_html, "html.parser")
    return pd.DataFrame(scrape_accommodations_from_page(page_soup,booking_url, verbose=verbose))


def get_accommodations_booking(destinations_list: List[str], start_date: str, stay_duration: int = 2, step_length: int = 7, n_steps: int = 52, adults: int = 2, children: int = 0,
                           rooms: int = 1, max_price: int = 350, star_ratings: list = None, 
                           meal_plan: str = None, review_score: list = None, max_distance_meters: int = 5000, max_threads = 5, scroll_period= 0.2,verbose=False):
    
    start_time = time.time()

    booking_urls_list = build_booking_urls(destinations_list = destinations_list, start_date= start_date, stay_duration =stay_duration , step_length = step_length, n_steps = n_steps, adults = adults, children = children,
                           rooms = rooms, max_price = max_price, star_ratings = star_ratings, meal_plan = meal_plan, review_score = review_score, max_distance_meters = max_distance_meters)
    
    print(f"It took {time.time() - start_time} seconds to build the urls")
    booking_html_contents_total, booking_urls_list = accommodations_booking_selenium_fetch_all_html_contents_concurrent(booking_urls_list, max_threads=max_threads, scroll_period=scroll_period)
    print(f"It took {time.time() - start_time} seconds for selenium to get the html contents")

    print("Now parsing with beautiful soup")
    total_accommodations_df = accommodations_booking_soup_from_all_html_contents_parallel(booking_html_contents_total, booking_urls_list,verbose=verbose)
    return total_accommodations_df
        
        

### Activities - civitatis

def get_pagination_htmls_by_city_date(city_name, date_start, date_end, page_start, n_pages, driver):

    html_contents = []
    activities_link_list = []

    for page_number in range(page_start, n_pages + page_start):

        activities_link = f"https://www.civitatis.com/es/{city_name}/?page={page_number}&fromDate={date_start}&toDate={date_end}"

        driver.get(activities_link)
        driver.maximize_window()
        
        # make sure availability cards and amount of available activities show in the page
        # only if main page with activities
        try:
            driver.find_element("ces selector","#activity-filters--applied > div > div.o-search-toolbar__title.o-search-toolbar__title.is-no-results > div > div > div > div")
            continue
        except:
            if driver.current_url != "https://www.civitatis.com/es/":
                try:
                    WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(("css selector","div.m-availability"))
                    )
                    
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(("css selector","#activitiesShowing"))
                    )
                    driver.execute_script('window.scrollBy(0, 4000)')

                except:
                    pass
                

                html_content = driver.page_source

                html_contents.append(html_content)
                activities_link_list.append(activities_link)

    return html_contents, activities_link_list






def activities_civitatis_extract_all_from_city(city_name, date_start, date_end, verbose=False):
    period = 6
    # civitatis can check widows of 15 days max, calculate number of iterations
    date_start_datetime = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    date_end_datetime = datetime.datetime.strptime(date_end, "%Y-%m-%d")

    n_iter = int((date_end_datetime - date_start_datetime).days / period)

    # define accumulator df
    total_actitivities_df = pd.DataFrame()

    for iter in range(1,n_iter+1):
        # define url 
        date_start_iter = (date_start_datetime + datetime.timedelta(days=period*(iter-1))).strftime("%Y-%m-%d")
        date_end_iter = ((date_start_datetime + datetime.timedelta(days=period*iter))).strftime("%Y-%m-%d")
        first_link = f"https://www.civitatis.com/es/{city_name}/?fromDate={date_start_iter}&toDate={date_end_iter}"

        # open driver
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(first_link)

        # make sure availability cards and amount of available activities show in the page
        try:
            driver.execute_script('window.scrollBy(0, 400)')
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(("css selector","div.m-availability"))

            )
            
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(("css selector","#activitiesShowing"))
            )
        except:
            pass

        # parse first page to get last page number
        html_content1 = driver.page_source

        soup = BeautifulSoup(html_content1, "html.parser")

        last_page = math.ceil(int(soup.find("div",{"class","columns o-pagination__showing"}).find("div",{"class":"left"}).text.split()[0])/20)
        last_page

        page_start = 2
        n_pages = last_page - 1

        # collect unparsed pages html
        html_contents = get_pagination_htmls_by_city_date(city_name, date_start_iter, date_end_iter, page_start, n_pages, driver)
        html_contents = list(html_content1).extend(html_contents)

        # parse each page and scrape elements

        for page_html in html_contents:

            page_soup = BeautifulSoup(page_html, "html.parser")
            page_activities_df = pd.DataFrame(scrape_activities_from_page(page_soup, verbose=verbose))
            total_actitivities_df = pd.concat([total_actitivities_df,page_activities_df]).reset_index(drop=True)
            

    return total_actitivities_df




def activities_civitatis_selenium_get_all_html_contents(cities_list, date_start, date_end):
    period = 6
    # civitatis can check widows of 15 days max, calculate number of iterations
    date_start_datetime = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    date_end_datetime = datetime.datetime.strptime(date_end, "%Y-%m-%d")

    n_iter = int((date_end_datetime - date_start_datetime).days / period)

    # define accumulator html_list
    html_contents_total = []
    pages_urls_total = []
    
    # open driver
    driver = webdriver.Chrome()
    driver.maximize_window()
    for city_name in cities_list:
        for iter in range(1,n_iter+1):

            # define url 
            date_start_iter = (date_start_datetime + datetime.timedelta(days=period*(iter-1))).strftime("%Y-%m-%d")
            date_end_iter = ((date_start_datetime + datetime.timedelta(days=period*iter))).strftime("%Y-%m-%d")

            first_link = f"https://www.civitatis.com/es/{city_name}/?fromDate={date_start_iter}&toDate={date_end_iter}"

            # navigate
            driver.get(first_link)

            # make sure availability cards and amount of available activities show in the page
            try:
                driver.find_element("ces selector","#activity-filters--applied > div > div.o-search-toolbar__title.o-search-toolbar__title.is-no-results > div > div > div > div")
                continue
            except:
                try:
                    driver.execute_script('window.scrollBy(0, 400)')
                    WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(("css selector","div.m-availability"))
                    )
                    
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(("css selector","#activitiesShowing"))
                    )
                except:
                    pass

                    # parse first page to get last page number
                html_content1 = driver.page_source

                soup = BeautifulSoup(html_content1, "html.parser")

                last_page = math.ceil(int(soup.find("div",{"class","columns o-pagination__showing"}).find("div",{"class":"left"}).text.split()[0])/20)
                last_page

                page_start = 2
                n_pages = last_page - 1

                # collect unparsed pages html
                html_contents, pages_urls = get_pagination_htmls_by_city_date(city_name, date_start_iter, date_end_iter, page_start, n_pages, driver)
                html_contents.append(html_content1)
                html_contents_total.extend(html_contents)

                pages_urls_total.append(first_link)
                pages_urls_total.extend(pages_urls)

    return html_contents_total, pages_urls_total



def activities_civitatis_extract_all_activites(cities_list, date_start, date_end, verbose):
    html_contents_total = activities_civitatis_selenium_get_all_html_contents(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents(html_contents_total,verbose=verbose)

    return total_activities_df

def activities_civitatis_soup_from_all_html_contents(html_contents_total,verbose=False):

    total_actitivities_df = pd.DataFrame()
    for page_num, page_html in enumerate(html_contents_total):
        page_soup = BeautifulSoup(page_html, "html.parser")
        page_activities_df = pd.DataFrame(scrape_activities_from_page(page_soup, verbose=verbose))
        total_actitivities_df = pd.concat([total_actitivities_df,page_activities_df]).reset_index(drop=True)
            
    return total_actitivities_df


def activities_civitatis_extract_all_activites_multithread(cities_list, date_start, date_end, verbose):
    html_contents_total, pages_urls = activities_civitatis_selenium_get_all_html_contents(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents_multithread(html_contents_total, pages_urls, verbose=verbose)

    return total_activities_df

def activities_civitatis_soup_from_all_html_contents_multithread(html_contents_total, verbose=False):
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        page_dfs = list(executor.map(lambda page_html: parse_single_page(page_html, verbose), html_contents_total))

    total_activities_df = pd.concat(page_dfs).reset_index(drop=True)
    end_time = time.time()
    print(f"The whole multithread Beautiful Soup process took {end_time-start_time}")
    return total_activities_df


def activities_civitatis_extract_all_activites_parallel(cities_list, date_start, date_end, verbose):
    html_contents_total, pages_urls = activities_civitatis_selenium_get_all_html_contents(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents_parallel(html_contents_total, pages_urls,verbose=verbose)

    return total_activities_df


def activities_civitatis_soup_from_all_html_contents_parallel(html_contents_total, pages_urls, verbose=False):
    start_time = time.time()
    with ProcessPoolExecutor() as executor:

        page_dfs = list(executor.map(parse_single_page_wrapper, html_contents_total, pages_urls, [verbose] * len(html_contents_total)))

    total_activities_df = pd.concat(page_dfs).reset_index(drop=True)
    end_time = time.time()
    print(f"The whole parallel Beautiful Soup process took {end_time-start_time}")
    return total_activities_df


def parse_single_page_wrapper(page_html, page_url, verbose=False):
    return parse_single_page(page_html, page_url, verbose=verbose)


def parse_single_page(page_html, page_url, verbose=False):
    page_soup = BeautifulSoup(page_html, "html.parser")
    return pd.DataFrame(scrape_activities_from_page(page_soup, page_url, verbose=verbose))


def scrape_activities_from_page(page_soup, page_url, verbose=False):
    activity_data_dict = {
            "query_date": [],
            "city": [],
            "activity_date_range_start": [],
            "activity_date_range_end": [],
            "activity_name": [],
            "description": [],
            "url": [],
            "image": [],
            "image2": [],
            "available_days": [],
            "available_times": [],
            "duration": [],
            "latitude": [],
            "longitude": [],
            # "address": [],
            "price": [],
            "currency": [],
            "category": [],
            "spanish": []
    }

    for element in page_soup.findAll("div",{"class","o-search-list__item"}):

        availability_cards = list(set(element.findAll("div", {"class": "m-availability__item"})) - 
                                set(element.findAll("div", {"class": "m-availability__item _no-dates"})))
        
        activity_scraper_dict = {
            "query_date": lambda _ : datetime.datetime.now(),

            "city": lambda _: re.findall(r".com/es/(\w+)/", page_url)[0],

            "activity_date_range_start": lambda _: re.findall(r"fromDate=(\d{4}-\d{2}-\d{2})", page_url)[0],

            "activity_date_range_end": lambda _: re.findall(r"toDate=(\d{4}-\d{2}-\d{2})", page_url)[0],

            "activity_name": lambda element: element.find("a", {"class": "ga-trackEvent-element _activity-link"})["title"],

            "description": lambda element: element.find("div", {"class": "comfort-card__text l-list-card__text"}).text.strip().replace("\xa0", " "),

            "url": lambda element: "www.civitatis.com" + element.find("a",{"data-eventcategory":"Actividades Listado"})["href"],

            # image/gif
            "image": lambda element: "www.civitatis.com" + element.find("img")["src"],

            # image/gif
            "image2": lambda element: "www.civitatis.com" + element.find("img")["data-src"],

            # NOTE_: I'LL HAVE TO HANDLE LAST AND FIRST DAYS OF MONTH CAREFULLY, AS MONTH NOT SPECIFIED
            "available_days": lambda _: [el.find('br').next_sibling.strip() for el in availability_cards],
            
            "available_times": lambda _: [[time.text for time in el.find_all("span", {"class": "_time"})] for el in availability_cards],
            
            "duration": lambda element: element.find("span",{"class":"comfort-card__feature _duration has-tip top _processed"}).text.strip(),
            
            # address: use latitude and longitude, then convert with geopy
            "latitude": lambda element: element.find("article", recursive=False)["data-latitude"],
            "longitude": lambda element: element.find("article", recursive=False)["data-longitude"],

            # "address": lambda element: geolocator.reverse(
            #     (element.find("article", recursive=False)["data-latitude"], 
            #     element.find("article", recursive=False)["data-longitude"])
            # ).address,
    
            "price": lambda element: json.loads(element.find("a", {"class": "ga-trackEvent-element _activity-link"})["data-gtm-new-model-click"])["ecommerce"]["click"]["products"][0]["price"],
            
            "currency": lambda element: json.loads(element.find("a", {"class": "ga-trackEvent-element _activity-link"})["data-gtm-new-model-click"])["ecommerce"]["currencyCode"],
            
            "category": lambda element: element.find("span",{"data-tooltip-class":"tooltip activity-tooltip city-list__feature-tooltip"}).text.strip(),

            "spanish": lambda element: element.find("span",{"class":"comfort-card__feature _lang has-tip top _processed"}).text.strip()
        }
        

        for key, activity_scraper_function in activity_scraper_dict.items():
            try:
                
                if not activity_scraper_function(element): # if empty list or other
                    activity_data_dict[key].append(np.nan)
                else: 
                    activity_data_dict[key].append(activity_scraper_function(element))

            except Exception as e:
                if verbose == True:
                    print(f"Error filling {key} due to {e}")
                activity_data_dict[key].append(np.nan)

    return activity_data_dict



# Top - bottom function definition
### Soup parallel/multithread + selenium concurrent

def activities_civitatis_extract_all_activites_parallel_selenium(cities_list, date_start, date_end, verbose):
    html_contents_total, pages_urls = activities_civitatis_selenium_get_all_html_contents_concurrent(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents_parallel(html_contents_total,pages_urls,verbose=verbose)

    return total_activities_df

def activities_civitatis_extract_all_activites_multithread_selenium(cities_list, date_start, date_end, verbose):
    html_contents_total = activities_civitatis_selenium_get_all_html_contents_concurrent(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents_multithread(html_contents_total,verbose=verbose)

    return total_activities_df


def activities_civitatis_selenium_get_all_html_contents_concurrent(cities_list, date_start, date_end):
    # Determine optimal max_workers, usually best around the number of CPUs for Selenium
    max_workers = min(len(cities_list), os.cpu_count() or 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_city_htmls, city, date_start, date_end) for city in cities_list]
        
        # Collect results as they complete
        html_contents_total = []
        pages_urls_total = []
        for future in futures:
            html_contents_total.extend(future.result()[0])
            pages_urls_total.extend(future.result()[1])
    
    return html_contents_total, pages_urls_total


### Concurrent selenium
def fetch_city_htmls(city_name, date_start, date_end):
    period = 6
    date_start_datetime = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    date_end_datetime = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    n_iter = int((date_end_datetime - date_start_datetime).days / period)
    
    html_contents_total = []
    pages_urls_total = []
    
    # Open a new WebDriver instance for each thread
    driver = webdriver.Chrome()
    driver.maximize_window()
    
    for iter in range(1, n_iter + 1):
        # Calculate iteration end date
        date_start_iter = (date_start_datetime + datetime.timedelta(days=period*(iter-1))).strftime("%Y-%m-%d")
        date_end_iter = ((date_start_datetime + datetime.timedelta(days=period*iter))).strftime("%Y-%m-%d")
        first_link = f"https://www.civitatis.com/es/{city_name}/?fromDate={date_start_iter}&toDate={date_end_iter}"

        # Navigate
        driver.get(first_link)

        # Wait for elements to load
        try:
            driver.execute_script('window.scrollBy(0, 400)')
            WebDriverWait(driver, 5).until(EC.presence_of_element_located(("css selector", "div.m-availability")))
            WebDriverWait(driver, 5).until(EC.presence_of_element_located(("css selector", "#activitiesShowing")))
        except:
            pass

        # Parse page to get last page number
        html_content1 = driver.page_source
        soup = BeautifulSoup(html_content1, "html.parser")
        last_page = math.ceil(int(soup.find("div", {"class", "columns o-pagination__showing"}).find("div", {"class": "left"}).text.split()[0]) / 20)
        
        # Get all pagination pages
        html_contents, page_urls = get_pagination_htmls_by_city_date(city_name, date_start_iter, date_end_iter, 2, last_page - 1, driver)
        html_contents.append(html_content1)
        html_contents_total.extend(html_contents)
        pages_urls_total.append(first_link)
        pages_urls_total.extend(page_urls)
    
    driver.quit()
    return html_contents_total, pages_urls_total

### Soup parallel + selenium concurrent optimized

def activities_civitatis_extract_all_activites_parallel_selenium_optimized(cities_list, date_start, date_end, verbose):
    html_contents_total, pages_urls = activities_civitatis_selenium_get_all_html_contents_concurrent_optimized(cities_list, date_start, date_end)

    print("Now parsing with beautiful soup")
    total_activities_df = activities_civitatis_soup_from_all_html_contents_parallel(html_contents_total,pages_urls,verbose=verbose)

    return total_activities_df

def activities_civitatis_selenium_get_all_html_contents_concurrent_optimized(cities_list, date_start, date_end):
    # Determine optimal max_workers, usually best around the number of CPUs for Selenium
    max_workers = min(len(cities_list), os.cpu_count() or 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_city_htmls_optimized, city, date_start, date_end) for city in cities_list]
        
        # Collect results as they complete
        html_contents_total = []
        pages_urls_total = []
        for future in futures:
            html_contents_total.extend(future.result()[0])
            pages_urls_total.extend(future.result()[1])
    
    return html_contents_total, pages_urls_total


# Concurrent selenium optimized
def fetch_city_htmls_optimized(city_name, date_start, date_end):
    period = 6
    date_start_datetime = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    date_end_datetime = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    n_iter = int((date_end_datetime - date_start_datetime).days / period)
    
    html_contents_total = []
    pages_urls_total = []

    # add optimization options
    options = Options()
    options.add_argument("--no-sandbox")      # Enables no-sandbox mode
    options.add_argument("--disable-gpu")     # Disables GPU usage
    # options.add_argument("--headless")        # Runs Chrome in headless mode
    # Open a new WebDriver instance for each thread
    driver = webdriver.Chrome(options=options)
    
    
    driver.maximize_window()
    
    for iter in range(1, n_iter + 1):
        # Calculate iteration end date
        date_start_iter = (date_start_datetime + datetime.timedelta(days=period*(iter-1))).strftime("%Y-%m-%d")
        date_end_iter = ((date_start_datetime + datetime.timedelta(days=period*iter))).strftime("%Y-%m-%d")
        first_link = f"https://www.civitatis.com/es/{city_name}/?fromDate={date_start_iter}&toDate={date_end_iter}"

        # Navigate
        driver.get(first_link)

        # Wait for elements to load
        try:
            driver.execute_script('window.scrollBy(0, 400)')
            WebDriverWait(driver, 5).until(EC.presence_of_element_located(("css selector", "div.m-availability")))
            WebDriverWait(driver, 5).until(EC.presence_of_element_located(("css selector", "#activitiesShowing")))
        except:
            pass

        # Parse page to get last page number
        html_content1 = driver.page_source
        soup = BeautifulSoup(html_content1, "html.parser")
        last_page = math.ceil(int(soup.find("div", {"class", "columns o-pagination__showing"}).find("div", {"class": "left"}).text.split()[0]) / 20)
        
        # Get all pagination pages
        html_contents, pages_urls = get_pagination_htmls_by_city_date(city_name, date_start_iter, date_end_iter, 2, last_page - 1, driver)
        html_contents.append(html_content1)
        html_contents_total.extend(html_contents)

        pages_urls_total.append(first_link)
        pages_urls_total.extend(pages_urls)
    
    driver.quit()
    return html_contents_total, pages_urls_total


### Weather - forecast
async def fetch_forecast(city, latitude, longitude,params):
    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL_FORECAST, params={**params, "latitude": latitude, "longitude": longitude}) as response:
            data = await response.json()
            return {city: data.get("daily", {})}

async def get_forecast(cities,params):
    tasks = [fetch_forecast(city, lat, lon,params) for city, (lat, lon) in cities.items()]
    results = await asyncio.gather(*tasks)
    
    forecast_data = {city: result[city] for result in results for city in result}
    all_forecasts = []
    for city, daily_data in forecast_data.items():
        city_df = pd.DataFrame(daily_data)
        city_df["city"] = city
        all_forecasts.append(city_df)
    
    forecast_df = pd.concat(all_forecasts, ignore_index=True)
    return forecast_df

### Weather - history
async def fetch_weather_data_city(url, city, latitude, longitude, params):
    params.update({
        "latitude": latitude,
        "longitude": longitude
    })
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if "daily" in data:
                    df = pd.DataFrame(data["daily"])
                    df["city"] = city  # Add city name to the DataFrame
                    return df
                else:
                    print(f"No daily data for {city}")
                    return pd.DataFrame()  
            else:
                print(f"Error {response.status} for {city}")
                return pd.DataFrame()  


async def get_weather_history_for_cities(cities_dict,params):
    tasks = [fetch_weather_data_city(BASE_URL_ARCHIVE, city, lat, lon,params) for city, (lat, lon) in cities_dict.items()]
    results = await asyncio.gather(*tasks)
    
    all_cities_df = pd.concat(results, ignore_index=True)
    return all_cities_df
