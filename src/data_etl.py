# data processing
import pandas as pd
import numpy as np

# browser automation
import selenium

# working with time
import time
from datetime import date
import datetime

# working with asynchronous functions
import asyncio


# data extraction support functions
import data_extraction_support as des

list_of_countries_or_cities = ["spain","bilbao"]

async def create_airports_table():

    countries_airports = des.create_country_airport_code_df(list_of_countries_or_cities)

    countries_airports[["city","latitude","longitude"]] = await des.get_cities_coordinates(countries_airports["city"].to_list())

    countries_airports.to_csv("../data/airport_codes/countries_airports.csv")
