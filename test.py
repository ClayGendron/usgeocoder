import pandas as pd
import numpy as np
import requests
from pyprojroot import here
from concurrent.futures import ThreadPoolExecutor

import logging
import http.client

http.client.HTTPConnection.debuglevel = 1

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True


def get_geocode(address):
    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
    geocode_params = {'benchmark': 'Public_AR_Current', 'format': 'json', 'address': address}
    geocode_req = requests.get(base_geocode_url, params = geocode_params)

    try:
        geocode_data = geocode_req.json()
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        print('Decoding JSON has failed for address: ' + address)
        return None

    # Check if the 'result' key is in the geocode_data dictionary
    if 'result' in geocode_data and geocode_data['result']['addressMatches']:
        # Get coordinates from Geocoding API response
        coordinates = geocode_data['result']['addressMatches'][0]['coordinates']

        # Request to FCC Census Block API
        census_params = {'latitude': coordinates['y'], 'longitude': coordinates['x'], 'format': 'json'}

        # Get Census Tract from FCC API response
        census_latitude = census_params['latitude']
        census_longitude = census_params['longitude']
        print({'Address': address, 'Latitude': census_latitude, 'Longitude': census_longitude})
        return {'Address': address, 'Latitude': census_latitude, 'Longitude': census_longitude}
    else:
        return None
    
address = '156 Front St, Exeter, NH 03833'
get_geocode(address)