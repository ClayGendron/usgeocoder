import pandas as pd
import requests
from datetime import date
from time import sleep
from concurrent.futures import ThreadPoolExecutor

def request_address_geocode(address, benchmark='Public_AR_Current'):
    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
    geocode_params = {
        'benchmark': benchmark, 
        'format': 'json', 
        'address': address
    }
    
    today = date.today().strftime('%Y-%m-%d')

    sleep_delay = 0.1
    timeouts = [0.5, 1, 2, 5]
    for t in timeouts:
        # try request for address geocode
        try:
            geocode_req = requests.get(base_geocode_url, params=geocode_params, timeout=t)
            geocode_data = geocode_req.json()

            # if the request was successful but didn't match an address
            if 'result' in geocode_data and not geocode_data['result']['addressMatches']:
                print(f'Address {address} did not match any records.')

                sleep(sleep_delay)
                return None

            # if the request was successful and matched an address return first match
            elif 'result' in geocode_data and geocode_data['result']['addressMatches']:
                coordinates = geocode_data['result']['addressMatches'][0]['coordinates']
                census_latitude = coordinates['y']
                census_longitude = coordinates['x']

                sleep(sleep_delay)
                return {'Address': address, 'Date': today, 'Latitude': census_latitude, 'Longitude': census_longitude}

        # if request failed, manage the error or exception
        except ValueError:
            print('Decoding JSON has failed for address: ' + address)

            sleep(sleep_delay)
            return None

        except requests.exceptions.Timeout:
            if t == timeouts[-1]:
                print(f'All attempts failed for address: {address}')

                sleep(sleep_delay)
                return None

            sleep(sleep_delay)
            continue

        except requests.exceptions.RequestException as e:
            # Catch any unforeseen requests-related exceptions
            print(f'Request exception occurred for address {address}: {e}')

            sleep(sleep_delay)
            return None

def request_coordinates_geocode(latitude_longitude, benchmark='Public_AR_Current', vintage='Current_Current'):
    latitude = latitude_longitude[0]
    longitude = latitude_longitude[1]
    
    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates'
    geocode_params = {
        'benchmark': benchmark,
        'format': 'json',
        'vintage': vintage,
        'y': latitude,
        'x': longitude
    }
    today = date.today().strftime('%Y-%m-%d')
    
    sleep_delay = 0.1
    timeouts = [0.5, 1, 2, 5]
    for t in timeouts:
        try:
            geocode_req = requests.get(base_geocode_url, params=geocode_params, timeout=t)
            geocode_data = geocode_req.json()
            
            # if the request was successful and contains the 'result' key
            if 'result' in geocode_data:
                geographies = geocode_data['result']['geographies']

                return {
                    'Latitude': latitude,
                    'Longitude': longitude,
                    'Date': today,
                    'State': geographies['States'][0]['BASENAME'],
                    'County': geographies['Counties'][0]['BASENAME'],
                    'Urban Area': geographies['Urban Areas'][0]['BASENAME'],
                    'Census Block': geographies['2020 Census Blocks'][0]['BASENAME'],
                    'Census Tract': geographies['Census Tracts'][0]['BASENAME']
                }
            
        except ValueError:
            print(f'Decoding JSON has failed for coordinates: ({latitude}, {longitude})')
            sleep(sleep_delay)
            return None
        
        except requests.exceptions.Timeout:
            if t == timeouts[-1]:
                print(f'All attempts failed for coordinates: ({latitude}, {longitude})')
                sleep(sleep_delay)
                return None
            
            sleep(sleep_delay)
            continue
        
        except requests.exceptions.RequestException as e:
            # Catch any unforeseen requests-related exceptions
            print(f'Request exception occurred for coordinates ({latitude}, {longitude}): {e}')
            sleep(sleep_delay)
            return None

def batch_geocoder(data, direction='forward', request=None, n_threads=1):
    # if direction doesn't equal 'forward' or 'reverse', raise error
    if direction not in ['forward', 'reverse']:
        raise ValueError('direction must be either "forward" or "reverse"')
    
    # show warning if n_threads is set very high and ask user if they want to set n_threads to 100
    if n_threads > 100:
        print('WARNING: n_threads is set very high and you may experience rate limits.')
        print('Would you like to set n_threads to the recomended max of 100? (y/n)')
        response = input()
        if response == 'y':
            n_threads = 100
        else:
            print('If this process fails, try reducing n_threads to 100 or less.')

    data = set(data)
    
    if direction == 'forward':
        located_df = pd.DataFrame(columns=['Address', 'Date', 'Latitude', 'Longitude'])
        failed_df = pd.DataFrame(columns=['Address', 'Date'])
        if request is None:
            request = request_address_geocode
        
    elif direction == 'reverse':
        located_df = pd.DataFrame(columns=['Latitude', 'Longitude', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract'])
        failed_df = pd.DataFrame(columns=['Latitude', 'Longitude', 'Date'])
        if request is None:
            request = request_coordinates_geocode

    # set up ThreadPoolExecutor to run request_address_geocode
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for result in executor.map(request, data):
            if result is not None:
                located_df = pd.concat([located_df, pd.DataFrame([result])], ignore_index=True)
            else:
                failed_df = pd.concat([failed_df, pd.DataFrame([result])], ignore_index=True)

    return located_df, failed_df
