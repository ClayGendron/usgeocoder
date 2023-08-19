import pandas as pd
import requests
from datetime import date
from time import sleep
from concurrent.futures import ThreadPoolExecutor

BENCHMARK = 'Public_AR_Current'
VINTAGE = 'Current_Current'

sleep_delay = 0.1
timeouts = [0.5, 1, 2, 5]

def request_address_geocode(address, benchmark=BENCHMARK, batch=False):
    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
    geocode_params = {
        'benchmark': benchmark, 
        'format': 'json', 
        'address': address
    }
    
    today = date.today().strftime('%Y-%m-%d')
    
    def successful_response(address, coordinates):
        longitude = coordinates['x']
        latitude = coordinates['y']
        response = {
            'Address': address,
            'Date': today,
            'Longitude': longitude,
            'Latitude': latitude
        }
        
        return response
    
    def failed_response(address):
        response = {
            'Address': address,
            'Date': today,
            'Longitude': None,
            'Latitude': None
        }
        
        return response
    
    for t in timeouts:
        # try request for address geocode
        try:
            geocode_req = requests.get(base_geocode_url, params=geocode_params, timeout=t)
            geocode_data = geocode_req.json()

            # if the request was successful but didn't match an address
            if 'result' in geocode_data and not geocode_data['result']['addressMatches']:
                sleep(sleep_delay)
                if batch:
                    return failed_response(address)
                else:
                    print(f'Address {address} did not match any records.')
                    return None

            # if the request was successful and matched an address return first match
            elif 'result' in geocode_data and geocode_data['result']['addressMatches']:
                coordinates = geocode_data['result']['addressMatches'][0]['coordinates']
                sleep(sleep_delay)
                return successful_response(address, coordinates)

        # if request failed, manage the error or exception
        except ValueError:
            sleep(sleep_delay)
            if batch:
                return failed_response(address)
            else:
                print('Decoding JSON has failed for address: ' + address)
                return None

        except requests.exceptions.Timeout:
            if t == timeouts[-1]:
                sleep(sleep_delay)
                if batch:
                    return failed_response(address)
                else:
                    print(f'All attempts failed for address: {address}')
                    return None

            sleep(sleep_delay)
            continue

        except requests.exceptions.RequestException as e:
            # Catch any unforeseen requests-related exceptions
            sleep(sleep_delay)
            if batch:
                return failed_response(address)
            else:
                print(f'Request exception occurred for address {address}: {e}')
                return None

def request_coordinates_geocode(longitude_latitude, benchmark=BENCHMARK, vintage=VINTAGE, batch=False):
    longitude = longitude_latitude[0]
    latitude = longitude_latitude[1]
    
    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates'
    geocode_params = {
        'benchmark': benchmark,
        'vintage': vintage,
        'format': 'json',
        'x': longitude,
        'y': latitude
    }
    
    today = date.today().strftime('%Y-%m-%d')
    
    def successful_response(longitude, latitude, geographies):
        response = {
            'Coordinates': (longitude, latitude),
            'Date': today,
            'State': geographies['States'][0]['BASENAME'],
            'County': geographies['Counties'][0]['BASENAME'],
            'Census Block': geographies['2020 Census Blocks'][0]['BASENAME'],
            'Census Tract': geographies['Census Tracts'][0]['BASENAME']
        }
        
        return response
    
    def failed_response(longitude, latitude):
        response = {
            'Coordinates': (longitude, latitude),
            'Date': today,
            'State': None,
            'County': None,
            'Census Block': None,
            'Census Tract': None
        }
        
        return response
    
    for t in timeouts:
        try:
            geocode_req = requests.get(base_geocode_url, params=geocode_params, timeout=t)
            geocode_data = geocode_req.json()
            
            # if the request was successful but didn't match an address
            if 'result' in geocode_data and len(geocode_data['result']['geographies']) == 0:                
                sleep(sleep_delay)
                if batch:
                    print(failed_response(longitude, latitude))
                    return failed_response(longitude, latitude)
                else:
                    print(f'Coordinates ({longitude}, {latitude}) did not match any records.')
                    return None
            
            # if the request was successful and contains the 'result' key
            elif 'result' in geocode_data:
                geographies = geocode_data['result']['geographies']
                return successful_response(longitude, latitude, geographies)
            
        except ValueError:
            sleep(sleep_delay)
            if batch:
                print(failed_response(longitude, latitude))
                return failed_response(longitude, latitude)
            else:
                print(f'Decoding JSON has failed for coordinates: ({longitude}, {latitude})')
                return None
        
        except requests.exceptions.Timeout:
            if t == timeouts[-1]:
                sleep(sleep_delay)
                if batch:
                    print(failed_response(longitude, latitude))
                    return failed_response(longitude, latitude)
                else:
                    print(f'All attempts failed for coordinates: ({longitude}, {latitude})')
                    sleep(sleep_delay)
                return None
            
            sleep(sleep_delay)
            continue
        
        except requests.exceptions.RequestException as e:
            # Catch any unforeseen requests-related exceptions
            sleep(sleep_delay)
            if batch:
                print(failed_response(longitude, latitude))
                return failed_response(longitude, latitude)
            else:
                print(f'Request exception occurred for coordinates ({longitude}, {latitude}): {e}')
                return None

def batch_geocoder(data, direction='forward', n_threads=1):
    # raise error if invalid direction
    if direction not in ['forward', 'reverse']:
        raise ValueError('direction must be either "forward" or "reverse"')
    
    # show warning if n_threads is set very high and ask user if they want to set n_threads to 100
    if n_threads > 100:
        print('WARNING: n_threads is set very high and you may experience rate limits.')
        print('Would you like to set n_threads to the recommended max of 100? (y/n)')
        response = input()
        if response == 'y':
            n_threads = 100
        else:
            print('If this process fails, try reducing n_threads to 100 or less.')

    data = set(data)
    
    forward_cols = ['Address', 'Date', 'Longitude', 'Latitude']
    reverse_cols = ['Coordinates', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract']
    
    if direction == 'forward':
        located_df = pd.DataFrame(columns=forward_cols)
        failed_df = pd.DataFrame(columns=forward_cols)
        request = request_address_geocode
        
    elif direction == 'reverse':
        located_df = pd.DataFrame(columns=reverse_cols)
        failed_df = pd.DataFrame(columns=reverse_cols)
        request = request_coordinates_geocode
    
    # build wrapper to set request function to batch mode
    def batch_request(data):
        return request(data, batch=True)
    
    # set up ThreadPoolExecutor to run request functions in parallel
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for result in executor.map(batch_request, data):
            if result[next(reversed(result.keys()))] is not None: # if the request was successful
                located_df = pd.concat([located_df, pd.DataFrame([result])], ignore_index=True)
            else:
                failed_df = pd.concat([failed_df, pd.DataFrame([result])], ignore_index=True)
    
    return located_df, failed_df