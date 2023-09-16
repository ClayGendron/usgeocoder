import pandas as pd
import requests
from datetime import date
from time import sleep
from concurrent.futures import ThreadPoolExecutor

BENCHMARK = 'Public_AR_Current'
VINTAGE = 'Current_Current'

sleep_delay = 0.1
timeouts = [0.5, 1, 2, 5]


def geocode_address(address, benchmark=BENCHMARK, batch=False):
    """
    Request geocoding information for a given address using the U.S. Census Geocoder.

    Parameters
    ----------
    address : str
        The address string to geocode.
    benchmark : str, optional
        The benchmark string for the geocoding request. Default value is specified by `BENCHMARK`.
    batch : bool, optional
        Whether or not the function is being used in a batch process. Default value is False.

    Returns
    -------
    dict
        A dictionary with the geocoding result, containing the following keys:
        - Address : str
            The original requested address.
        - Date : str
            The current date of the request.
        - Longitude : float or None
            Longitude of the geocoded address, or None if geocoding was unsuccessful.
        - Latitude : float or None
            Latitude of the geocoded address, or None if geocoding was unsuccessful.
    """

    base_geocode_url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
    geocode_params = {
        'benchmark': benchmark,
        'format': 'json',
        'address': address
    }

    today = date.today().strftime('%Y-%m-%d')

    def successful_response(requested_address, response_coordinates):
        """ Construct and return a successful geocode response. """
        longitude = response_coordinates['x']
        latitude = response_coordinates['y']
        longitude_latitude = (longitude, latitude)
        response = {
            'Address': requested_address,
            'Date': today,
            'Longitude': longitude,
            'Latitude': latitude,
            'Coordinates': longitude_latitude
        }

        return response

    def failed_response(requested_address):
        """ Construct and return a failed geocode response. """
        response = {
            'Address': requested_address,
            'Date': today,
            'Longitude': None,
            'Latitude': None,
            'Coordinates': None
        }

        return response

    for t in timeouts:
        # Try request for address geocode
        try:
            geocode_req = requests.get(base_geocode_url, params=geocode_params, timeout=t)
            geocode_data = geocode_req.json()

            # If the request was successful but didn't match an address
            if 'result' in geocode_data and not geocode_data['result']['addressMatches']:
                sleep(sleep_delay)
                if batch:
                    return failed_response(address)
                else:
                    print(f'Address {address} did not match any records.')
                    return None

            # If the request was successful and matched an address return first match
            elif 'result' in geocode_data and geocode_data['result']['addressMatches']:
                coordinates = geocode_data['result']['addressMatches'][0]['coordinates']
                sleep(sleep_delay)
                return successful_response(address, coordinates)

        # Handle JSON decoding error
        except ValueError:
            sleep(sleep_delay)
            if batch:
                return failed_response(address)
            else:
                print('Decoding JSON has failed for address: ' + address)
                return None

        # Handle request timeout
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

        # Handle any other unforeseen requests-related exceptions
        except requests.exceptions.RequestException as e:
            sleep(sleep_delay)
            if batch:
                return failed_response(address)
            else:
                print(f'Request exception occurred for address {address}: {e}')
                return None


def geocode_coordinates(longitude_latitude, benchmark=BENCHMARK, vintage=VINTAGE, batch=False):
    """
    Request geographical information based on given coordinates using the U.S. Census Geocoder.

    Parameters
    ----------
    longitude_latitude : tuple of (float, float)
        A tuple of (longitude, latitude) to geocode.
    benchmark : str, optional
        The benchmark string for the geocoding request. Default value is specified by `BENCHMARK`.
    vintage : str, optional
        The vintage string for the geocoding request. Default value is specified by `VINTAGE`.
    batch : bool, optional
        Whether or not the function is being used in a batch process. Default value is False.

    Returns
    -------
    dict
        A dictionary with the geocoding result, containing:
        - Coordinates : tuple of (float, float)
            The original requested (longitude, latitude).
        - Date : str
            The current date of the request.
        - State : str or None
            The state where the coordinates are located, or None if geocoding was unsuccessful.
        - County : str or None
            The county where the coordinates are located, or None if geocoding was unsuccessful.
        - Census Block : str or None
            The census block of the coordinates, or None if geocoding was unsuccessful.
        - Census Tract : str or None
            The census tract of the coordinates, or None if geocoding was unsuccessful.
    """

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

    def successful_response(requested_longitude, requested_latitude, response_geographies):
        """ Construct and return a successful geocode response. """
        response = {
            'Coordinates': (requested_longitude, requested_latitude),
            'Date': today,
            'State': response_geographies['States'][0]['BASENAME'],
            'County': response_geographies['Counties'][0]['BASENAME'],
            'Census Block': response_geographies['2020 Census Blocks'][0]['BASENAME'],
            'Census Tract': response_geographies['Census Tracts'][0]['BASENAME']
        }

        return response

    def failed_response(req_longitude, req_latitude):
        """Construct and return a failed geocode response."""
        response = {
            'Coordinates': (req_longitude, req_latitude),
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

            # If the request was successful but didn't match an address
            if 'result' in geocode_data and len(geocode_data['result']['geographies']) == 0:
                sleep(sleep_delay)
                if batch:
                    print(failed_response(longitude, latitude))
                    return failed_response(longitude, latitude)
                else:
                    print(f'Coordinates ({longitude}, {latitude}) did not match any records.')
                    return None

            # If the request was successful and contains the 'result' key
            elif 'result' in geocode_data:
                geographies = geocode_data['result']['geographies']
                return successful_response(longitude, latitude, geographies)

        # Handle JSON decoding error
        except ValueError:
            sleep(sleep_delay)
            if batch:
                print(failed_response(longitude, latitude))
                return failed_response(longitude, latitude)
            else:
                print(f'Decoding JSON has failed for coordinates: ({longitude}, {latitude})')
                return None

        # Handle request timeout
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

        # Handle any other unforeseen requests-related exceptions
        except requests.exceptions.RequestException as e:
            sleep(sleep_delay)
            if batch:
                print(failed_response(longitude, latitude))
                return failed_response(longitude, latitude)
            else:
                print(f'Request exception occurred for coordinates ({longitude}, {latitude}): {e}')
                return None


def batch_geocode(data, direction='forward', n_threads=1):
    """
    Batch geocoding function that supports both forward and reverse geocoding.

    Parameters
    ----------
    data : list or set of str or tuple
        A collection of addresses (for forward geocoding) or coordinates (for reverse geocoding) to be geocoded.
    direction : str, optional
        Direction of geocoding:
        - 'forward' for addresses.
        - 'reverse' for coordinates.
        Default is 'forward'.
    n_threads : int, optional
        Number of threads to be used for parallel processing. Default is 1.

    Returns
    -------
    located_df : pd.DataFrame
        DataFrame with successfully geocoded data. Columns vary based on `direction`:
        - 'forward': ['Address', 'Date', 'Longitude', 'Latitude', 'Coordinates']
        - 'reverse': ['Coordinates', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract']
    failed_df : pd.DataFrame
        DataFrame with data that couldn't be geocoded. Columns are consistent with `located_df`.

    Raises
    ------
    ValueError
        If the `direction` parameter is neither 'forward' nor 'reverse'.

    Notes
    -----
    If `n_threads` is set higher than 100, a warning will be displayed with a recommendation to set `n_threads` to 100
    to avoid potential rate limits.
    """

    # Raise error if invalid direction
    if direction not in ['forward', 'reverse']:
        raise ValueError('direction must be either "forward" or "reverse"')

    # Show warning if n_threads is set very high and ask user if they want to set n_threads to 100
    if n_threads > 100:
        print('WARNING: n_threads is set very high and you may experience rate limits.')
        print('Would you like to set n_threads to the recommended max of 100? (y/n)')
        response = input()
        if response == 'y':
            n_threads = 100
        else:
            print('If this process fails, try reducing n_threads to 100 or less.')

    # Convert data to set to remove duplicates
    data = set(data)

    # Define columns for the output DataFrames based on direction
    forward_cols = ['Address', 'Date', 'Longitude', 'Latitude', 'Coordinates']
    reverse_cols = ['Coordinates', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract']

    # Select geocoding function based on direction
    located_df = pd.DataFrame()
    if direction == 'forward':
        request = geocode_address
        output_cols = forward_cols

    elif direction == 'reverse':
        request = geocode_coordinates
        output_cols = reverse_cols

    # Wrapper function to set geocoding requests to batch mode
    def batch_request(batch_data):
        return request(batch_data, batch=True)

    # Initialize empty lists to hold results
    located_results = []
    failed_results = []

    # Use ThreadPoolExecutor to execute geocoding requests in parallel
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for result in executor.map(batch_request, data):
            if result[next(reversed(result.keys()))] is not None:
                located_results.append(result)
            else:
                failed_results.append(result)

    # Convert lists to DataFrames
    located_df = pd.DataFrame(located_results, columns=output_cols)
    failed_df = pd.DataFrame(failed_results, columns=output_cols)

    return located_df, failed_df
