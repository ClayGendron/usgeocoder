import pandas as pd
from datetime import date
from pyprojroot import here

from .utils import create_address_list, create_coordinates_list
from .census_api import request_address_geocode, request_coordinates_geocode, batch_geocoder

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pyprojroot')


class Geocoder:
    def __init__(self):
        self.data = pd.DataFrame()
        self.addresses = pd.Series()
        self.coordinates = pd.Series()
        
        files = {
            'located_addresses': ['Address', 'Date', 'Longitude', 'Latitude'],
            'failed_addresses': ['Address', 'Date', 'Longitude', 'Latitude'],
            'located_coordinates': ['Longitude', 'Latitude', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract'],
            'failed_coordinates': ['Longitude', 'Latitude', 'Date', 'State', 'County', 'Urban Area', 'Census Block', 'Census Tract'],
        }

        if here('geocoder').exists():
            for file_name, columns in files.items():
                setattr(self, file_name, self._load_or_create_csv(file_name, columns))
        else:
            here('geocoder').mkdir()
            for file_name, columns in files.items():
                df = pd.DataFrame(columns=columns)
                df.to_csv(here(f'geocoder/{file_name}.csv'), index=False)
                setattr(self, file_name, df)

    @staticmethod
    def _load_or_create_csv(file_name, columns):
        path = here(f'geocoder/{file_name}.csv')
        if path.exists():
            return pd.read_csv(path)
        
        else:
            print(f'{file_name}.csv does not exist. Creating a new one.')
            print(f'If you have an existing {file_name}.csv data, move it to the geocoder directory.')
            df = pd.DataFrame(columns=columns)
            df.to_csv(path, index=False)
            return pd.DataFrame(columns=columns)

    def add_data(self, data):
        self.addresses = create_address_list(data)
        self.data = data

    def run(self):
        self._forward_geocoder(self.addresses)
        self.save_data()

    def save_data(self):
        self.located_addresses.to_csv(here('geocoder/located_addresses.csv'), index=False)
        self.failed_addresses.to_csv(here('geocoder/failed_addresses.csv'), index=False)

    def _forward_geocoder(self, addresses, n_threads=100):
        # remove any addresses that have already been geocoded
        for address_list in [self.located_addresses['Address'].values, self.failed_addresses['Address'].values]:
            addresses = addresses.difference(address_list)
        
        located_df, failed_df = batch_geocoder(
            data=addresses, 
            direction='forward', 
            request=self._forward_request, 
            n_threads=n_threads
        )
        
        self.located_addresses = pd.concat([self.located_addresses, located_df], ignore_index=True)
        self.failed_addresses = pd.concat([self.failed_addresses, failed_df], ignore_index=True)
        self.save_data()
        
        
class ReverseGeocoder:
    def __init__(self):
        # check if geocoder directory exists, if not create it
        if here('geocoder').exists():
            try:
                self.located_coordinates = pd.read_csv(here('geocoder/located_coordinates.csv'))
            except FileNotFoundError:
                print('Geocoder directory exists but located_coordinates.csv does not, creating new one.')
                print('If you have an existing located_coordinates.csv data, move it to the geocoder directory.')
                self.located_coordinates = pd.DataFrame(columns=['Longitude', 'Latitude', 'Date', 'Address'])

            try:
                self.failed_coordinates = pd.read_csv(here('geocoder/failed_coordinates.csv'))
            except FileNotFoundError:
                print('Geocoder directory exists but failed_coordinates.csv does not. Creating new one.')
                print('If you have an existing failed_coordinates.csv data, move it to the geocoder directory.')
                self.failed_coordinates = pd.DataFrame(columns=['Longitude', 'Latitude', 'Date'])

        else:
            here('geocoder').mkdir()
            self.located_coordinates = pd.DataFrame(columns=['Longitude', 'Latitude', 'Date', 'Address'])
            self.located_coordinates.to_csv(here('geocoder/located_coordinates.csv'), index=False)

            self.failed_coordinates = pd.DataFrame(columns=['Longitude', 'Latitude', 'Date'])
            self.failed_coordinates.to_csv(here('geocoder/failed_coordinates.csv'), index=False)

        # initialize empty data and addresses
        self.data = pd.DataFrame()
        self.coordinates = pd.Series()
        self.path = here('geocoder')

    def add_data(self, data):
        self.coordinates = data[['Longitude', 'Latitude']]
        self.data = data

    def run(self):
        self._reverse_geocoder(self.coordinates)
        self.save_data()

    def save_data(self):
        self.located_coordinates.to_csv(here('geocoder/located_coordinates.csv'), index=False)
        self.failed_coordinates.to_csv(here('geocoder/failed_coordinates.csv'), index=False)

    def _reverse_request(self, coordinates):
        # if request_address_geocode returns None, return the address and today's date
        address = request_coordinates_geocode(coordinates)
        if address is not None:
            return address
        else:
            return {'Latitude': coordinates[0], 'Longitude': coordinates[1], 'Date': date.today().strftime('%Y-%m-%d')}

    def _reverse_geocoder(self, coordinates, n_threads=100):
        # remove any addresses that have already been geocoded
        for coordinates_list in [self.located_coordinates[['Longitude', 'Latitude']].values, self.failed_coordinates[['Longitude', 'Latitude']].values]:
            coordinates = coordinates.difference(coordinates_list)
        
        located_df, failed_df = batch_geocoder(
            data=coordinates, 
            direction='reverse', 
            request=self._reverse_request, 
            n_threads=n_threads
        )
        
        self.located_coordinates = pd.concat([self.located_coordinates, located_df], ignore_index=True)
        self.failed_coordinates = pd.concat([self.failed_coordinates, failed_df], ignore_index=True)
        self.save_data()
        
def delete_failed_addresses(self, date=None):
    today = date.today()

    if date is None:
        failed_addresses_num = len(self.failed_addresses)
        print(f'Attempting to delete all {failed_addresses_num} failed addresses.')
        print('Future attempts to geocode addresses will try these addresses again.')
        print('Are you sure you want to delete all failed addresses? (y/n)')
        response = input()

        if response == 'y':
            self.failed_addresses = pd.DataFrame(columns=['Address', 'Date'])
            self.save_data()
            print('Successfully deleted all failed addresses.')
            return None

        else:
            print('Aborting address deletion.')
            return None

    # if date is a number, delete all failed addresses n days old or older
    if date.isdigit():
        date = today - pd.Timedelta(days=date)

    # if date is a string, delete all failed addresses on that date or older
    elif isinstance(date, str):
        date = pd.to_datetime(date)

    elif isinstance(date, pd.Timestamp):
        date = date

    failed_addresses_num = len(self.failed_addresses[pd.to_datetime(self.failed_addresses['Date']) <= date])
    print(f'Attempting to delete all {failed_addresses_num} failed addresses from {date} or earlier.')
    print('Future attempts to geocode addresses will try these addresses again.')
    print('Are you sure you want to delete all failed addresses? (y/n)')
    response = input()

    if response == 'y':
        self.failed_addresses = self.failed_addresses[pd.to_datetime(self.failed_addresses['Date']) > date]
        self.save_data()
        print('Successfully deleted failed addresses.')

    else:
        print('Aborting address deletion.')