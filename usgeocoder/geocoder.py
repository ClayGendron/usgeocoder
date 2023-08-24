import pandas as pd
from pyprojroot import here

from .utils import create_address_list, create_coordinates_list
from .census_api import batch_geocoder

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pyprojroot')


class Geocoder:
    def __init__(self):
        self.addresses = pd.Series()
        self.coordinates = pd.Series()
        self.located_addresses = None
        self.failed_coordinates = None
        self.located_coordinates = None
        self.failed_addresses = None

        files = {
            'located_addresses': ['Address', 'Date', 'Longitude', 'Latitude'],
            'failed_addresses': ['Address', 'Date', 'Longitude', 'Latitude'],
            'located_coordinates': ['Coordinates', 'Date', 'State', 'County', 'Census Block', 'Census Tract'],
            'failed_coordinates': ['Coordinates', 'Date', 'State', 'County', 'Census Block', 'Census Tract'],
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

    def add_addresses(self, data):
        if isinstance(data, pd.DataFrame):
            self.addresses = create_address_list(data)
        elif isinstance(data, pd.Series):
            self.addresses = data
        else:
            try:
                self.addresses = pd.Series(data)
                
            except TypeError:
                print('Data must be a pandas dataframe, series, or list.')
                return None

    def add_coordinates(self, data):
        if isinstance(data, pd.DataFrame):
            self.coordinates = create_coordinates_list(data)
        elif isinstance(data, pd.Series):
            self.coordinates = data
        else:
            try:
                self.coordinates = pd.Series(data)
                
            except TypeError:
                print('Data must be a pandas dataframe, series, or list.')
                return None

    def forward(self, addresses=None):
        if addresses is not None:
            # add addresses to self.addresses if given
            self.add_addresses(addresses)

        # load addresses from self.addresses and convert to set
        addresses = set(self.addresses)
        # remove any addresses that have already been geocoded
        located_addresses = self.located_addresses['Address'].values
        failed_addresses = self.failed_addresses['Address'].values
        for seen_addresses in [located_addresses, failed_addresses]:
            addresses = addresses.difference(seen_addresses)

        located_df, failed_df = batch_geocoder(data=addresses, direction='forward', n_threads=100)
        self.located_addresses = pd.concat([self.located_addresses, located_df], ignore_index=True)
        self.failed_addresses = pd.concat([self.failed_addresses, failed_df], ignore_index=True)

        self.save_data()

    def reverse(self, coordinates=None):
        if coordinates is not None:
            # add coordinates to self.coordinates if given
            self.add_coordinates(coordinates)

        # load coordinates from self.coordinates and convert to set
        coordinates = set(self.coordinates)
        
        # remove any coordinates that have already been geocoded
        located_coordinates = self.located_coordinates['Coordinates'].values
        failed_coordinates = self.failed_coordinates['Coordinates'].values
        for seen_coordinates in [located_coordinates, failed_coordinates]:
            coordinates = coordinates.difference(seen_coordinates)
        
        located_df, failed_df = batch_geocoder(data=coordinates, direction='reverse', n_threads=100)
        self.located_coordinates = pd.concat([self.located_coordinates, located_df], ignore_index=True)
        self.failed_coordinates = pd.concat([self.failed_coordinates, failed_df], ignore_index=True)
        
        self.save_data()

    def save_data(self):
        self.located_addresses.to_csv(here('geocoder/located_addresses.csv'), index=False)
        self.failed_addresses.to_csv(here('geocoder/failed_addresses.csv'), index=False)
        self.located_coordinates.to_csv(here('geocoder/located_coordinates.csv'), index=False)
        self.failed_coordinates.to_csv(here('geocoder/failed_coordinates.csv'), index=False)
