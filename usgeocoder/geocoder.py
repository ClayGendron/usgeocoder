import pandas as pd
from pyprojroot import here

from .utils import create_address_list, create_coordinates_list
from .census_api import batch_geocoder

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pyprojroot')


class Geocoder:
    """
    A class to manage the geocoding process by performing forward and reverse geocoding and saving the results locally.

    Attributes:
    - addresses (pd.Series): Series of addresses to be geocoded.
    - coordinates (pd.Series): Series of coordinates for reverse geocoding.
    - located_addresses (DataFrame): Addresses that have been successfully geocoded.
    - failed_coordinates (DataFrame): Coordinates that failed reverse geocoding.
    - located_coordinates (DataFrame): Coordinates that have been successfully reverse geocoded.
    - failed_addresses (DataFrame): Addresses that failed geocoding.
    """

    def __init__(self):
        """ Initializes the Geocoder instance. Loads or creates necessary CSV files for storing results. """
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
                setattr(self, file_name, self.load_or_create_csv(file_name, columns))
        else:
            here('geocoder').mkdir()
            for file_name, columns in files.items():
                df = pd.DataFrame(columns=columns)
                df.to_csv(here(f'geocoder/{file_name}.csv'), index=False)
                setattr(self, file_name, df)

    @staticmethod
    def load_or_create_csv(file_name, columns):
        """
        Load an existing CSV file or create a new one if it doesn't exist.

        Parameters:
        - file_name (str): The name of the CSV file to be loaded or created.
        - columns (list of str): List of column names for the CSV file.

        Returns:
        - DataFrame: Loaded data or an empty DataFrame with specified columns.
        """

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
        """
        Add addresses to the Geocoder instance.

        Parameters:
            - data (pd.DataFrame, pd.Series, list): Data containing addresses.
        """
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
        """
        Add coordinates to the Geocoder instance.

        Parameters:
            - data (pd.DataFrame, pd.Series, list): Data containing coordinates.
        """
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
        """
        Conduct forward geocoding on the provided addresses.

        Parameters:
        - addresses (pd.DataFrame, pd.Series, optional): Uses addresses stored in the instance if not provided.
        """

        if addresses is not None:
            # add addresses to self.addresses if given
            self.add_addresses(addresses)

        # Load addresses from self.addresses and convert to set
        addresses = set(self.addresses)
        # Remove any addresses that have already been geocoded
        located_addresses = self.located_addresses['Address'].values
        failed_addresses = self.failed_addresses['Address'].values
        for seen_addresses in [located_addresses, failed_addresses]:
            addresses = addresses.difference(seen_addresses)

        located_df, failed_df = batch_geocoder(data=addresses, direction='forward', n_threads=100)
        self.located_addresses = pd.concat([self.located_addresses, located_df], ignore_index=True)
        self.failed_addresses = pd.concat([self.failed_addresses, failed_df], ignore_index=True)

        # Add geocoding results to self.coordinates if not already there
        if self.coordinates is None:
            self.add_coordinates(self.located_addresses)

        self.save_data()

    def reverse(self, coordinates=None):
        """
        Conduct reverse geocoding on the provided coordinates.

        Parameters:
        - coordinates (pd.DataFrame, pd.Series, optional): Uses coordinates stored in the instance if not provided.
        """

        if coordinates is not None:
            # add coordinates to self.coordinates if given
            self.add_coordinates(coordinates)

        # Load coordinates from self.coordinates and convert to set
        coordinates = set(self.coordinates)
        
        # Remove any coordinates that have already been geocoded
        located_coordinates = self.located_coordinates['Coordinates'].values
        failed_coordinates = self.failed_coordinates['Coordinates'].values
        for seen_coordinates in [located_coordinates, failed_coordinates]:
            coordinates = coordinates.difference(seen_coordinates)
        
        located_df, failed_df = batch_geocoder(data=coordinates, direction='reverse', n_threads=100)
        self.located_coordinates = pd.concat([self.located_coordinates, located_df], ignore_index=True)
        self.failed_coordinates = pd.concat([self.failed_coordinates, failed_df], ignore_index=True)
        
        self.save_data()

    def save_data(self):
        """ Save geocoding results to CSV files. """
        self.located_addresses.to_csv(here('geocoder/located_addresses.csv'), index=False)
        self.failed_addresses.to_csv(here('geocoder/failed_addresses.csv'), index=False)
        self.located_coordinates.to_csv(here('geocoder/located_coordinates.csv'), index=False)
        self.failed_coordinates.to_csv(here('geocoder/failed_coordinates.csv'), index=False)

    def delete_data(self, records='failed', time=365):
        """
        Filter out geocoding results older than the specified time.

        Parameters:
        - records (str): Type of records to filter. Options are 'failed', 'located', or 'all'.
        - time (int, str): Number of days to keep geocoding results. Can also be 'week', 'month', 'year', or 'all'.

        Raises:
        - ValueError: If time is not an integer or one of 'week', 'month', 'year', or 'all'.
        - ValueError: If records is not 'failed', 'located', or 'all'.
        """
        # Validate and interpret the 'time' parameter
        time_map = {'week': 7, 'month': 30, 'year': 365, 'all': 999999}
        time = time_map.get(time, time)

        try:
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=int(time))
        except ValueError:
            print("Time must be an integer or one of 'week', 'month', 'year', or 'all'.")
            return None

        # Define a dictionary to store references to the relevant attributes
        data_refs = {
            'located_addresses': self.located_addresses,
            'failed_addresses': self.failed_addresses,
            'located_coordinates': self.located_coordinates,
            'failed_coordinates': self.failed_coordinates
        }

        filtered_data = {}

        # Determine which records to filter
        if records == 'all':
            keys = data_refs.keys()
        elif records in ['located', 'failed']:
            keys = [key for key in data_refs.keys() if records in key]
        else:
            raise ValueError("Records must be 'all', 'located', or 'failed'.")

        # Filter the data
        for key in keys:
            filtered_data[key] = data_refs[key][data_refs[key]['Date'] > cutoff_date]

        # Display the number of records to be deleted
        print('Deleting data...')
        for key in keys:
            print(f' - {len(data_refs[key]) - len(filtered_data[key])} {key.replace("_", " ")}.')
        print()
        print('Geocoder will attempt to re-geocode these addresses and coordinates in the future.')
        print('This cannot be undone.')

        # Confirm the deletion with the user
        confirmation = input('Would you like to continue? (y/n) ')
        if confirmation == 'y':
            for key in keys:
                setattr(self, key, filtered_data[key])
            self.save_data()
            print('Data deletion complete.')
        else:
            print('Aborting data deletion.')
