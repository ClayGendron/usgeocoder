import pandas as pd
import os
from pathlib import Path

from .utils import create_address_list, create_coordinates_list
from .census_api import batch_geocode


ROOT = Path(os.getcwd())


class Geocoder:
    """
    A class to manage the geocoding process by performing forward and reverse geocoding and saving the results locally.

    Attributes
    ----------
    addresses : pd.Series
        Series of addresses to be geocoded.
    coordinates : pd.Series
        Series of coordinates for reverse geocoding.
    located_addresses : pd.DataFrame
        Addresses that have been successfully geocoded.
    failed_coordinates : pd.DataFrame
        Coordinates that failed reverse geocoding.
    located_coordinates : pd.DataFrame
        Coordinates that have been successfully reverse geocoded.
    failed_addresses : pd.DataFrame
        Addresses that failed geocoding.

    Methods
    -------
    load_or_create_csv(file_name, columns) -> pd.DataFrame
        Load an existing CSV file or create a new one if it doesn't exist.
    add_addresses(data)
        Add addresses to the Geocoder instance.
    add_coordinates(data)
        Add coordinates to the Geocoder instance.
    forward(addresses=None)
        Conduct forward geocoding on the provided addresses.
    reverse(coordinates=None)
        Conduct reverse geocoding on the provided coordinates.
    save_data()
        Save geocoding results to CSV files.
    delete_data(records='failed', time=365)
        Filter out geocoding results older than the specified time.
    """

    def __init__(self, data=None):
        """ Initializes the Geocoder instance. Loads or creates necessary CSV files for storing results. """
        # Initialize attributes
        self.data = None
        self.addresses = None
        self.coordinates = None
        self.located_addresses = None
        self.failed_coordinates = None
        self.located_coordinates = None
        self.failed_addresses = None

        # Initialize CSV files
        files = {
            'located_addresses': ['Address', 'Date', 'Longitude', 'Latitude', 'Coordinates'],
            'failed_addresses': ['Address', 'Date', 'Longitude', 'Latitude', 'Coordinates'],
            'located_coordinates': ['Coordinates', 'Date', 'State', 'County', 'Census Block', 'Census Tract'],
            'failed_coordinates': ['Coordinates', 'Date', 'State', 'County', 'Census Block', 'Census Tract'],
        }

        # Load existing CSV files or create new ones if they don't exist
        if (ROOT / 'geocoder').exists():
            for file_name, columns in files.items():
                setattr(self, file_name, self.load_or_create_csv(file_name, columns))
        else:
            (ROOT / 'geocoder').mkdir()
            for file_name, columns in files.items():
                df = pd.DataFrame(columns=columns)
                df.to_csv(ROOT / 'geocoder' / f'{file_name}.csv', index=False)
                setattr(self, file_name, df)

        # Add data if provided
        if data is not None:
            self.add_data(data)

    @staticmethod
    def load_or_create_csv(file_name, columns):
        """
        Load an existing CSV file or create a new one if it doesn't exist.

        Parameters
        ----------
        file_name : str
            The name of the CSV file to be loaded or created.
        columns : list of str
            List of column names for the CSV file.

        Returns
        -------
        pd.DataFrame
            Loaded data or an empty DataFrame with specified columns.
        """

        path = ROOT / 'geocoder' / f'{file_name}.csv'
        if path.exists():
            return pd.read_csv(path)
        
        else:
            print(f'{file_name}.csv does not exist. Creating a new one.')
            print(f'If you have an existing {file_name}.csv data, move it to the geocoder directory.')
            df = pd.DataFrame(columns=columns)
            df.to_csv(path, index=False)
            return pd.DataFrame(columns=columns)

    def add_data(self, data):
        """
        Add data to the Geocoder instance.

        Parameters
        ----------
        data : pd.DataFrame
            Data containing addresses or coordinates
        """

        # Ensure that pd.DataFrame contains an Address or Coordinates column
        if isinstance(data, pd.DataFrame):
            # Check for Coordinates first to avoid unnecessary forward geocoding
            if 'Coordinates' in data.columns:
                self.add_coordinates(data['Coordinates'])
            elif 'Address' in data.columns:
                self.add_addresses(data['Address'])
            else:
                raise ValueError('Data must contain an Address or Coordinates column.')

            self.data = data.copy()

        # Raise an error if data is not a pandas dataframe
        else:
            raise TypeError('Data must be a pandas dataframe.')

    def add_addresses(self, data):
        """
        Add addresses to the Geocoder instance.

        Parameters
        ----------
        data : pd.DataFrame, pd.Series, list
            Data containing addresses.
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

        Parameters
        ----------
        data : pd.DataFrame, pd.Series, list
            Data containing coordinates.
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

    def forward(self, addresses=None, verbose=False):
        """
        Conduct forward geocoding on the provided addresses.

        Parameters
        ----------
        addresses : pd.DataFrame, pd.Series, optional
            Uses addresses stored in the instance if not provided.
        verbose : bool, optional
            Print progress to console. Default is False.

        Raises
        ------
        ValueError: If no addresses are provided to instance.
        ValueError: If no addresses are successfully geocoded.
        """

        # Add addresses to self.addresses if given
        if addresses is not None:
            self.add_addresses(addresses)
        # Ensure that Addresses have been provided to Geocoder
        if self.addresses is None:
            raise ValueError('No addresses were provided to Geocoder instance. Forward geocoding failed.'
                             'Please add addresses to Geocoder instance or provide addresses to forward() method.')

        # Load addresses from self.addresses and convert to set
        addresses = set(self.addresses)
        # Remove any addresses that have already been geocoded
        located_addresses = self.located_addresses['Address'].values
        failed_addresses = self.failed_addresses['Address'].values
        for seen_addresses in [located_addresses, failed_addresses]:
            addresses = addresses.difference(seen_addresses)

        # Print the number of addresses to be geocoded
        if verbose:
            number_of_addresses = len(addresses)
            number_of_addresses = f'{number_of_addresses:,}'
            print(f'Geocoding {number_of_addresses} addresses...')

        # Batch geocoder
        located_df, failed_df = batch_geocode(data=addresses, direction='forward', n_threads=100)

        # Add geocoding results to self.located_addresses and self.failed_addresses
        # Raise an error if no addresses were successfully geocoded
        if located_df.empty:
            raise ValueError('No addresses were successfully geocoded. Review Geocoder.addresses.')
        # If self.located_addresses is empty, set it to located_df
        elif self.located_addresses.empty:
            self.located_addresses = located_df.copy()
        # Otherwise, concatenate located_df to self.located_addresses
        else:
            self.located_addresses = pd.concat([self.located_addresses, located_df], ignore_index=True)

        # Pass if failed_df is empty
        if failed_df.empty:
            pass
        # If self.failed_addresses is empty, set it to failed_df
        elif self.failed_addresses.empty:
            self.failed_addresses = failed_df.copy()
        # Otherwise, concatenate failed_df to self.failed_addresses
        else:
            self.failed_addresses = pd.concat([self.failed_addresses, failed_df], ignore_index=True)

        # Add geocoding results to self.coordinates if not already there
        if self.coordinates is None:
            self.add_coordinates(self.located_addresses)

        # Print the number of addresses located and failed addresses
        if verbose:
            number_of_located_addresses = len(located_df)
            number_of_located_addresses = f'{number_of_located_addresses:,}'
            number_of_failed_addresses = len(failed_df)
            number_of_failed_addresses = f'{number_of_failed_addresses:,}'

            print('Geocoding complete')
            print(f' - {number_of_located_addresses} addresses were located')
            print(f' - {number_of_failed_addresses} addresses failed')

        self.save_data()

    def reverse(self, coordinates=None, verbose=False):
        """
        Conduct reverse geocoding on the provided coordinates.

        Parameters
        ----------
        coordinates : pd.DataFrame, pd.Series, optional
            Uses coordinates stored in the instance if not provided.
        verbose : bool, optional
            Print progress to console. Default is False.

        Raises
        ------
        ValueError: If no coordinates are provided to instance.
        ValueError: If no coordinates are successfully geocoded.
        """

        # Add coordinates to self.coordinates if given
        if coordinates is not None:
            self.add_coordinates(coordinates)

        # Ensure that Coordinates have been provided to Geocoder
        if self.coordinates is None:
            raise ValueError('No coordinates were provided to Geocoder instance. Reverse geocoding failed.'
                             'Please add coordinates to Geocoder instance or provide coordinates to reverse() method.')

        # Load coordinates from self.coordinates and convert to set
        coordinates = set(self.coordinates)
        
        # Remove any coordinates that have already been geocoded
        located_coordinates = self.located_coordinates['Coordinates'].values
        failed_coordinates = self.failed_coordinates['Coordinates'].values
        for seen_coordinates in [located_coordinates, failed_coordinates]:
            coordinates = coordinates.difference(seen_coordinates)

        # Print the number of coordinates to be geocoded
        if verbose:
            number_of_coordinates = len(coordinates)
            number_of_coordinates = f'{number_of_coordinates:,}'
            print(f'Reverse geocoding {number_of_coordinates} coordinates...')

        # Batch geocoder
        located_df, failed_df = batch_geocode(data=coordinates, direction='reverse', n_threads=100)

        # Add geocoding results to self.located_coordinates and self.failed_coordinates
        # Raise an error if no coordinates were successfully geocoded
        if located_df.empty:
            raise ValueError('No coordinates were successfully geocoded. Review Geocoder.coordinates data.')
        # If self.located_coordinates is None, set it to located_df
        elif self.located_coordinates is None:
            self.located_coordinates = located_df.copy()
        # Otherwise, concatenate located_df to self.located_coordinates
        else:
            self.located_coordinates = pd.concat([self.located_coordinates, located_df], ignore_index=True)

        # Pass if failed_df is empty
        if failed_df.empty:
            pass
        # If self.failed_coordinates is None, set it to failed_df
        elif self.failed_coordinates is None:
            self.failed_coordinates = failed_df.copy()
        # Otherwise, concatenate failed_df to self.failed_coordinates
        else:
            self.failed_coordinates = pd.concat([self.failed_coordinates, failed_df], ignore_index=True)

        # Print the number of coordinates located and failed coordinates
        if verbose:
            number_of_located_coordinates = len(located_df)
            number_of_located_coordinates = f'{number_of_located_coordinates:,}'
            number_of_failed_coordinates = len(failed_df)
            number_of_failed_coordinates = f'{number_of_failed_coordinates:,}'

            print('Reverse geocoding complete')
            print(f' - {number_of_located_coordinates} coordinates were located')
            print(f' - {number_of_failed_coordinates} coordinates failed')
        
        self.save_data()

    def merge_data(self, data=None, verbose=False):
        """
        Merge data with located_addresses and located_coordinates.

        Parameters
        ----------
        data : pd.DataFrame, optional
            Data to be merged with located_addresses and located_coordinates.
        verbose : bool, optional
            Print progress to console. Default is False.

        Raises
        ------
        ValueError: If no data is provided to instance.
        ValueError: If no data is successfully merged.
        """

        if data is not None:
            self.add_data(data)

        if self.data is None:
            raise ValueError('No data was provided to Geocoder instance. Data merge failed.'
                             'Please add data to Geocoder instance or provide data to merge_data() method.')

        # Merge data
        if 'Coordinates' in self.data.columns:
            if self.located_coordinates is None:
                raise ValueError('No coordinates have been successfully geocoded. Data merge failed.'
                                 'Please run reverse() method to reverse geocode coordinate data.')

            self.data = self.data.merge(self.located_coordinates, how='left', on='Coordinates')

        elif 'Address' in self.data.columns:
            if self.located_addresses is None:
                raise ValueError('No addresses have been successfully geocoded. Data merge failed.'
                                 'Please run forward() method to geocode address data.')

            self.data = self.data.merge(self.located_addresses, how='left', on='Address')

            if self.located_coordinates is not None:
                self.data = self.data.merge(self.located_coordinates, how='left', on='Coordinates')

        if verbose:
            print('Data merge complete')

    def process(self, forward=True, reverse=True, merge=True, data=None, verbose=False):
        """
        Process data by conducting forward and reverse geocoding and merging the results.

        Parameters
        ----------
        forward : bool, optional
            Conduct forward geocoding. Default is True.
        reverse : bool, optional
            Conduct reverse geocoding. Default is True.
        merge : bool, optional
            Merge data with located_addresses and located_coordinates. Default is True.
        data : pd.DataFrame, optional
            Data to be processed.
        verbose : bool, optional
            Print progress to console. Default is False.

        Returns
        -------
        pd.DataFrame
            Data with geocoding results if merge=True.
        """

        if data is not None:
            self.add_data(data)

        if forward:
            self.forward(verbose=verbose)
            if verbose:
                print()
        if reverse:
            self.reverse(verbose=verbose)
            if verbose:
                print()
        if merge:
            self.merge_data(verbose=verbose)
            if verbose:
                print()

        if verbose:
            print('Processing complete')

        if merge:
            return self.data

    def save_data(self):
        """ Save geocoding results to CSV files. """
        self.located_addresses.to_csv(ROOT / 'geocoder/located_addresses.csv', index=False)
        self.failed_addresses.to_csv(ROOT / 'geocoder/failed_addresses.csv', index=False)
        self.located_coordinates.to_csv(ROOT / 'geocoder/located_coordinates.csv', index=False)
        self.failed_coordinates.to_csv(ROOT / 'geocoder/failed_coordinates.csv', index=False)

    def delete_data(self, records='failed', time=365):
        """
        Filter out geocoding results older than the specified time.

        Parameters
        ----------
        records : str, optional
            Type of records to filter. Options are 'failed', 'located', or 'all'. Default is 'failed'.
        time : int or str, optional
            Number of days to keep geocoding results. Can also be 'week', 'month', 'year', or 'all'. Default is 365.

        Raises
        ------
        ValueError
            If time is not an integer or one of 'week', 'month', 'year', or 'all'.
        ValueError
            If records is not 'failed', 'located', or 'all'.
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
