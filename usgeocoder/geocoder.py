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
    failed_addresses : pd.DataFrame
        Addresses that failed geocoding.
    located_coordinates : pd.DataFrame
        Coordinates that have been successfully reverse geocoded.
    failed_coordinates : pd.DataFrame
        Coordinates that failed reverse geocoding.

    Methods
    -------
    add_data(data)
        Add data to the Geocoder instance.
    add_addresses(data)
        Add addresses to the Geocoder instance.
    add_coordinates(data)
        Add coordinates to the Geocoder instance.
    forward(addresses=None, verbose=False)
        Conduct forward geocoding on the provided addresses.
    reverse(coordinates=None, verbose=False)
        Conduct reverse geocoding on the provided coordinates.
    merge_data(data=None, verbose=False)
        Merge data with located_addresses and located_coordinates.
    process(forward=True, reverse=True, merge=True, data=None, verbose=True)
        Process data by conducting forward and reverse geocoding and merging the results.
    save_data()
        Save geocoding results to CSV files.
    delete_data(records='failed', time=365)
        Filter out geocoding results older than the specified time.
    """

    def __init__(self, data=None, path=ROOT):
        """ Initializes the Geocoder instance. Loads or creates necessary CSV files for storing results. """
        # Initialize attributes
        self.root = path
        self.data = None
        self.addresses = None
        self.coordinates = None
        self.address_cols = ['Address', 'Date', 'Longitude', 'Latitude', 'Coordinates']
        self.coordinate_cols = ['Coordinates', 'Date', 'State', 'County', 'CensusBlock', 'CensusTract']
        self.located_addresses = pd.DataFrame(columns=self.address_cols)
        self.failed_addresses = pd.DataFrame(columns=self.address_cols)
        self.located_coordinates = pd.DataFrame(columns=self.coordinate_cols)
        self.failed_coordinates = pd.DataFrame(columns=self.coordinate_cols)

        # Create geocoder directory
        if self.root is not None:
            # Initialize CSV files
            files = {
                'located_addresses': self.located_addresses,
                'failed_addresses': self.failed_addresses,
                'located_coordinates': self.located_coordinates,
                'failed_coordinates': self.failed_coordinates
            }
            # Load existing CSV files or create new ones if they don't exist
            if (self.root / 'geocoder').exists():
                for file_name, columns in files.items():
                    setattr(self, file_name, self.load_or_create_csv(file_name, columns))
            else:
                (self.root / 'geocoder').mkdir()
                for file_name, df in files.items():
                    df.to_csv(self.root / 'geocoder' / f'{file_name}.csv', index=False)

        # Add data if provided
        if data is not None:
            self.add_data(data)

    def load_or_create_csv(self, file_name, df):
        """
        Load an existing CSV file or create a new one if it doesn't exist.

        Parameters
        ----------
        file_name : str
            The name of the CSV file to be loaded or created.
        df: pd.DataFrame
            The table to be saved to the CSV file if it doesn't exist.

        Returns
        -------
        pd.DataFrame
            Loaded data or an empty DataFrame with specified columns.

        Raises
        ------
        ValueError
            If the path directory is set to None.
        """
        # Raise an error if path directory is set to None
        if self.root is None:
            raise ValueError('Path directory set to None. Please define Path to save geocoding data.')

        path = self.root / 'geocoder' / f'{file_name}.csv'
        if path.exists():
            return pd.read_csv(path)

        else:
            print(f'{file_name}.csv does not exist. Creating a new one.')
            print(f'If you have an existing {file_name}.csv data, move it to the geocoder directory.')
            df.to_csv(path, index=False)
            return df

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
        ValueError:
            If no addresses are provided to instance.
        ValueError:
            If no addresses are successfully geocoded.
        """

        # Add addresses to self.addresses if given
        if addresses is not None:
            self.add_addresses(addresses)
        # Ensure that Addresses have been provided to Geocoder
        if self.addresses is None:
            raise ValueError('No addresses were provided to Geocoder instance. Forward geocoding failed. '
                             'Please add addresses to Geocoder instance or provide addresses to forward() method.')

        # Load addresses from self.addresses and convert to set
        addresses = set(self.addresses)
        # Remove any addresses that have already been geocoded
        located_addresses = self.located_addresses['Address'].values
        failed_addresses = self.failed_addresses['Address'].values
        for seen_addresses in [located_addresses, failed_addresses]:
            addresses = addresses.difference(seen_addresses)

        # Batch geocoder
        if len(addresses) > 0:
            located_df, failed_df = batch_geocode(data=addresses, direction='forward', n_threads=100, verbose=verbose)

            # Add geocoding results to self.located_addresses and self.failed_addresses
            # Raise an error if no addresses were successfully geocoded
            if located_df.empty and len(self.located_addresses) == 0:
                raise ValueError('No addresses were successfully geocoded. Review Geocoder.addresses.')
            # If self.located_addresses is empty, set it to located_df
            elif self.located_addresses.empty:
                self.located_addresses = located_df.copy()
            # If located_df is empty, pass
            elif located_df.empty:
                pass
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

            # Print the number of addresses located and failed addresses
            if verbose:
                print('Forward geocoding complete')
                print(f' - {len(located_df):,} addresses were located')
                print(f' - {len(failed_df):,} addresses failed')

            if self.root is not None:
                self.save_data()

        else:
            print('All addresses have already been geocoded.')

        # Add geocoding results to self.coordinates if not already there
        if self.coordinates is None:
            self.add_coordinates(self.located_addresses)

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
            raise ValueError('No coordinates were provided to Geocoder instance. Reverse geocoding failed. '
                             'Please add coordinates to Geocoder instance or provide coordinates to reverse() method.')

        # Load coordinates from self.coordinates and convert to set
        coordinates = set(self.coordinates)
        
        # Remove any coordinates that have already been geocoded
        located_coordinates = self.located_coordinates['Coordinates'].values
        failed_coordinates = self.failed_coordinates['Coordinates'].values
        for seen_coordinates in [located_coordinates, failed_coordinates]:
            coordinates = coordinates.difference(seen_coordinates)

        # Batch geocoder
        if len(coordinates) > 0:
            located_df, failed_df = batch_geocode(data=coordinates, direction='reverse', n_threads=100, verbose=verbose)

            # Add geocoding results to self.located_coordinates and self.failed_coordinates
            # Raise an error if no coordinates were successfully geocoded
            if located_df.empty and len(self.located_coordinates) == 0:
                raise ValueError('No coordinates were successfully geocoded. Review Geocoder.coordinates data.')
            # If self.located_coordinates is None, set it to located_df
            elif self.located_coordinates is None:
                self.located_coordinates = located_df.copy()
            # If located_df is empty, pass
            elif located_df.empty:
                pass
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
                print('Reverse geocoding complete')
                print(f' - {len(located_df):,} coordinates were located')
                print(f' - {len(failed_df):,} coordinates failed')

            if self.root is not None:
                self.save_data()

        else:
            print('All coordinates have already been geocoded.')

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
        ValueError:
            If no data is provided to instance.
        """

        if data is not None:
            self.add_data(data)

        if self.data is None:
            raise ValueError('No data was provided to Geocoder instance. Data merge failed.'
                             'Please pass data to the merge_data() method.')

        # Merge data
        if 'Address' in self.data.columns and len(self.located_addresses) > 0:
            # Rename 'Date' column to avoid conflict with located_addresses
            temp_located_addresses = self.located_addresses.copy()
            temp_located_addresses.rename(columns={'Date': 'ReverseGeocodeDate'}, inplace=True)

            # Alert user if there are matching column names in self.data and located_addresses
            data_cols = [col for col in self.data.columns if col != 'Address']
            matching_cols = [col for col in data_cols if col in temp_located_addresses.columns]
            if len(matching_cols) > 0:
                print('There are matching columns in the base data and located_addresses:', matching_cols)
                print('Columns from located_addresses will be suffixed with "_Geocoder"')

            # Merge data with located_addresses
            self.data = self.data.merge(temp_located_addresses, how='left', on='Address', suffixes=('', '_Geocoder'))

        if 'Coordinates' in self.data.columns and len(self.located_coordinates) > 0:
            # Rename 'Date' column to avoid conflict with located_addresses
            temp_located_coordinates = self.located_coordinates.copy()
            temp_located_coordinates.rename(columns={'Date': 'ForwardGeocodeDate'}, inplace=True)

            # Alert user if there are matching column names in self.data and located_coordinates
            data_cols = [col for col in self.data.columns if col != 'Coordinates']
            matching_cols = [col for col in data_cols if col in temp_located_coordinates.columns]
            if len(matching_cols) > 0:
                print('There are matching columns in the base data and located_coordinates:', matching_cols)
                print('Columns from located_coordinates will be suffixed with "_Geocoder"')

            # Merge data with located_coordinates
            self.data = self.data.merge(temp_located_coordinates, how='left', on='Coordinates', suffixes=('', '_Geocoder'))

    def process(self, forward=True, reverse=True, merge=True, data=None, verbose=True):
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

        # Geocoding process
        if verbose:
            print('Geocoding Data...')
        if forward:
            self.forward(verbose=verbose)

        if reverse:
            self.reverse(verbose=verbose)

        if verbose:
            print('-' * 50)

        # Merge data
        if merge:
            if verbose:
                print('Merging data...')
            self.merge_data(verbose=verbose)
            if verbose:
                print('-' * 50)

        if verbose:
            print('Processing complete')

        if merge:
            return self.data

    def save_data(self):
        """
        Save geocoding results to CSV files.

        Raises:
        ------
        ValueError:
            If path directory is set to None.
        """
        # Raise an error if path directory is set to None
        if self.root is None:
            raise ValueError('Path directory set to None. Please define Path to save geocoding data.')

        self.located_addresses.to_csv(self.root / 'geocoder/located_addresses.csv', index=False)
        self.failed_addresses.to_csv(self.root / 'geocoder/failed_addresses.csv', index=False)
        self.located_coordinates.to_csv(self.root / 'geocoder/located_coordinates.csv', index=False)
        self.failed_coordinates.to_csv(self.root / 'geocoder/failed_coordinates.csv', index=False)

    def delete_data(self, records='failed', time=365, confirm=True):
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
            If path directory is set to None.
        ValueError
            If time is not an integer or one of 'week', 'month', 'year', or 'all'.
        ValueError
            If records is not 'failed', 'located', or 'all'.
        """
        # Raise an error if path directory is set to None
        if self.root is None:
            raise ValueError('Path directory set to None. Please define Path to save geocoding data.')

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

        # Convert 'Date' column to datetime format
        for key in data_refs.keys():
            data_refs[key]['Date'] = pd.to_datetime(data_refs[key]['Date'], errors='coerce')

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
        if confirm:
            confirmation = input('Would you like to continue? (y/n) ')
        else:
            confirmation = 'y'

        if confirmation == 'y':
            for key in keys:
                setattr(self, key, filtered_data[key])
            self.save_data()
            print('Data deletion complete.')

        else:
            print('Aborting data deletion.')
