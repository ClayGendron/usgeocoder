import unittest
import pandas as pd
import os
from pathlib import Path
import shutil

from usgeocoder import Geocoder, concatenate_address, concatenate_coordinates

# Get root of test directory
ROOT = Path(os.getcwd())

class TestGeocoder(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.state_capitals = pd.read_csv(ROOT / 'state_capitals.csv')
        cls.coordinates = pd.DataFrame({
            'Longitude': [-77.0365, -80.8431],
            'Latitude': [38.8951, 35.2271]
        })

    def setUp(self):
        self.geo = Geocoder()

    def tearDown(self):
        if os.path.exists(ROOT / 'geocoder'):
            shutil.rmtree(ROOT / 'geocoder')

    def test_directory_creation(self):
        self.assertTrue(os.path.exists(ROOT / 'geocoder'))

    def test_add_addresses(self):
        self.geo.add_addresses(self.state_capitals)
        self.assertEqual(len(self.geo.addresses), 56)

    def test_add_coordinates(self):
        self.geo.add_coordinates(self.coordinates)
        self.assertEqual(len(self.geo.coordinates), 2)

    def test_forward_geocoding(self):
        self.state_capitals['Address'] = concatenate_address(self.state_capitals)
        self.geo.add_data(self.state_capitals)
        test = self.geo.process(forward=True, reverse=False, verbose=False)
        self.assertTrue(len(self.geo.located_addresses) > 0)
        self.assertTrue('Address' in test.columns)
        self.assertTrue('Coordinates' in test.columns)

    def test_reverse_geocoding(self):
        self.coordinates['Coordinates'] = concatenate_coordinates(self.coordinates)
        self.geo.add_data(self.coordinates)
        test = self.geo.process(forward=False, reverse=True, verbose=False)
        self.assertTrue(len(self.geo.located_coordinates) > 0)
        self.assertTrue('Coordinates' in test.columns)
        self.assertTrue('State' in test.columns)

    def test_forward_and_reverse_geocoding(self):
        self.state_capitals['Address'] = concatenate_address(self.state_capitals)
        self.geo.add_data(self.state_capitals)
        test = self.geo.process(verbose=False)
        self.assertTrue(len(self.geo.located_addresses) > 0)
        self.assertTrue(len(self.geo.located_coordinates) > 0)
        self.assertTrue('Address' in test.columns)
        self.assertTrue('Coordinates' in test.columns)

    def test_merge_data(self):
        self.state_capitals['Address'] = concatenate_address(self.state_capitals)
        self.geo.add_data(self.state_capitals)
        self.geo.process(forward=True, reverse=True, merge=False, verbose=False)
        merged_data = self.geo.process(merge=True, verbose=False)
        self.assertTrue('Address' in merged_data.columns)
        self.assertTrue('Coordinates' in merged_data.columns)

    def test_no_addresses_to_geocode(self):
        with self.assertRaises(Exception) as context:
            self.geo.add_addresses(self.state_capitals.head(0))  # Empty DataFrame
        self.assertTrue('No addresses were found in the dataframe' in str(context.exception))

    def test_no_coordinates_to_geocode(self):
        with self.assertRaises(Exception) as context:
            self.geo.add_coordinates(self.coordinates.head(0))  # Empty DataFrame
        self.assertTrue('No coordinates were found in the dataframe' in str(context.exception))

    def test_delete_data(self):
        self.state_capitals['Address'] = concatenate_address(self.state_capitals)
        self.geo.add_data(self.state_capitals)
        self.geo.process(forward=True, reverse=False, verbose=False)
        initial_located_addresses = len(self.geo.located_addresses)
        self.geo.delete_data(records='located', time=0, confirm=False)
        self.assertLess(len(self.geo.located_addresses), initial_located_addresses)

    def test_handle_missing_columns(self):
        with self.assertRaises(Exception):
            self.geo.add_addresses(pd.DataFrame({'City': ['City1']}))

if __name__ == '__main__':
    unittest.main()