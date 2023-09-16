import unittest
import pandas as pd
import os
from pathlib import Path
import shutil

from usgeocoder import Geocoder, concatenate_address

# Get root of test directory
ROOT = Path(os.getcwd())


class TestGeocoder(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.state_capitals = pd.read_csv(ROOT / 'state_capitals.csv')

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

    def test_geocoding(self):
        self.state_capitals['Address'] = concatenate_address(self.state_capitals)
        self.geo.add_data(self.state_capitals)
        test = self.geo.process(verbose=True)
        self.assertTrue(len(self.geo.located_addresses) > 0)
        self.assertTrue(len(self.geo.located_coordinates) > 0)
        self.assertTrue('Address' in test.columns)
        self.assertTrue('Coordinates' in test.columns)


if __name__ == '__main__':
    unittest.main()
