# USGeoCoder
USGeoCoder is an easy and free-to-use package for geocoding US addresses with the US Census Geocoder API.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Usage](#usage)
   - [Geocoder Class](#geocoder-class)
   - [Batch Geocoder Function](#batch-geocoder-function)
   - [API Request Functions](#api-request-functions)
4. [Contribute](#contribute)
5. [License](#license)

## Overview

Thank you for your interest in USGeoCoder package!
This package was created to solve two problems I encountered while trying to geocode data in my data pipelines:

1. Geocode thousands of addresses in a reasonable amount of time without caps on total requests.
2. Do it for free.

The [US Census Geocoder API](https://geocoding.geo.census.gov/geocoder/) was the best solution I found to meet these requirements.
There are limitations, of course (the main one being that this API only works for US addresses), but by sending requests in parallel, this package can geocode around 2,000 - 4,000 addresses per minute without ever hitting a rate limit or a total request cap.

This package is designed to help anyone, from an individual data scientist or developer working on small projects to a business managing large data pipelines.
If this package helps you, I would love to hear from you! And I would love it even more if you give feedback or contribute to the package 😊

**Note:** This package is in a Beta state, so please be aware that there may be bugs or issues. Thank you for your patience.

## Installation

Make sure you have Python 3 installed along with the pandas library.

```bash
pip install usgeocoder
```

## Usage

This package consists of three main sets of functions and classes.

- Geocoder Class (Data Manager for Batch Geocoder)
- Batch Geocoder Function (Parallelize API Request Functions)
- API Request Functions (Forward and Reverse)

The components will be detailed below in order.

### Geocoder Class

```python
from usgeocoder import Geocoder
```

The goal of the `Geocoder` class is to organize the geocoding process in a data pipeline.
When the `Geocoder` class is initialized, it will create a directory called `geocoder` in the current working directory.
This new directory will store each address or set of coordinates seen by the `Geocoder` class.
If this directory already exists, the `Geocoder` class will instead load in the data from the directory.

It is recommended to initialize the `Geocoder` class like so:

```python
geo = Geocoder()
```

Once the `Geocoder` class is initialized, there is a small amount of data setup required.

The recommended way to integrate the `Geocoder` class into your data pipeline is to add a column to your dataframe that has the full address or set of coordinates.
This column should be called `Address` or `Coordinates`, respectively.
This is recommended as it is the easiest way of joining the geocoded data back into your original dataframe.

If you have a dataframe with separate columns for street address, city, state, and zip, you can use a helper function to create a new `Address` column or create the column yourself.

To begin the geocoding process, you must add data to the `Geocoder` class.

```python
# Add a dataframe with address parts
# Create a new column with full address with helper function
from usgeocoder import concatenate_address
df = pd.DataFrame(columns=['address 1', 'address 2', 'city', 'state', 'zip code', 'important feature'])
df.rename(columns={'address 1': 'Street Address', 'address 2': 'City', 'city': 'State', 'state': 'Zip'}, inplace=True)
df['Address'] = concatenate_address(df)
geo.add_addresses(df['Address'])

# Rename column with full address to 'Address'
df = pd.DataFrame(columns=['full address', 'address 1', 'address 2', 'city', 'state', 'zip code', 'important feature'])
df.rename(columns={'full address': 'Address'}, inplace=True)
geo.add_addresses(df['Address'])
```

If you have a list or series of full addresses, you can also easily add those to your `Geocoder` class.
Addresses should look like this: `123 Main St, City, State Zip`.

```python
# Add a list or pd.Series of addresses
addresses = ['123 Main St, City, State Zip', '456 Main St, City, State Zip']
geo.add_addresses(addresses)
```

These steps work just the same for reverse geocoding with coordinates.
Coordinates should look like this: `(-70.207895, 43.623068)`.

```python
# Add a dataframe with coordinate parts
# Create a new column with full coordinates with helper function
from usgeocoder import concatenate_coordinates
df = pd.DataFrame(columns=['x', 'y', 'important feature'])
df.rename(columns={'x': 'Longitude', 'y': 'Latitude'}, inplace=True)
df['Coordinates'] = concatenate_coordinates(df)
geo.add_coordinates(df['Coordinates'])

# Rename column with full coordinates to 'Coordinates'
df = pd.DataFrame(columns=['xy', 'x', 'y', 'important feature'])
df.rename(columns={'xy': 'Coordinates'}, inplace=True)
geo.add_coordinates(df['Coordinates'])

# Add a list or pd.Series of coordinates
coordinates = [(-70.207895, 43.623068), (-71.469826, 43.014701)]
geo.add_coordinates(coordinates)
```

Once the data has been added, you can instruct the `Geocoder` class to geocode the data with the `forward()` and `reverse()` methods.
Forward geocoding will take addresses and return coordinates.
Reverse geocoding will take coordinates and return address parts.

```python
geo.forward()
geo.reverse()
```

If you used the helper function to create a new `Address` or `Coordinates` column, you can now simply merge the geocoded data back into your original dataframe.

```python
# Merge addresses
df_merged = df.merge(geo.located_addresses, how='left', on='Address')

# Merge coordinates
df_merged = df.merge(geo.located_coordinates, how='left', on='Coordinates')
```

### Batch Geocoder Function

```python
from usgeocoder import batch_geocoder
```

When running `geo.forward()` or `geo.reverse()`, the method calls the `batch_geocoder` function under the hood.
If you want to run the geocoder on its own, you can do that like so:

```python
# Forward
addresses = ['123 Main St, City, State Zip', '456 Main St, City, State Zip']
located_addresses, failed_addresses = batch_geocoder(addresses, direction='forward', n_threads=100)

# Reverse
coordinates = [(-70.207895, 43.623068), (-71.469826, 43.014701)]
located_coordinates, failed_coordinates = batch_geocoder(coordinates, direction='reverse', n_threads=100)
```

Tip: The `batch_geocoder` function has been optimized to run at a max of 100 for `n_threads`.
Increasing `n_threads` beyond 100 will increase the likelihood of hitting a rate limit error.

### API Request Functions

```python
from usgeocoder import geocode_address, geocode_coordinates
```

If your preference is to run the API request on a single address or set of coordinates, you can do that like so:

```python
# Forward
address = '123 Main St, City, State Zip'
response = geocode_address(address)

# Reverse
coordinates = (-70.207895, 43.623068)
response = geocode_coordinates(coordinates)
```

Tip: Coordinate pairs are stored as (Longitude, Latitude) or (x, y).
If results are not as expected, try switching the order of the coordinates.

## Contribute

If you would like to make this package better, please consider contributing 😊

## License

[MIT](https://choosealicense.com/licenses/mit/)
