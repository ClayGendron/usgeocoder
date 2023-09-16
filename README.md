<h1 align="center">
<img src="https://raw.githubusercontent.com/claygendron/usgeocoder/main/USGeocoder.png" width="1200">
</h1>

# Overview

Thank you for your interest in USGeoCoder package!
USGeoCoder is an easy and free-to-use package for geocoding US addresses with the US Census Geocoder API.
This package was created to solve two problems I encountered while trying to geocode data in my data pipelines:

1. Geocode thousands of addresses in a reasonable amount of time without caps on total requests.
2. Do it for free.

The [US Census Geocoder API](https://geocoding.geo.census.gov/geocoder/) was the best solution I found to meet these requirements.
There are limitations, of course (the main one being that this API only works for US addresses), but by sending requests in parallel, this package can geocode around 2,000 - 4,000 addresses per minute without ever hitting a rate limit or a total request cap.

This package is designed to help anyone, from an individual data scientist or developer working on small projects to a business managing large data pipelines.
If this package helps you, I would love to hear from you! And I would love it even more if you give feedback or contribute to the package 😊

**Note:** This package is in a Beta state, so please be aware that there may be bugs or issues. Thank you for your patience.

# Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)
   - [API Request Functions](#api-request-functions)
   - [Batch Geocoder Function](#batch-geocoder-function)
   - [Geocoder Class](#geocoder-class)
3. [Contribute](#contribute)
4. [License](#license)

# Installation

Make sure you have Python 3 installed, along with the pandas library.

```bash
pip install usgeocoder
```

# Usage

This package consists of three main sets of functions and classes.

- API Request Functions (Forward and Reverse)
- Batch Geocoder Function (Parallelize API Request Functions)
- Geocoder Class (Data Manager for Batch Geocoder)

The components will be detailed below in order.

## API Request Functions

```python
from usgeocoder import geocode_address, geocode_coordinates
```

It is very simple to run a single request to geocode an address or a pair of coordinates.

Addresses should look like this: `123 Main St, City, State Zip`.

Coordinates should look like this: `(Longitude, Latitude)`.

```python
# Forward
address = '123 Main St, City, State Zip'
response = geocode_address(address)

# Reverse
coordinates = (-70.207895, 43.623068)
response = geocode_coordinates(coordinates)
```

**Note:** Notice coordinate pairs are stored as (Longitude, Latitude) or (x, y).
If results are not as expected, try switching the order of the coordinates.
For instance, Google Maps shows points as (Latitude, Longitude) or (y, x).
The order of (Longitude, Latitude) was chosen because it is consistent with the mathematical convention of plotting points on a Cartesian plane, and it is how many GIS systems order coordinate points.

## Batch Geocoder Function

```python
from usgeocoder import batch_geocoder
```

The `batch_geocoder` function will allow you to parallelize the requests in the `geocode_address` and `geocode_coordinates` functions.

```python
# Forward
addresses = ['123 Main St, City, State Zip', '456 Main St, City, State Zip']
located, failed = batch_geocoder(addresses, direction='forward', n_threads=100)

# Reverse
coordinates = [(-70.207895, 43.623068), (-71.469826, 43.014701)]
located, failed = batch_geocoder(coordinates, direction='reverse', n_threads=100)
```

**Note:** The `batch_geocoder` function has been optimized to run at a max of 100 for `n_threads`.
Increasing `n_threads` beyond 100 will increase the likelihood of hitting a rate limit error.

## Geocoder Class

```python
from usgeocoder import Geocoder
```

The `Geocoder` class aims to organize the geocoding process in a data pipeline.
When the `Geocoder` class is initialized, it will create a directory called `geocoder` in the current working directory.
This new directory will store each address or set of coordinates seen by the `Geocoder` class.
If this directory already exists, the `Geocoder` class will instead load in the data from the directory.
A directory is created to avoid making duplicate requests to the API for the same address or set of coordinates, whether the request was successful or not.

### Using the Process Method

The recommended way to use the `Geocoder` class is to initialize it and then use the `process()` method to manage what actions to take in the geocoding process.
The `process()` method will take a dataframe that has a column with complete addresses or sets of coordinates.
This column should be called `Address` or `Coordinates` and be formatted the same as required by the API request functions.
By default, the `process()` method will perform the following:

- Add the data from the pipeline to the `Geocoder` class.
- Forward geocode the addresses.
- Reverse geocode the coordinates from the forward geocoding step.
- Merge the geocoded data back to a copy of the original dataframe.
- Return the merged dataframe.

Here is an example of using the `process()` method.

```python
geo = Geocoder()
geocoded_df = geo.process(data=df)

# or

geo = Geocoder(df)
geocoded_df = geo.process()
```

If you want to customize the geocoding process, you can flip certain steps to `True` or `False` in the `process()` method.
Here is an example of the defaults.

```python
geo.process(
   data=df,
   forward=True,
   reverse=True,
   merge=True,
   verbose=False
)
```

**Note:** The `Geocoder` class was designed assuming that most users will be geocoding addresses.
Therefore, the default behavior is to forward geocode addresses and then reverse geocode the coordinates from the forward geocoding step.
If you are strictly reverse geocoding coordinates, you can set `forward=False` in the `process()` method to skip the forward geocoding step.

### Using Separate Methods

If you want to use the `Geocoder` class to manage the geocoding process but would like to use separate methods for each step, you can do so.
Here is an example of the separate methods utilized in the `process()` method.

```python
geo = Geocoder(df)
geo.forward()
geo.reverse()
geo.merge_data()
geocoded_df = geo.data
```

**Note:** When adding data to the `Geocoder` class, it is designed to add the `Address` or `Coordinates` as an un-duplicated list to its `addresses` and `coordinates` attributes.
When the `forward()` or `reverse()` methods are called, they look to these attributes for the data to geocode.
If you add a dataframe with both `Address` and `Coordinates` columns, the `Geocoder` class will only populate the `coordinates` attribute as there is no need to forward geocode the addresses.
If the `forward()` method is called, it will raise an error.

### Using Helper Functions

If you have a dataframe with separate columns for `Street Address`, `City`, `State`, and `Zip`, and named accordingly, you can use a helper function to create a new `Address` column, or create the column yourself.
The below example illustrates a simple step to rename the `existing_cols` to the required column names.

```python
# Create a new column with complete address using helper function
from usgeocoder import concatenate_address
existing_cols = ['address 1', 'address 2', 'city', 'state', 'zip code', 'important feature']
df = pd.DataFrame(columns=existing_cols)
df.rename(columns={
   'address 1': 'Street Address', 
   'city': 'City', 
   'state': 'State', 
   'zip code': 'Zip'
}, inplace=True)

df['Address'] = concatenate_address(df)
```

These steps work just the same for reverse geocoding to create a new `Coordinates` column with separate `Longitude` and `Latitude` columns.

```python
# Create a new column with complete coordinates using helper function
from usgeocoder import concatenate_coordinates
existing_cols = ['x', 'y', 'important feature']
df = pd.DataFrame(columns=existing_cols)
df.rename(columns={
   'x': 'Longitude', 
   'y': 'Latitude'
}, inplace=True)

df['Coordinates'] = concatenate_coordinates(df)
```

# Contribute

If you would like to make this package better, please consider contributing 😊

# License

[MIT](https://choosealicense.com/licenses/mit/)
