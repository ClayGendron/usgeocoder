import pandas as pd


def concatenate_address(df):
    """
    Create a series of concatenated address components into a single formatted address string.

    Parameters:
    ----------
    df : pd.DataFrame
        A DataFrame containing the columns 'Street Address', 'City', 'State',
        and 'ZIP'.

    Returns:
    -------
    pd.Series
        A Pandas Series containing the concatenated addresses, where each entry
        corresponds to a row in the input DataFrame.
    """

    df = df.copy()
    df['Street Address'] = df['Street Address'].fillna('').astype(str)
    df['City'] = df['City'].fillna('').astype(str)
    df['State'] = df['State'].fillna('').astype(str)
    df['ZIP'] = df['ZIP'].fillna('').astype(str)

    # Pad ZIP codes with leading zeros and then slice to take the first 5 characters
    df['ZIP'] = df['ZIP'].str.zfill(5).str[0:5]

    # Initialize address with 'Street Address'
    address = df['Street Address']

    # If other address parts are not empty, concatenate with separator
    address += df['City'].where(df['City'] == '', ', ' + df['City'])
    address += df['State'].where(df['State'] == '', ', ' + df['State'])
    address += df['ZIP'].where(df['ZIP'] == '', ' ' + df['ZIP'])

    return address.str.strip()


def concatenate_coordinates(df):
    """
    Create a series of (Longitude, Latitude) coordinate tuples from a DataFrame.

    Parameters:
    ----------
    df : pd.DataFrame
        A DataFrame containing the columns 'Longitude' and 'Latitude'.

    Returns:
    -------
    pd.Series
        A Pandas Series containing the (Longitude, Latitude) tuples, where each
        entry corresponds to a row in the input DataFrame.
    """

    coordinates = list(zip(df['Longitude'], df['Latitude']))
    return pd.Series(coordinates, index=df.index)


def create_address_list(df):
    """
    Extract a list of unique addresses from a DataFrame.

    The DataFrame should either contain a single 'Address' column or four separate columns
    ['Street Address', 'City', 'State', 'ZIP'] for the function to extract and concatenate the addresses.

    Parameters:
    ----------
    df : pd.DataFrame
        DataFrame with either 'Address' column or ['Street Address', 'City', 'State', and 'ZIP'] columns.

    Returns:
    -------
    list
        List of unique addresses. Empty addresses are removed from the list.

    Raises:
    ------
    Exception
        If the DataFrame does not have the required columns.
    Exception
        If no addresses are found after processing.
    """

    address_parts_cols = ['Street Address', 'City', 'State', 'ZIP']
    address_col = ['Address']

    # Ensure columns exist
    if not set(address_parts_cols).issubset(set(df.columns)) and not set(address_col).issubset(set(df.columns)):
        raise Exception('The dataframe must have the following columns:'
                        "['Street Address', 'City', 'State', 'ZIP'] or 'Address'")

    # Handle case where there's only 'Address' column
    elif set(address_col).issubset(set(df.columns)):
        df = df.dropna(subset=['Address'])
        addresses = df['Address']

    # Handle case where there are 'Street Address', 'City', 'State', and 'ZIP' columns
    elif set(address_parts_cols).issubset(set(df.columns)):
        df = df.dropna(subset=address_parts_cols)
        addresses = concatenate_address(df)

    # This should not be reached based on previous checks, but included for clarity.
    else:
        raise Exception('Unexpected columns in dataframe.')

    addresses_list = addresses.drop_duplicates().tolist()
    addresses_list = [address for address in addresses_list if address]

    if len(addresses_list) == 0:
        raise Exception('No addresses were found in the dataframe. Please check the column names and try again.')

    return addresses_list


def create_coordinates_list(df):
    """
    Extract a list of unique coordinates from a DataFrame.

    Given a DataFrame with either a single 'Coordinates' column (in tuple format) or separate 'Longitude'
    and 'Latitude' columns, this function pairs and extracts unique coordinates.

    Parameters:
    ----------
    df : pd.DataFrame
        A DataFrame with either a 'Coordinates' column or ['Longitude', 'Latitude'] columns.

    Returns:
    -------
    list
        List of unique (Longitude, Latitude) coordinates.

    Raises:
    ------
    Exception
        If the DataFrame does not have the required columns.
    Exception
        If no coordinates are found after processing.
    """

    coordinate_parts_cols = ['Longitude', 'Latitude']
    coordinates_col = ['Coordinates']

    # Ensure columns exist
    if not set(coordinate_parts_cols).issubset(df.columns) and not set(coordinates_col).issubset(df.columns):
        raise Exception('The dataframe must have the following columns:'
                        "['Longitude', 'Latitude'] or 'Coordinates'")

    # Handle case where there's only 'Coordinates' column
    if set(coordinates_col).issubset(set(df.columns)):
        df = df.dropna(subset=['Coordinates'])
        coordinates = df['Coordinates']

    # Handle case where there are 'Longitude' and 'Latitude' columns
    elif set(coordinate_parts_cols).issubset(set(df.columns)):
        df = df.dropna(subset=coordinate_parts_cols)
        coordinates = concatenate_coordinates(df)

    # This should not be reached based on previous checks, but included for clarity.
    else:
        raise Exception('Unexpected columns in dataframe.')

    coordinates_list = coordinates.drop_duplicates().tolist()

    if len(coordinates_list) == 0:
        raise Exception('No coordinates were found in the dataframe. Please check the column names and try again.')

    return coordinates_list
