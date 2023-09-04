import pandas as pd


def concatenate_address(df):
    """
    Function to concatenate address columns.

    Parameters:
    - df (DataFrame): DataFrame containing the columns 'Street Address', 'City', 'State', and 'ZIP'.

    Returns:
    - Series: A Pandas Series of concatenated addresses.
    """

    address_parts = [
        df['Street Address'].fillna(''),
        ', ' + df['City'].fillna(''),
        ', ' + df['State'].fillna(''),
        ' ' + df['ZIP'].str[:5].fillna('')
    ]
    return ''.join(address_parts).str.strip()


def concatenate_coordinates(df):
    """
    Function to create coordinate tuples.

    Parameters:
    - df (DataFrame): DataFrame containing the columns 'Longitude' and 'Latitude'.

    Returns:
    - Series: A Pandas Series of (Longitude, Latitude) coordinate tuples.
    """

    coordinates = list(zip(df['Longitude'], df['Latitude']))
    return pd.Series(coordinates, index=df.index)


def create_address_list(df):
    """
    Extract a list of unique addresses from a DataFrame.

    The DataFrame should either contain a single 'Address' column or four separate columns
    ['Street Address', 'City', 'State', 'ZIP'] for the function to extract and concatenate the addresses.

    Parameters:
    - df (DataFrame): DataFrame with either 'Address' column or ['Street Address', 'City', 'State', and 'ZIP'] columns.

    Returns:
    - list: List of unique addresses. Empty addresses are removed from the list.

    Raises:
    - Exception: If the DataFrame does not have the required columns.
    - Exception: If no addresses are found after processing.
    """

    address_parts_cols = ['Street Address', 'City', 'State', 'ZIP']
    address_col = ['Address']

    # Ensure columns exist
    if not set(address_parts_cols).issubset(set(df.columns)) and not set(address_col).issubset(set(df.columns)):
        raise Exception('The dataframe must have the following columns:'
                        "['Street Address', 'City', 'State', 'ZIP'] or 'Address'")

    # Handle case where there's only 'Address' column
    elif set(address_col).issubset(set(df.columns)):
        addresses = df['Address']

    # Handle case where there are 'Street Address', 'City', 'State', and 'ZIP' columns
    elif set(address_parts_cols).issubset(set(df.columns)):
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

    The DataFrame should either contain a single 'Coordinates' column (with tuple format) or two separate columns
    ['Longitude', 'Latitude'] for the function to extract and pair the coordinates.

    Parameters:
    - df (DataFrame): DataFrame with either 'Coordinates' column or ['Longitude', 'Latitude'] columns.

    Returns:
    - list: List of unique (Longitude, Latitude) coordinates.

    Raises:
    - Exception: If the DataFrame does not have the required columns.
    - Exception: If no coordinates are found after processing.
    """

    coordinate_parts_cols = ['Longitude', 'Latitude']
    coordinates_col = ['Coordinates']

    # Ensure columns exist
    if not (set(coordinate_parts_cols).issubset(df.columns) and not set(coordinates_col).issubset(df.columns)):
        raise Exception('The dataframe must have the following columns:'
                        "['Longitude', 'Latitude'] or 'Coordinates'")

    # Handle case where there's only 'Coordinates' column
    if coordinates_col in df.columns:
        df = df.dropna(subset=[coordinates_col])
        coordinates = df[coordinates_col]

    # Handle case where there are 'Longitude' and 'Latitude' columns
    elif set(coordinate_parts_cols).issubset(df.columns):
        df = df.dropna(subset=coordinate_parts_cols)
        coordinates = concatenate_coordinates(df)

    # This should not be reached based on previous checks, but included for clarity.
    else:
        raise Exception('Unexpected columns in dataframe.')

    coordinates_list = coordinates.drop_duplicates().tolist()

    if len(coordinates_list) == 0:
        raise Exception('No coordinates were found in the dataframe. Please check the column names and try again.')

    return coordinates_list
