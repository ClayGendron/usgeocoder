import pandas as pd

# create a new column of the full address
def apply_concatenate_address(row):
    try:
        address_parts = [
            str(row['Address 1']).strip() if pd.notna(row['Address 1']) else '',
            ' ' + str(row['Address 2']).strip() if pd.notna(row['Address 2']) else '',
            ', ' + str(row['City']).strip() if pd.notna(row['City']) else '',
            ', ' + str(row['State']).strip() if pd.notna(row['State']) else '',
            ' ' + str(row['ZIP']).strip()[0:5] if pd.notna(row['ZIP']) and len(str(row['ZIP']).strip()) >= 5  else ''
        ]
        # remove any empty strings and join all parts
        address_parts = [part for part in address_parts if part.strip()]
        address = ''.join(address_parts)
        
        return address
    
    except Exception as e:
        # Log the error and the row for debugging purposes
        print(f'Error processing row: {row}. Error: {e}')
        return None
    
# create a new column of longitude and latitude coordinates
def apply_concatenate_coordinates(row):
    try:
        # strip any whitespace and convert to decimal number
        coordinates = (float(str(row['Longitude']).strip()), float(str(row['Latitude']).strip()))
        return coordinates
    
    except Exception as e:
        # Log the error and the row for debugging purposes
        print(f'Error processing row: {row}. Error: {e}')
        return None

# create a list of addresses from a dataframe
def create_address_list(df):
    address_parts_cols = ['Address 1', 'Address 2', 'City', 'State', 'ZIP']
    address_col = ['Address']
    
    if len(df.columns) == 5:
        df.columns = address_parts_cols
        addresses = df.apply(apply_concatenate_address, axis = 1)
        
    elif not set(address_parts_cols).issubset(set(df.columns)) and not set(address_col).issubset(set(df.columns)):
        raise Exception('The dataframe must have the following columns: [Address 1, Address 2, City, State, ZIP] or [Address]')
    
    elif set(address_col).issubset(set(df.columns)):
        addresses = df['Address']
    
    elif set(address_parts_cols).issubset(set(df.columns)):
        addresses = df.apply(apply_concatenate_address, axis = 1)
    
    addresses_list = addresses.drop_duplicates().tolist()
    addresses_list = [address for address in addresses_list if address]
    
    if len(addresses_list) == 0:
        raise Exception('No addresses were found in the dataframe. Please check the column names and try again.')
    
    return addresses_list

def create_coordinates_list(df):
    coordinate_parts_cols = ['Longitude', 'Latitude']
    coordinates_col = 'Coordinates'
    
    # Ensure columns exist
    if not (set(coordinate_parts_cols).issubset(df.columns) or coordinates_col in df.columns):
        raise Exception('The dataframe must have the following columns: [Longitude, Latitude] or [Coordinates]')
    
    # Handle case where there's only 'Coordinates' column
    if coordinates_col in df.columns:
        df = df.dropna(subset=[coordinates_col])
        coordinates = df[coordinates_col]

    # Handle case where there are 'Longitude' and 'Latitude' columns
    elif set(coordinate_parts_cols).issubset(df.columns):
        df = df.dropna(subset=coordinate_parts_cols)
        coordinates = df.apply(apply_concatenate_coordinates, axis=1)
    else:
        # This should not be reached based on previous checks, but included for clarity.
        raise Exception('Unexpected columns in dataframe.')
        
    coordinates_list = coordinates.drop_duplicates().tolist()
    
    if len(coordinates_list) == 0:
        raise Exception('No coordinates were found in the dataframe. Please check the column names and try again.')
    
    return coordinates_list
