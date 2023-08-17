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
    
# create a new column of latitude and longitude coordinates
def apply_concatenate_coordinates(row):
    try:
        # strip any whitespace and convert to decimal number
        coordinates = (float(str(row['Latitude']).strip()), float(str(row['Longitude']).strip()))
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
    coordinate_parts_cols = ['Latitude', 'Longitude']
    coordinates_col = ['Coordinates']
    
    if len(df.columns) == 2:
        df.columns = coordinate_parts_cols
        df.dropna(subset=['Latitude', 'Longitude'], how='any', inplace=True)
        coordinates = df.apply(apply_concatenate_coordinates, axis = 1)
        
    elif not set(coordinate_parts_cols).issubset(set(df.columns)) and not set(coordinates_col).issubset(set(df.columns)):
        raise Exception('The dataframe must have the following columns: [Latitude, Longitude] or [Coordinates]')
    
    elif set(coordinates_col).issubset(set(df.columns)):
        df.dropna(subset=['Coordinates'], how='any', inplace=True)
        coordinates = df['Coordinates']
        
    elif set(coordinate_parts_cols).issubset(set(df.columns)):
        df.dropna(subset=['Latitude', 'Longitude'], how='any', inplace=True)
        coordinates = df.apply(apply_concatenate_coordinates, axis = 1)
        
    coordinates_list = coordinates.drop_duplicates().tolist()
    
    if len(coordinates_list) == 0:
        raise Exception('No coordinates were found in the dataframe. Please check the column names and try again.')
    
    return coordinates_list