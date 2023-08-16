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

# create a list of addresses from a dataframe
def create_address_list(df):
    address_parts_cols = ['Address 1', 'Address 2', 'City', 'State', 'ZIP']
    address_col = ['Address']
    
    if len(df.columns) == 5:
        df.columns = address_parts_cols
    elif not set(address_parts_cols).issubset(set(df.columns)) and not set(address_col).issubset(set(df.columns)):
        raise Exception('The dataframe must have the following columns: [Address 1, Address 2, City, State, ZIP] or [Address]')
    elif set(address_col).issubset(set(df.columns)):
        addresses = df['Address']
    elif set(address_parts_cols).issubset(set(df.columns)):
        addresses = df.apply(apply_concatenate_address, axis = 1)
    
    addresses_list = addresses.drop_duplicates().tolist()
    addresses_list = [address for address in addresses_list if address.strip()] # remove any empty strings
    
    if len(addresses_list) == 0:
        raise Exception('No addresses were found in the dataframe. Please check the column names and try again.')
    
    return addresses_list