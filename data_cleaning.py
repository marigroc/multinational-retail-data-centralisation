import pandas as pd
import re

class DataCleaning:
    def __init__(self):
        pass
    """
    This class provides methods for cleaning data, handling NULL values, date errors, and incorrect data types in datasets.
    It can be used to clean both user data and card data.

    Methods:
    1. clean_user_data(user_data)
        - Cleans user data by dropping rows with NULL values, handling date errors, and filtering incorrect rows.
        - Parameters:
            - user_data (DataFrame): The user data to be cleaned.
        - Returns:
            - DataFrame: The cleaned user data.

    2. clean_card_data(card_data)
        - Cleans card data by handling NULL values, date errors, and formatting errors in card numbers.
        - Parameters:
            - card_data (DataFrame): The card data to be cleaned.
        - Returns:
            - DataFrame: The cleaned card data.
    """
    def clean_user_data(self, user_data):
        
        # Drop rows with NULL values
        user_data = user_data.dropna()

        # Handle date formats
        date_columns = ['date_of_birth', 'join_date']  
        for column in date_columns:
            user_data[column] = pd.to_datetime(user_data[column], errors='coerce').dt.date

        # Handle countries
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        user_data = user_data[user_data['country'].isin(valid_countries)]
        # Display the filtered DataFrame
        # Mapping of misspelled country codes to correct codes 
        corrections = {
            'GGB': 'GB',
            'US': 'USA',
            'GER': 'DE'
        }

        # Correct the country_code values
        user_data['country_code'] = user_data['country_code'].map(corrections).fillna(user_data['country_code'])

        # print(user_data)
        # Define a function to standardize phone numbers
        def standardize_phone_number(row):
            phone = row['phone_number']

            # Remove the two-digit country code if it is present
            if row['country'] in ['Germany', 'United Kingdom', 'United States']:
                phone = re.sub(fr'^\+44|^\+49|^{row["country_code"]}', '', phone)
            
            # Remove non-digit characters from the phone number
            phone = re.sub(r'\D', '', phone)

            # Add a leading '0' if the number doesn't start with '0'
            if not phone.startswith('0'):
                phone = '0' + phone

            return phone

        # Apply the standardize_phone_number function to the 'phone_number' column
        user_data['phone_number'] = user_data.apply(standardize_phone_number, axis=1)

        # Drop duplicates based on specific columns
        columns_to_check = ['first_name', 'last_name', 'company', 'email_address', 'user_uuid']
        user_data = user_data.drop_duplicates(subset=columns_to_check)

        # Fix the email address formatting
        def correct_email_format(email):
            return ''.join(e for e in email if e.isalnum() or e in ['.', '@', '-', '_'])

        # Apply the function to the 'email_address' column
        user_data['email_address'] = user_data['email_address'].apply(correct_email_format)

        # Return the cleaned data
        # print(user_data) # For debugging
        return user_data

    def clean_card_data(self, card_data):
        # Handle NULL values
        card_data = card_data.dropna()

        # Drop duplicates based on specific columns
        columns_to_check = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        card_data = card_data.drop_duplicates(subset=columns_to_check)

        # Assuming 'expiry_date' is in 'MM/YY' format and 'date_payment_confirmed' is in 'YYYY-MM-DD' format
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date

        # Optionally, handle NULL values in 'card_number' by filling them with a placeholder
        card_data['card_number'].fillna('Unknown', inplace=True)

        # Return the cleaned card data
        return card_data
    
    def clean_store_data(self, stores_data):
        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_store_data = stores_data.copy()

        # Drop rows where all values are NaN.
        cleaned_store_data.dropna(how='all', inplace=True)

        # Convert 'opening_date' to datetime and replace invalid values with NaN.
        cleaned_store_data['opening_date'] = pd.to_datetime(cleaned_store_data['opening_date'], infer_datetime_format=True, errors='coerce').dt.date

        # Define a regular expression pattern to match rows with random letters and numbers.
        pattern = r'^[a-zA-Z0-9]*$'

        # Create a boolean mask for missing or random values in the 'opening_date' column.
        mask = (cleaned_store_data['opening_date'].isna()) | (cleaned_store_data['opening_date'].astype(str).str.contains(pattern))

        # Filter out rows with missing or random values in the 'opening_date' column.
        cleaned_store_data = cleaned_store_data[~mask]

        # Remove duplicate rows based on all columns.
        cleaned_store_data = cleaned_store_data.drop_duplicates()

        # Create a mapping for correcting continent names with common typos.
        continent_mapping = {
            "eeEurope": "Europe",
            "eeAmerica": "America"
        }

        # Replace misstyped continent names with their correct names.
        cleaned_store_data['continent'] = cleaned_store_data['continent'].replace(continent_mapping)
        return cleaned_store_data
    
    def convert_product_weights(self, product_df):
        decimal_of_kg = []
        for index, row in product_df.iterrows():
            weight = str(row['weight']).strip('.')
            weight = str(weight).strip('l')
            weight = str(weight).strip()  # remove any leading/trailing spaces
            if weight.endswith('kg'):
                decimal_of_kg.append(float(weight[:-2]))
            elif 'x' in weight:
                num, unit = weight.split('x')
                decimal_of_kg.append(float(num) * float(unit.rstrip('g')) / 1000)
            elif weight.endswith('g'):
                decimal_of_kg.append(float(weight[:-1]) / 1000)
            elif weight.endswith('oz'):
                decimal_of_kg.append(float(weight[:-2]) * 0.0283)
            elif weight.endswith('m'):
                decimal_of_kg.append(float(weight[:-1]) / 1000)
            else:
                decimal_of_kg.append(float(weight))
        

        product_df.loc[:, 'weight'] = decimal_of_kg

        return product_df
    
    def clean_product_data(self, product_df):

        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_product_data = product_df.copy()

        # Define a regular expression pattern to match rows with random letters and numbers.
        pattern = r'^[a-zA-Z0-9]*$'

        # Create a boolean mask for missing or random values in the 'weight' column.
        mask = (cleaned_product_data['weight'].isna()) | (cleaned_product_data['weight'].astype(str).str.contains(pattern))

        # Filter out rows with missing or random values in the 'weight' column.
        cleaned_product_data = cleaned_product_data[~mask]

        # Remove duplicate rows based on all columns.
        cleaned_product_data = cleaned_product_data.drop_duplicates()

        # Apply the weight conversion and classification
        cleaned_product_data = self.convert_product_weights(cleaned_product_data)

        return cleaned_product_data
    
    def clean_orders_data(self, orders_data):

        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_orders_data = orders_data.copy()

        # Remove specific columns '1', 'first_name' and 'last_name'
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_data = cleaned_orders_data.drop(columns=columns_to_remove)

        return cleaned_orders_data
    
    def clean_json(self, json_data):

        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_json_data = json_data.copy()
        cleaned_json_data.dropna(how='all', inplace=True)

        # Convert the 'day', 'month', and 'year' columns to numeric (integer) type and handle any conversion errors by setting them to NaN
        cleaned_json_data['month'] = pd.to_numeric(cleaned_json_data['month'], errors='coerce')
        cleaned_json_data['day'] = pd.to_numeric(cleaned_json_data['day'], errors='coerce')
        cleaned_json_data['year'] = pd.to_numeric(cleaned_json_data['year'], errors='coerce')
        random_symbols = r'^[a-zA-Z0-9]*$'

        # Create a boolean mask for 'month' values that are not in the range [1, 12], 'day' not in [1, 31], and year not in [1992,2022]
        year_mask = ~cleaned_json_data['year'].between(1992, 2022)
        month_mask = ~cleaned_json_data['month'].between(1, 12)
        day_mask = ~cleaned_json_data['day'].between(1, 31)  
        timest_mask = (cleaned_json_data['timestamp'].isna()) | (cleaned_json_data['timestamp'].astype(str).str.contains(random_symbols))

        # Combine the masks for both 'month' and 'day' columns using a logical OR
        combined_mask = year_mask | month_mask | day_mask | timest_mask

        # Filter out rows where 'month' values are not in the range [1, 12]
        cleaned_json_data = cleaned_json_data[~combined_mask]

        # Filter 'time_period' column to keep only values in the specified list
        valid_time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        cleaned_json_data = cleaned_json_data[cleaned_json_data['time_period'].isin(valid_time_periods)]

        return cleaned_json_data
        
        
