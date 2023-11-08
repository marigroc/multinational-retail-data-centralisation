import pandas as pd
import re

class DataCleaning:
    def __init__(self):
        pass
    """
    This class provides methods for cleaning data, handling NULL values, date errors, and incorrect data types in datasets.
    It can be used to clean user data, card data, store data, product data, orders data, and JSON data.

    Public Methods for User Data:
    1. clean_user_data(user_data)
        - Cleans user data by dropping rows with NULL values, handling date errors, and filtering incorrect rows.
        - Parameters:
            - user_data (DataFrame): The user data to be cleaned.
        - Returns:
            - DataFrame: The cleaned user data.

    Public Methods for Card Data:
    2. clean_card_data(card_data)
        - Cleans card data by handling NULL values, date errors, and formatting errors in card numbers.
        - Parameters:
            - card_data (DataFrame): The card data to be cleaned.
        - Returns:
            - DataFrame: The cleaned card data.
    
    Public Methods for Store Data:
    3. clean_store_data(stores_data)
        - Cleans store data by converting date columns, removing invalid dates, duplicates, and correcting continent names.
        - Parameters:
            - stores_data (DataFrame): The store data to be cleaned.
        - Returns:
            - DataFrame: The cleaned store data.

    Public Methods for Product Data:
    4. clean_product_data(product_df)
        - Cleans product data by removing rows with missing or random values in the 'weight' column and converting product weights.
        - Parameters:
            - product_df (DataFrame): The product data to be cleaned.
        - Returns:
            - DataFrame: The cleaned product data.

    Public Methods for Orders Data:
    5. clean_orders_data(orders_data)
        - Cleans orders data by removing specific columns.
        - Parameters:
            - orders_data (DataFrame): The orders data to be cleaned.
        - Returns:
            - DataFrame: The cleaned orders data.

    Public Methods for JSON Data:
    6. clean_json(json_data)
        - Cleans JSON data by converting date columns, removing rows with invalid dates, and filtering valid time periods.
        - Parameters:
            - json_data (DataFrame): The JSON data to be cleaned.
        - Returns:
            - DataFrame: The cleaned JSON data.
    """
        
    # Public methods for user data
    def clean_user_data(self, user_data):
        user_data = self._drop_rows_with_null_values(user_data)
        user_data = self._handle_date_formats(user_data)
        user_data = self._filter_valid_countries(user_data)
        user_data = self._correct_country_codes(user_data)
        user_data = self._standardize_phone_numbers(user_data)
        user_data = self._drop_user_data_duplicates(user_data)
        user_data = self._correct_email_formats(user_data)
        return user_data

    # Private helper methods for user data
    def _drop_rows_with_null_values(self, user_data):
        return user_data.dropna()

    def _handle_date_formats(self, user_data):
        date_columns = ['date_of_birth', 'join_date']
        for column in date_columns:
            user_data[column] = pd.to_datetime(user_data[column], errors='coerce').dt.date
        return user_data

    def _filter_valid_countries(self, user_data):
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        return user_data[user_data['country'].isin(valid_countries)]

    def _correct_country_codes(self, user_data):
        corrections = {'GGB': 'GB', 'US': 'USA', 'GER': 'DE'}
        user_data['country_code'] = user_data['country_code'].map(corrections).fillna(user_data['country_code'])
        return user_data

    def _standardize_phone_numbers(self, user_data):
        # Implement phone number standardization logic
        return user_data

    def _drop_user_data_duplicates(self, user_data):
        columns_to_check = ['first_name', 'last_name', 'company', 'email_address', 'user_uuid']
        return user_data.drop_duplicates(subset=columns_to_check)
    
    def _correct_email_formats(self, user_data):
         # Fix the email address formatting
        def correct_email_format(email):
            return ''.join(e for e in email if e.isalnum() or e in ['.', '@', '-', '_'])

        # Apply the function to the 'email_address' column
        user_data['email_address'] = user_data['email_address'].apply(correct_email_format)
        return user_data
    

    # Public methods for card data
    def clean_card_data(self, card_data):
        card_data = self._drop_null_values(card_data)
        card_data = self._drop_card_data_duplicates(card_data)
        card_data = self._parse_date_format(card_data)
        return card_data

    # Private helper methods for card data
    def _drop_null_values(self, card_data):
        return card_data.dropna()

    def _drop_card_data_duplicates(self, card_data):
        columns_to_check = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        return card_data.drop_duplicates(subset=columns_to_check)

    def _parse_date_format(self, card_data):
        # Assuming 'expiry_date' is in 'MM/YY' format and 'date_payment_confirmed' is in 'YYYY-MM-DD' format
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date

        return card_data


    # Public methods for store data
    def clean_store_data(self, stores_data):

        cleaned_store_data = stores_data.copy()
        cleaned_store_data = self._clean_date_columns(cleaned_store_data)
        cleaned_store_data = self._remove_invalid_dates(cleaned_store_data)
        cleaned_store_data = self._remove_duplicate_rows(cleaned_store_data)
        cleaned_store_data = self._correct_continent_names(cleaned_store_data)
        cleaned_store_data = self._make_numbers(cleaned_store_data)
        cleaned_store_data = self._remove_non_numeric_staff(cleaned_store_data)

        return cleaned_store_data
    
    # Private helper methods for card data
    def _clean_date_columns(self, df):
        date_columns = ['opening_date']
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], infer_datetime_format=True, errors='coerce').dt.date
        return df

    def _remove_invalid_dates(self, df):
        pattern = r'^[a-zA-Z0-9]*$'
        mask = (df['opening_date'].isna()) | (df['opening_date'].astype(str).str.contains(pattern))
        df = df[~mask]
        return df

    def _make_numbers(self, df):
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        return df
    
    def _remove_duplicate_rows(self, df):
        df = df.drop_duplicates()
        return df

    def _remove_non_numeric(self, value):
        return re.sub(r'[^0-9]', '', str(value))
    
    def _remove_non_numeric_staff(self, df):
        df = df.dropna(subset=['staff_numbers'], how='any')
        df.loc[:, 'staff_numbers'] = df['staff_numbers'].apply(self._remove_non_numeric)
        return df
    
    def _correct_continent_names(self, df):
        continent_mapping = {
            "eeEurope": "Europe",
            "eeAmerica": "America"
        }
        df['continent'] = df['continent'].replace(continent_mapping)
        return df


    # Public methods for product data
    def clean_product_data(self, product_df):

        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_product_data = product_df.copy()
        cleaned_product_data = self._missing_and_random(cleaned_product_data)
        cleaned_product_data = self._convert_product_weights(cleaned_product_data)

        return cleaned_product_data
    
    # Private methods for product data
    def _convert_product_weights(self, product_df):
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
    
    def _missing_and_random(self, cleaned_product_data):
        
        # Remove duplicate rows based on all columns.
        cleaned_product_data = cleaned_product_data.drop_duplicates()
        # Define a regular expression pattern to match rows with random letters and numbers.
        pattern = r'^[a-zA-Z0-9]*$'
        # Create a boolean mask for missing or random values in the 'weight' column.
        mask = (cleaned_product_data['weight'].isna()) | (cleaned_product_data['weight'].astype(str).str.contains(pattern))
        # Filter out rows with missing or random values in the 'weight' column.
        cleaned_product_data = cleaned_product_data[~mask]
        
        return cleaned_product_data
    

    # Public methods for orders data
    def clean_orders_data(self, orders_data):
        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_orders_data = orders_data.copy()
        # Call the method to remove specific columns
        cleaned_orders_data = self._remove_specific_columns(cleaned_orders_data)
        return cleaned_orders_data
    
    # Private helper methods for card data
    def _remove_specific_columns(self, input_df):
        # Define columns to remove
        columns_to_remove = ['first_name', 'last_name', '1']
        # Create a copy of the input DataFrame and remove specific columns
        cleaned_df = input_df.drop(columns=columns_to_remove)
        return cleaned_df

    def clean_json(self, json_data):
        # Make a copy of the input DataFrame to avoid modifying the original data.
        cleaned_json_data = json_data.copy()
        cleaned_json_data.dropna(how='all', inplace=True)
        cleaned_json_data = self._convert_json_columns(cleaned_json_data)
        cleaned_json_data = self._filter_invalid_dates(cleaned_json_data)
        cleaned_json_data = self._filter_valid_time_periods(cleaned_json_data)

        return cleaned_json_data
    
    def _filter_valid_time_periods(self, json_data):
        # Filter 'time_period' column to keep only values in the specified list
        valid_time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        return json_data[json_data['time_period'].isin(valid_time_periods)]

    def _filter_invalid_dates(self, json_data):
        # Create masks for invalid values in 'month', 'day', 'year', and 'timestamp' columns
        year_mask = ~json_data['year'].between(1992, 2022)
        month_mask = ~json_data['month'].between(1, 12)
        day_mask = ~json_data['day'].between(1, 31)
        random_symbols = r'^[a-zA-Z0-9]*$'
        timest_mask = (json_data['timestamp'].isna()) | json_data['timestamp'].astype(str).str.contains(random_symbols)

        # Combine the masks for 'month' and 'day' columns using a logical OR
        combined_mask = year_mask | month_mask | day_mask | timest_mask

        # Filter out rows with invalid dates
        return json_data[~combined_mask]
    
    def _convert_json_columns(self, json_data):
        # Convert the 'day', 'month', and 'year' columns to numeric (integer) 
        # type and handle any conversion errors by setting them to NaN
        json_data['month'] = pd.to_numeric(json_data['month'], errors='coerce')
        json_data['day'] = pd.to_numeric(json_data['day'], errors='coerce')
        json_data['year'] = pd.to_numeric(json_data['year'], errors='coerce')
        return json_data
