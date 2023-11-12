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

    Public Methods for DateTime Data:
    6. clean_time(time_data)
        - Cleans datetime data by converting date columns, removing rows with invalid dates, and filtering valid time periods.
        - Parameters:
            - time_data (DataFrame): The datetime data to be cleaned.
        - Returns:
            - DataFrame: The cleaned datetime data.
    """
        
    # Common private method
    def _remove_duplicate_rows(self, df, column):
        return df.drop_duplicates(subset=column)
    
# 1.
    # Public methods for user data
    def clean_user_data(self, user_data):
        clean_user_data = user_data.copy()
        clean_user_data = self._drop_rows_with_null_values(clean_user_data)
        clean_user_data = self._filter_valid_countries(clean_user_data)
        clean_user_data = self._correct_country_codes(clean_user_data)
        clean_user_data = self._standardize_phone_numbers(clean_user_data)
        clean_user_data.reset_index(drop=True, inplace=True)
        return clean_user_data

    # Private methods for user data
    def _drop_rows_with_null_values(self, df):
        return df.dropna()

    def _filter_valid_countries(self, df):
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        return df[df['country'].isin(valid_countries)]

    def _correct_country_codes(self, df):
        corrections = {'GGB': 'GB'}
        df['country_code'] = df['country_code'].map(corrections).fillna(df['country_code'])
        return df

    def _standardize_phone_numbers(self, df):
        # Remove the two-digit country code if it is present
        df['phone_number'] = df.apply(lambda row: re.sub(fr'^\+44|^\+49|^{row["country_code"]}', '', row['phone_number']), axis=1)
        # Remove non-digit characters from the phone number
        df['phone_number'] = df['phone_number'].apply(lambda phone: re.sub(r'\D', '', phone))
        # Add a leading '0' if the number doesn't start with '0'
        df['phone_number'] = df['phone_number'].apply(lambda phone: '0' + phone if not phone.startswith('0') else phone)
        return df

# 2.
    # Public methods for card data
    def clean_card_data(self, card_data):
        clean_card_data = card_data.copy()
        clean_card_data = self._parse_date_format(clean_card_data)
        clean_card_data = self._remove_duplicate_rows(clean_card_data, column=['card_number'])
        clean_card_data = self._remove_long_expiry_dates(clean_card_data)
        clean_card_data = self._remove_short_card_numbers(clean_card_data)
        clean_card_data = self._remove_non_numeric_symbols(clean_card_data, 'card_number')
        clean_card_data = self._drop_null_values(clean_card_data)
        clean_card_data.reset_index(drop=True, inplace=True)
        return clean_card_data

    # Private methods for card data
    def _drop_null_values(self, df):
        return df.dropna()
    
    def _remove_short_card_numbers(self, df):
        # Remove rows with 'card_number' shorter than 8 digits
        df = df[df['card_number'].str.len() >= 8]
        return df
    
    def _remove_long_expiry_dates(self, df):
        # Remove rows with 'expiry_date' longer than 5 characters
        df = df[df['expiry_date'].str.len() <= 5]
        return df

    def _parse_date_format(self, df):
        # Assuming 'expiry_date' is in 'MM/YY' format and 'date_payment_confirmed' is in 'YYYY-MM-DD' format
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date
        return df
    
    def _remove_non_numeric_symbols(self, df, column_name):
        # Use regular expressions to replace non-numeric characters with an empty string
        df[column_name] = df[column_name].str.replace(r'[^0-9]', '', regex=True)
        return df

# 3.
    # Public methods for store data
    def clean_store_data(self, stores_data):
        cleaned_store_data = stores_data.copy()
        cleaned_store_data = self._clean_date_columns(cleaned_store_data)
        cleaned_store_data = self._remove_invalid_dates(cleaned_store_data)
        cleaned_store_data = self._correct_continent_names(cleaned_store_data)
        cleaned_store_data = self._remove_non_numeric_staff(cleaned_store_data)
        cleaned_store_data.reset_index(drop=True, inplace=True)
        if 'level_0' in cleaned_store_data.columns:
            # Drop 'level_0' column
            cleaned_store_data.drop(['level_0'], axis=1, inplace=True)
            # Reset index
            cleaned_store_data.reset_index(drop=True, inplace=True)
        else:
            print("'level_0' column not found in DataFrame. Exiting function.")
            return cleaned_store_data 
    
    # Private methods for card data
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

# 4.
    # Public methods for product data
    def clean_product_data(self, product_df):
        cleaned_product_data = product_df.copy()
        cleaned_product_data = self._missing_and_random(cleaned_product_data, column='product_price')
        cleaned_product_data = self._convert_product_weights(cleaned_product_data)
        cleaned_product_data.reset_index(drop=True, inplace=True)
        return cleaned_product_data
    
    # Private methods for product data
    def _convert_product_weights(self, df):
        decimal_of_kg = []
        for index, row in df.iterrows():
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
        df.loc[:, 'weight'] = decimal_of_kg
        return df
    
    def _missing_and_random(self, df, column):      
        # Define a regular expression pattern to match rows with random symbols
        pattern = r'^[a-zA-Z0-9]*$'
        # Create a boolean mask for rows with random symbols in the specified column
        mask = df[column].astype(str).str.contains(pattern)
        # Filter out rows with random symbols
        df = df[~mask]
        return df
    
# 5.
    # Public methods for orders data
    def clean_orders_data(self, orders_data):
        cleaned_orders_data = orders_data.copy()
        cleaned_orders_data = self._remove_specific_columns(cleaned_orders_data)
        if 'level_0' in cleaned_orders_data.columns:
            # Drop 'level_0' column
            cleaned_orders_data.drop(['level_0'], axis=1, inplace=True)
            # Reset index
            cleaned_orders_data.reset_index(drop=True, inplace=True)
        else:
            print("'level_0' column not found in DataFrame.")
        return cleaned_orders_data
    
    # Private methods for card data
    def _remove_specific_columns(self, df):
        # Define columns to remove
        columns_to_remove = ['first_name', 'last_name', '1']
        # Create a copy of the input DataFrame and remove specific columns
        df = df.drop(columns=columns_to_remove)
        return df
    
# 6.
    # Public methods for datetime
    def clean_time(self, time_data):
        clean_time = time_data.copy()
        clean_time.dropna(how='all', inplace=True)
        clean_time = self._convert_json_columns(clean_time)
        clean_time = self._filter_invalid_dates(clean_time)
        clean_time = self._filter_valid_time_periods(clean_time)
        clean_time.reset_index(drop=True, inplace=True)
        return clean_time
    
    # Private methods for datetime
    def _filter_valid_time_periods(self, df):
        valid_time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        return df[df['time_period'].isin(valid_time_periods)]

    def _filter_invalid_dates(self, df):
        year_mask = ~df['year'].between(1992, 2022)
        month_mask = ~df['month'].between(1, 12)
        day_mask = ~df['day'].between(1, 31)
        random_symbols = r'^[a-zA-Z0-9]*$'
        timest_mask = (df['timestamp'].isna()) | df['timestamp'].astype(str).str.contains(random_symbols)
        # Combine the masks for 'month' and 'day' columns using a logical OR
        combined_mask = year_mask | month_mask | day_mask | timest_mask
        return df[~combined_mask]
    
    def _convert_json_columns(self, df):
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['day'] = pd.to_numeric(df['day'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        return df
