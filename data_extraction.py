import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import tabula
import requests
import boto3

class DataExtractor:
    """
    This class provides methods for extracting and retrieving data from a relational database and PDF documents.

    Attributes:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class for database connections.

    Methods:
        read_rds_table(db_connector, table_name)
            - Extracts data from a specified table in the connected database.
            - Parameters:
                - db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
                - table_name (str): Name of the table to extract data from.
            - Returns:
                - DataFrame: Extracted data from the specified table.

        extract_data_from_db(table_name)
            - Retrieves data from a specified table in the connected database.
            - Parameters:
                - table_name (str): Name of the table to retrieve data from.
            - Returns:
                - list of dictionaries: Retrieved data from the specified table.

        retrieve_pdf_data(url)
            - Extracts data from a PDF document located at the specified URL.
            - Parameters:
                - url (str): URL of the PDF document.
            - Returns:
                - DataFrame: Extracted data from the PDF.

    Note:
        To use the class, you need to pass an instance of the DatabaseConnector class when initializing DataExtractor.
    """
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        self.header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        self.stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        self.number_ofStores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    def read_rds_table(self, db_connector, table_name):
        try:
            # Initialize the database engine
            engine = db_connector.init_db_engine()

            # Construct the SQL query to select all data from the specified table
            query = f"SELECT * FROM {table_name}"

            # Read the data from the table into a DataFrame
            df = pd.read_sql(query, engine)

            # Drop the 'index' column if it exists
            if 'index' in df.columns:
                df = df.drop(columns=['index'])

            return df
        except SQLAlchemyError as e:
            print(f"Error reading table '{table_name}': {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def extract_data_from_db(self, table_name):
        # Check if the database engine is initialized
        if not hasattr(self.db_connector, 'engine'):
            print("Database engine not initialized. Call init_db_engine() first.")
            return []

        # Get a database connection and create a session
        conn = self.db_connector.engine.connect()

        try:
            # Execute a SELECT query to retrieve data from the specified table
            result = conn.execute(f"SELECT * FROM {table_name}")
            # Fetch the data as a list of dictionaries
            data = [row for row in result]
            return data
        except Exception as e:
            print(f"Error extracting data from the database: {str(e)}")
            return []
        finally:
            conn.close()

    def retrieve_pdf_data(self, pdf_url):

        try:
            # Read the PDF into a list of DataFrames (one DataFrame per page)
            dfs = tabula.read_pdf(pdf_url, pages='all')  # Use 'all' as a string

            # Concatenate DataFrames from all pages into a single DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df

        except Exception as e:
            print(f"Error extracting data from PDF: {str(e)}")
            return None

    def list_number_of_stores(self, number_of_stores_endpoint, header):
        try:
            response = requests.get(number_of_stores_endpoint, headers=header)
            response.raise_for_status()  # Raise an error for non-2xx responses
            data = response.json()
            number_of_stores = data['number_stores']
            return number_of_stores
        except requests.exceptions.RequestException as e:
            print(f"Error making the API request: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return 0

    def retrieve_stores_data(self, store_endpoint, header, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            store_url = store_endpoint.format(store_number=store_number)  # Provide the value for {store_number}
            response = requests.get(store_url, headers=header)
            if response.status_code == 200:
                store_data = response.json()  # Assuming the response contains store data
                stores_data.append(store_data)

        if stores_data:
            stores_df = pd.DataFrame(stores_data)
            
            return stores_df
        else:
            print("Failed to retrieve store data from the API.")
            return None
        
    def extract_from_s3(self, s3_address):
        client = boto3.client('s3')
        product_df = pd.read_csv(s3_address, index_col= 0) 
        return product_df
    
    def extract_s3_json(self, link):
        
        response = requests.get(link)
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        
        return df
        