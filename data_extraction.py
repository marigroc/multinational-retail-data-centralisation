from sqlalchemy.exc import SQLAlchemyError
import boto3
import pandas as pd
import requests
import tabula


class DataExtractor:
    """
    This class provides methods for extracting and retrieving data from a relational database, PDF documents,
    API endpoints, and Amazon S3 storage.

    Attributes:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class for database connections.

    Methods:
        read_rds_table(table_name)
            - Extracts data from a specified table in the connected database.
            - Parameters:
                - table_name (str): Name of the table to extract data from.
            - Returns:
                - DataFrame: Extracted data from the specified table.

        extract_data_from_db(table_name)
            - Retrieves data from a specified table in the connected database.
            - Parameters:
                - table_name (str): Name of the table to retrieve data from.
            - Returns:
                - list of dictionaries: Retrieved data from the specified table.

        retrieve_pdf_data(pdf_url)
            - Extracts data from a PDF document located at the specified URL.
            - Parameters:
                - pdf_url (str): URL of the PDF document.
            - Returns:
                - DataFrame: Extracted data from the PDF.

        list_number_of_stores(number_of_stores_endpoint, header)
            - Retrieves the number of stores from an API endpoint.
            - Parameters:
                - number_of_stores_endpoint (str): API endpoint for getting the number of stores.
                - header (dict): Headers for the API request.
            - Returns:
                - int: Number of stores.

        retrieve_stores_data(store_endpoint, header, number_of_stores)
            - Retrieves store data from an API endpoint for a specified number of stores.
            - Parameters:
                - store_endpoint (str): API endpoint for getting store data.
                - header (dict): Headers for the API request.
                - number_of_stores (int): Number of stores to retrieve.
            - Returns:
                - DataFrame: Extracted store data.

        extract_from_s3(s3_address)
            - Extracts data from an Amazon S3 storage location.
            - Parameters:
                - s3_address (str): Address of the data file in Amazon S3.
            - Returns:
                - DataFrame: Extracted data.

        extract_s3_json(link)
            - Extracts JSON data from an Amazon S3 storage location.
            - Parameters:
                - link (str): Link to the JSON data in Amazon S3.
            - Returns:
                - DataFrame: Extracted JSON data.
                
    Note:
        To use the class, you need to pass an instance of the DatabaseConnector class when initializing DataExtractor.
    """
    def __init__(self, db_connector):
        self.db_connector = db_connector

    # Read user_data data from the RDS table
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
        
    # Extract user_data from the RDS table
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

    # Read card_data data from the RDS table
    def retrieve_pdf_data(self, pdf_url):
        try:
            # Read the PDF into a list of DataFrames (one DataFrame per page)
            dfs = tabula.read_pdf(pdf_url, pages='all')  
            # Concatenate DataFrames from all pages into a single DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        except Exception as e:
            print(f"Error extracting data from PDF: {str(e)}")
            return None

    # Get the number_of_stores
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

    # Get the stores_data
    def retrieve_stores_data(self, store_endpoint, header, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            store_url = store_endpoint.format(store_number=store_number)  
            response = requests.get(store_url, headers=header)
            if response.status_code == 200:
                store_data = response.json()  
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
        #print(product_df.info())
        return product_df
    
    def extract_s3_json(self, link):
        response = requests.get(link)
        data = response.json()
        time_data = pd.DataFrame.from_dict(data)
        return time_data
        