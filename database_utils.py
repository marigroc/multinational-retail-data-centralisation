import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DatabaseConnector:
    """
    This class provides methods for connecting to a PostgreSQL database and performing database-related tasks.

    Attributes:
        db_creds (dict): A dictionary containing the database credentials.
        engine (sqlalchemy.engine.base.Engine): A SQLAlchemy database engine used for database connections.

    Methods:
        1. read_db_creds()
            - Reads the database credentials from a 'db_creds.yaml' file.
            - Returns:
                - dict: A dictionary containing the database credentials.

        2. init_db_engine()
            - Initializes a SQLAlchemy database engine using the database credentials.
            - Returns:
                - sqlalchemy.engine.base.Engine: A SQLAlchemy database engine.

        3. list_db_tables()
            - Lists all tables in the connected PostgreSQL database.
            - Returns:
                - list: A list of table names in the 'public' schema of the database.

        4. upload_to_db(data_df, table_name)
            - Uploads data from a Pandas DataFrame to a specified database table.
            - Parameters:
                - data_df (DataFrame): The data to be uploaded.
                - table_name (str): The name of the table in the database.
        
        5. close_connection()
            - Closes the database connection if it's open.

    Usage:
        Example usage of this class can be found in the '__main__' block of this script.
    """
    def __init__(self):

        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()
        
    def read_db_creds(self):
        try:
            with open('db_creds.yaml', 'r') as yaml_file:
                db_creds = yaml.safe_load(yaml_file)
                return db_creds
        except FileNotFoundError:
            print("db_creds.yaml file not found. Make sure to create it with the correct credentials.")
            return {}
        
    def init_db_engine(self):

        db_url = f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self):
        with self.engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = connection.execute(query)
            table_names = [row[0] for row in result]
            # print(table_names)
            
        return table_names

    def upload_to_db(self, data_df, table_name):

        host = "localhost"
        user = "postgres"
        dbname = "sales_data"
        password = ""
        port = "5432"

        with open('db_creds_local.yaml') as db_creds_local:
            creds = yaml.safe_load(db_creds_local)
        
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}")
        engine.connect()
        data_df.to_sql(table_name, engine, if_exists='replace')

    def close_connection(self):
        if hasattr(self, 'conn'):
            self.engine.dispose()
            print("Database connection closed")

if __name__ == "__main__":
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_cleaner = DataCleaning()
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"  # Define the PDF URL


    # Get the list of available tables
    table_names = db_connector.list_db_tables()

    user_data_table_name = 'legacy_users'
    order_table_name = "orders_table"

    # Use read_rds_table to extract the user data and return it as a Pandas DataFrame
    user_data = data_extractor.read_rds_table(db_connector, user_data_table_name)
    orders_data = data_extractor.read_rds_table(db_connector, order_table_name)
    # print(orders_data) # Debugging check
    if user_data is not None:
        # the user data in the 'user_data' DataFrame
        cleaned_user_data = data_cleaner.clean_user_data(user_data)  # This line cleans the user data
        # print(cleaned_user_data) # Debugging check
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')
        print(f"Data uploaded to 'dim_users' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{user_data_table_name}' table.")
    
    if orders_data is not None:
        # the orders data in the 'orders_data' DataFrame
        cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)  # This line cleans the user data
        # print(cleaned_orders_data) # Debugging check
        db_connector.upload_to_db(cleaned_orders_data, 'orders_table')
        print(f"Data uploaded to 'orders_table' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{order_table_name}' table.")

    extracted_data = data_extractor.retrieve_pdf_data(pdf_url)
    if extracted_data is not None:
        cleaned_data = data_cleaner.clean_card_data(extracted_data)
        db_connector.upload_to_db(cleaned_data, 'dim_card_details')

    db_connector.close_connection()   


    # Load, store, clean and upload store data
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    data_extractor = DataExtractor(db_connector)  # Create an instance of DataExtractor
    num_stores = data_extractor.list_number_of_stores(url, headers)
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    
    stores = data_extractor.retrieve_stores_data(url, headers, num_stores)
    if stores is not None:
        stores = DataCleaning().clean_store_data(stores)
        db_connector.upload_to_db(stores, 'dim_store_details')
        print(f"Data uploaded to 'dim_store_details' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{order_table_name}' table.")

    # Extract data from S3
    products_data = data_extractor.extract_from_s3("s3://data-handling-public/products.csv")
    if products_data is not None:
        # Apply data cleaning including weight conversion
        cleaned_products_data = data_cleaner.clean_product_data(products_data)
        db_connector.upload_to_db(cleaned_products_data, "dim_products")
        print(f"Data uploaded to 'dim_products' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{order_table_name}' table.")

    # Extract data from json S3
    json_data = data_extractor.extract_s3_json("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    if json_data is not None:
        # Apply data cleaning including weight conversion
        cleaned_json_data = data_cleaner.clean_json(json_data)
        db_connector.upload_to_db(cleaned_json_data, "dim_date_times")
        print(f"Data uploaded to 'dim_date_time' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{order_table_name}' table.")