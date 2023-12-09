from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from dotenv import load_dotenv
import os


"""
Data Processing Script

This script demonstrates the process of extracting, cleaning, and uploading data from various sources to a database.

1. It connects to a database using the `DatabaseConnector` class.
2. Extracts data from the database and external sources using the `DataExtractor` class.
3. Cleans the extracted data using the `DataCleaning` class.
4. Uploads the cleaned data to specific tables in the database.

Dependencies:
- data_extraction.py: Contains the DataExtractor class for extracting data.
- data_cleaning.py: Contains the DataCleaning class for cleaning data.
- database_utils.py: Contains the DatabaseConnector class for database operations.

Inputs:
- URLs for PDF and API endpoints for data extraction.
- S3 URLs for CSV and JSON data.
- Database table names for storing cleaned data.

Outputs:
- Success/failure messages for each data extraction, cleaning, and upload operation.

Usage:
- Make sure to set the appropriate values for URLs, table names, and authentication tokens.
- Ensure that the required dependencies are available in the environment.

"""
if __name__ == "__main__":
    # Load environment variables from the .env file
    load_dotenv()
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_cleaner = DataCleaning()
    # Access environment variables
    pdf_url = os.getenv("PDF_URL")
    num_stores_url = os.getenv("NUM_STORES_URL")
    store_detail_url = os.getenv("STORE_DETAIL_URL")
    api_key = os.getenv("API_KEY")
    s3_csv = os.getenv("S3_CSV")
    s3_json = os.getenv("S3_JSON")
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
        cleaned_user_data = data_cleaner.clean_user_data(user_data)  
        # print(cleaned_user_data) # Debugging check
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')
        print(f"Data uploaded to 'dim_users' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{user_data_table_name}' table.")
    
    if orders_data is not None:
        # the orders data in the 'orders_data' DataFrame
        cleaned_orders_data = data_cleaner.clean_orders_data(orders_data) 
        # print(cleaned_orders_data) # Debugging check
        db_connector.upload_to_db(cleaned_orders_data, 'orders_table')
        print(f"Data uploaded to 'orders_table' table successfully.")
    else:
        print(f"Failed to retrieve data from the '{order_table_name}' table.")
    # Get the card data, clean, and upload
    extracted_data = data_extractor.retrieve_pdf_data(pdf_url)
    if extracted_data is not None:
        cleaned_data = data_cleaner.clean_card_data(extracted_data)
        db_connector.upload_to_db(cleaned_data, 'dim_card_details')
        print(f"Data uploaded to 'dim_card_details' table successfully.")
    else:
        print("Failed to retrieve data from pdf.")
    db_connector.close_connection()   
    # Load, store, clean and upload store data
    data_extractor = DataExtractor(db_connector)  
    num_stores = data_extractor.list_number_of_stores(num_stores_url, headers)
    stores = data_extractor.retrieve_stores_data(store_detail_url, headers, num_stores)
    if stores is not None:
        stores = DataCleaning().clean_store_data(stores)
        db_connector.upload_to_db(stores, 'dim_store_details')
        print(f"Data uploaded to 'dim_store_details' table successfully.")
    else:
        print("Failed to retrieve data using API key.")
    # Extract products data from S3
    products_data = data_extractor.extract_from_s3(s3_csv)
    if products_data is not None:
        cleaned_products_data = data_cleaner.clean_product_data(products_data)
        db_connector.upload_to_db(cleaned_products_data, "dim_products")
        print(f"Data uploaded to 'dim_products' table successfully.")
    else:
        print("Failed to retrieve data from the s3 bucket.")
    # Extract time and dates data from json S3
    json_data = data_extractor.extract_s3_json(s3_json)
    if json_data is not None:
        cleaned_json_data = data_cleaner.clean_json(json_data)
        db_connector.upload_to_db(cleaned_json_data, "dim_date_times")
        print(f"Data uploaded to 'dim_date_time' table successfully.")
    else:
        print("Failed to retrieve data from the json file.")
   