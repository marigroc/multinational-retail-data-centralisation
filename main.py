from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


if __name__ == "__main__":
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_cleaner = DataCleaning()
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"  # Define the PDF URL
    num_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_detail_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    s3_csv = "s3://data-handling-public/products.csv"
    s3_json = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
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
        print(f"Data uploaded to 'dim_card_details' table successfully.")
    else:
        print("Failed to retrieve data from pdf.")
    db_connector.close_connection()   


    # Load, store, clean and upload store data
    data_extractor = DataExtractor(db_connector)  # Create an instance of DataExtractor
    num_stores = data_extractor.list_number_of_stores(num_stores_url, headers)
    stores = data_extractor.retrieve_stores_data(store_detail_url, headers, num_stores)
    if stores is not None:
        stores = DataCleaning().clean_store_data(stores)
        db_connector.upload_to_db(stores, 'dim_store_details')
        print(f"Data uploaded to 'dim_store_details' table successfully.")
    else:
        print("Failed to retrieve data using API key.")

    # Extract data from S3
    products_data = data_extractor.extract_from_s3(s3_csv)
    if products_data is not None:
        # Apply data cleaning including weight conversion
        cleaned_products_data = data_cleaner.clean_product_data(products_data)
        db_connector.upload_to_db(cleaned_products_data, "dim_products")
        print(f"Data uploaded to 'dim_products' table successfully.")
    else:
        print("Failed to retrieve data from the s3 bucket.")

    # Extract data from json S3
    json_data = data_extractor.extract_s3_json(s3_json)
    if json_data is not None:
        # Apply data cleaning including weight conversion
        cleaned_json_data = data_cleaner.clean_json(json_data)
        db_connector.upload_to_db(cleaned_json_data, "dim_date_times")
        print(f"Data uploaded to 'dim_date_time' table successfully.")
    else:
        print("Failed to retrieve data from the json file.")