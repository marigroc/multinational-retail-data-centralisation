# Multinational Retail Data Centralization

## Table of Contents
- [Description](#description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)
- [License Information](#license-information)

## Description
In this project I encountered issue of a multinational company that sells various goods across the globe.
Their sales data was spread across many different data sources and it was not sustainable.
My task was to help the organization to be more data-driven and the data to be accessible from one centralized location.
My first goal was to produce a system that stored the company data in a database that acts as as a single source of truth for sales data.

In order to achieve my goals I had to use my knowledge about AWS, SQL, data cleaning with Pandas, and web APIs.

## Installation Instructions
1. Clone the repository to your local machine.
2. Install the required libraries by running the following command:
   ```bash
   pip install -r requirements.txt
    ```
3. Create a db_creds.yaml file with the necessary database credentials.
4. Create a db_creds_local.yaml file with local database credentials if needed.
5. Set up your database and configure the connection accordingly.

## Usage Instructions

To use this project, follow these steps:

Run the main script at the bottom of the database_utils.py to extract, clean, and upload the data to your centralized database:
```bash
main_script.py
```
The script uses the DatabaseConnector, DataCleaning, and DataExtractor classes to:
Extract data from various sources (relational database, PDF, API, and S3).
Clean the extracted data to handle missing values, date errors, and other issues.
Upload the cleaned data to your centralized database.
Customize the project for your specific use case by modifying the classes and scripts as needed.

## File structure

database_utils.py: Contains the DatabaseConnector class for database connections.
data_cleaning.py: Contains the DataCleaning class for data cleaning operations.
data_extraction.py: Contains the DataExtractor class for data extraction and retrieval.
main.py: Demonstrates how to use the classes to extract, clean, and upload data.
star_schema.sql: Contains the SQL queries that creates the star-based schema for the database.
requirements.txt: Lists the required Python libraries.
README.md: This documentation file.

## License Information

This project is licensed under the MIT License. You can find more details in the LICENSE file.
