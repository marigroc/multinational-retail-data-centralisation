import yaml
from sqlalchemy import create_engine, text


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
        with open('db_creds_local.yaml') as db_creds_local:
            creds = yaml.safe_load(db_creds_local) 
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}")
        engine.connect()
        data_df.to_sql(table_name, engine, if_exists='replace')

    def close_connection(self):
        if hasattr(self, 'conn'):
            self.engine.dispose()
            print("Database connection closed")