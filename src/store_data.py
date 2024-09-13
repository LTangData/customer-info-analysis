import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
from mysql.connector import Error
from pathlib import Path
from src.config import EXTERNAL_DATA_DIR
from loguru import logger
from logs.log_config import configure_logging

# Load environment variables from .env file
load_dotenv()

# Constants
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USERNAME = os.environ.get('MYSQL_USERNAME')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE')


class MySQLDatabaseManager:
    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        '''Initializes the database connection.'''
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            if self.connection.is_connected():
                logger.info('Successfully connected to the database.')
        except Error as e:
            logger.error(f'Error connecting to MySQL: {e}')
            self.connection = None
    
    def create_table(self, table_name: str, columns: str) -> None:
        '''Creates a table with the specified columns.'''
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            logger.info(f'Table "{table_name}" created or exists already.')
        except Error as e:
            logger.error(f'Error creating table {table_name}: {e}')
    
    def insert_data(self, table_name: str, data: pd.DataFrame) -> None:
        '''Inserts data into the specified table.'''
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f'INSERT INTO {table_name} VALUES ({placeholders})'
        try:
            cursor = self.connection.cursor()
            for _, row in data.iterrows():
                cursor.execute(insert_query, tuple(row))
            self.connection.commit()
            cursor.close()
            logger.info(f'Data inserted into "{table_name}".')
        except Error as e:
            logger.error(f'Error inserting data into {table_name}: {e}')
    
    def close(self) -> None:
        '''Closes the database connection.'''
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info('Database connection closed.')


class CSVToMySQLLoader:
    def __init__(self, db_manager: MySQLDatabaseManager, csv_files: list) -> None:
        '''Initializes with a database manager and a list of CSV file paths.'''
        self.db_manager = db_manager
        self.csv_files = csv_files
    
    def load_csv(self, file_path: str) -> pd.DataFrame | None:
        '''Loads CSV file into a DataFrame.'''
        try:
            df = pd.read_csv(file_path)
            logger.info(f'Loaded data from "{file_path}".')
            return df
        except FileNotFoundError:
            logger.error(f'CSV file "{file_path}" not found.')
            return None
        except pd.errors.EmptyDataError:
            logger.error(f'CSV file "{file_path}" is empty.')
            return None
        except pd.errors.ParserError:
            logger.error(f'Error parsing CSV file "{file_path}".')
            return None
    
    def load_csv_to_db(self) -> None:
        '''Loads each CSV file into the database.'''
        for i, csv_file in enumerate(self.csv_files):
            table_name = Path(csv_file).stem[:-8]
            df = self.load_csv(csv_file)
            if df is not None:
                columns = self.generate_columns_definition(df)
                self.db_manager.create_table(table_name, columns)
                self.db_manager.insert_data(table_name, df)
    
    @staticmethod
    def generate_columns_definition(df: pd.DataFrame) -> str:
        '''Generates a MySQL-compatible column definition from the DataFrame.'''
        return ', '.join([f'{col} VARCHAR(255)' for col in df.columns])

def get_files(folder_path: str, file_extension: str = 'csv') -> list:
    '''Retrieves all files with specified extension from specified folder.'''
    try:
        # Validate that the folder exists
        if not os.path.exists(folder_path):
            logger.error(f'Folder "{folder_path}" does not exist.')
            return []

        # List all files with the specified extension
        files = [f for f in os.listdir(folder_path) if f.endswith(file_extension)]
        full_paths = [os.path.join(folder_path, f) for f in files]
        
        if not full_paths:
            logger.warning(f'No files with the extension "{file_extension}" found in this location "{folder_path}"')
        else:
            logger.info(f'Found {len(full_paths)} "{file_extension}" file(s) in this location "{folder_path}".')

        return full_paths
    except Exception as e:
        logger.error(f'Error retrieving file list: {e}')
        return []

def main():
    '''Main entry point for the script.'''
    configure_logging(__file__)  # Set up logging at the start of the program

    # Database connection settings
    db_settings = {
        'host': MYSQL_HOST,
        'user': MYSQL_USERNAME,
        'password': MYSQL_PASSWORD,
        'database': MYSQL_DATABASE
    }

    # List of CSV files
    file_extension = 'csv'
    csv_files = get_files(EXTERNAL_DATA_DIR, file_extension)

    # Initialize the database manager
    db_manager = MySQLDatabaseManager(**db_settings)

    # If the connection was successful, proceed
    if db_manager.connection:
        # Initialize the CSV loader and load data
        loader = CSVToMySQLLoader(db_manager, csv_files)
        loader.load_csv_to_db()

        # Close the database connection
        db_manager.close()


if __name__ == '__main__':
    main()