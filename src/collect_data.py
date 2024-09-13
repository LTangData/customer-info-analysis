import os
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from config import EXTERNAL_DATA_DIR, PROJ_ROOT
from loguru import logger

# Add the project root directory to the Python path
import sys
sys.path.append(str(PROJ_ROOT))

from logs.log_config import configure_logging

# Load environment variables from .env file
load_dotenv()

# Constants
KAGGLE_DATA_ID = os.getenv('KAGGLE_DATA_ID')
KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')


def setup_kaggle_api() -> KaggleApi:
    '''Sets up Kaggle API with authentication.'''
    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        logger.error('Kaggle username or API key not found in environment variables.')
        raise ValueError('Kaggle credentials are missing.')
    
    # Configure Kaggle API
    os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = KAGGLE_KEY

    api = KaggleApi()
    try:
        api.authenticate()
        logger.success('Kaggle API authentication successful.')
    except Exception as e:
        logger.error(f'Kaggle API authentication failed: {e}')
        raise
    return api

def collect_data_from_kaggle(api: KaggleApi) -> None:
    '''Downloads data from Kaggle using the given API.'''
    if not KAGGLE_DATA_ID:
        logger.error('KAGGLE_DATA_ID is not defined in the environment variables.')
        raise ValueError('Dataset ID is missing.')

    try:
        logger.info(f'Downloading dataset: {KAGGLE_DATA_ID}')
        api.dataset_download_files(KAGGLE_DATA_ID, path=EXTERNAL_DATA_DIR, unzip=True)
        logger.success('Dataset downloaded and extracted successfully.')
    except Exception as e:
        logger.error(f'Failed to download dataset: {e}')
        raise

def main():
    '''Main entry point for the script.'''
    configure_logging(__file__)  # Set up logging at the start of the program

    try:
        api = setup_kaggle_api()
        collect_data_from_kaggle(api)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        exit(1)

if __name__ == '__main__':
    main()