from pathlib import Path
from loguru import logger

# Logging configuration
def configure_logging(file: str) -> None:
    """Sets up logging to a file using loguru."""
    logs_dir = Path("logs/logs_data")
    logs_dir.mkdir(parents=True, exist_ok=True)  # Ensure the logs directory exists
    file_name = Path(file).stem
    logger.add(logs_dir/f'{file_name}.log', format='{time} {level} {message}', level='INFO')
