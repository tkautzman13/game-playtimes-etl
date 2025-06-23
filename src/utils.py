import yaml
from pathlib import Path
import argparse
import logging
import os
from datetime import datetime
from typing import Iterable


def load_config(config_path: str) -> dict:
    """
    Loads and validates a YAML configuration file from the specified path.
    Raises appropriate errors if the file is missing, empty, or invalid.

    Parameters:
    -----------
    config_path
        Path to the configuration YAML file.

    Returns:
    --------
    dict
        Dictionary containing the loaded configuration data.
    """
    logger = get_logger()

    try:
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError("Config file is empty or invalid")
        
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def ensure_directories_exist(paths: Iterable[str]) -> None:
    """
    Ensures that each directory in the given iterable exists.
    If a directory does not exist, it will be created.

    Args:
        paths (Iterable[str]): A collection of directory path strings to check and create if necessary.

    Returns:
        None
    """
    logger = get_logger()

    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info(f"Created directory: {path}")
        else:
            logger.debug(f"Directory already exists: {path}")


def setup_logger(name: str = "data_pipeline", log_level: int = logging.INFO, log_dir: str = "logs") -> logging.Logger:
    """
    Set up a logger that writes to both file and console.
    
    Parameters:
        name (str): Logger name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir (str): Directory to store log files
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get current date
    current_date = datetime.now()

    # Extract date components
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    log_dir = Path(log_dir) / year / month / day
    
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # File handler - with timestamp in filename
    timestamp = current_date.strftime("%Y%m%d_%H%M%S")
    log_filename = f"{name}_{timestamp}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = 'data_pipeline') -> logging.Logger:
    """
    Get an existing logger or create a new one if it doesn't exist.
    """
    return logging.getLogger(name)