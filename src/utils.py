import yaml
from pathlib import Path
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
        logging.error(f"Failed to load config: {e}")
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

    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Created directory: {path}")
        else:
            logging.debug(f"Directory already exists: {path}")


def create_date_folder_path(base_path: str) -> Path:
    """
    Create a date-based folder path structure.

    Parameters:
        base_path (str): Base directory path

    Returns:
        Path: Full path to the date folder
    """
    current_date = datetime.now()

    # Extract date components
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    # Build the folder path
    date_folder = Path(base_path) / year / month / day

    # Create the folder structure if it doesn't exist
    date_folder.mkdir(parents=True, exist_ok=True)

    return date_folder
