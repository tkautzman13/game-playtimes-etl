import re
import os
import pandas as pd
import logging

def parse_time_to_seconds(time_str: str) -> int:
    """
    Convert time string like '11 hours, 45 minutes, 10 seconds' to total minutes

    Parameters:
        time_str (str): Time string in format like '11 hours, 45 minutes, 10 seconds'

    Returns:
        int: Total time in seconds
    """
    if pd.isna(time_str) or time_str == "":
        return 0

    hours = 0
    minutes = 0
    seconds = 0

    # Extract hours
    hour_match = re.search(r"(\d\d+) hours,", str(time_str))
    if hour_match:
        hours = int(hour_match.group(1))

    # Extract minutes
    min_match = re.search(r"(\d\d+) minutes,", str(time_str))
    if min_match:
        minutes = int(min_match.group(1))

    # Extract seconds
    sec_match = re.search(r"(\d\d+) seconds", str(time_str))
    if sec_match:
        seconds = int(sec_match.group(1))

    return (hours * 3600) + (minutes * 60) + seconds


def extract_rom_title(rom_path: str) -> str:
    """    
    Extract the ROM title from the file path by performing the following:
    1. Remove the file extension
    2. Remove any text within parentheses or brackets
    3. Remove leading and trailing whitespace
    4. If the title contains ', The', move 'The' to the front

    Parameters:
        rom_path (str): Path to the ROM file
    Returns:
        str: Cleaned ROM title
    """
    
    rom_title = os.path.basename(rom_path)
    rom_title = os.path.splitext(rom_title)[0]
    rom_title = re.sub(r'\s*\(.*?\)|\s*\[.*?\]', '', rom_title) # Remove parentheses and brackets and their contents
    rom_title = rom_title.strip()
    if ', The' in rom_title:
        rom_title = 'The ' + rom_title.replace(', The', '')

    return rom_title


def load_log_data(base_directory: str) -> pd.DataFrame:
    """    
    Load log files from the specified directory and extract relevant data

    Parameters:
        log_dir (str): Path to the directory containing log files
    Returns:
        pd.DataFrame: DataFrame containing extracted data with columns 'filename', 'content_file', and 'runtime'
    """
    logging.info(f"Loading all log files from {base_directory}...")

    # Patterns
    content_pattern = r'\[INFO\] \[Content\]: Loading content file: "(.*?)"\.'
    time_pattern = r'\[INFO\] \[Core\]: Content ran for a total of: (.*)\.'

    # List to store results for all files
    results = []

    for root, dirs, files in os.walk(base_directory):
        for filename in files:
            if filename.lower().endswith(".log"):
                log_path = os.path.join(root, filename)
                data = {
                    "filename": filename,
                    "content_file": None,
                    "runtime": None
                }

                with open(log_path, 'r', encoding='latin-1') as f:
                    for line in f:
                        content_match = re.search(content_pattern, line)
                        if content_match:
                            data["content_file"] = content_match.group(1)

                        time_match = re.search(time_pattern, line)
                        if time_match:
                            data["runtime"] = time_match.group(1)

                results.append(data)

    logging.info(
        f"Loaded {len(results)} records from log files."
    )

    return pd.DataFrame(results)

