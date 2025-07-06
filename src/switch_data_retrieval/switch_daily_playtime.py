import pandas as pd
import glob
import os
from datetime import datetime, timedelta
import re
from src.utils import get_logger


def parse_time_to_minutes(time_str: str) -> int:
    """
    Convert time string like '11h 45m' to total minutes

    Parameters:
        time_str (str): Time string in format like '11h 45m', '2h', '30m', or empty/NaN

    Returns:
        int: Total time in minutes
    """
    if pd.isna(time_str) or time_str == "":
        return 0

    hours = 0
    minutes = 0

    # Extract hours
    hour_match = re.search(r"(\d+)h", str(time_str))
    if hour_match:
        hours = int(hour_match.group(1))

    # Extract minutes
    min_match = re.search(r"(\d+)m", str(time_str))
    if min_match:
        minutes = int(min_match.group(1))

    return hours * 60 + minutes


def load_all_extract_files(base_directory: str) -> pd.DataFrame:
    """
    Load all CSV files from directory and combine into single dataframe

    Parameters:
        base_directory (str): Path to the base directory containing CSV files (searches recursively)

    Returns:
        pd.DataFrame: Combined dataframe with all CSV data, including converted time_played_mins column
    """
    logger = get_logger('switch_playtime_pipeline')

    search_pattern = os.path.join(base_directory, "**", "*.csv")
    csv_files = glob.glob(search_pattern, recursive=True)

    if not csv_files:
        raise ValueError(f"No CSV files found in {base_directory}")
    
    # Group files by their immediate parent directory (lowest-level subfolder)
    files_by_folder = {}
    for file_path in csv_files:
        parent_dir = os.path.dirname(file_path)
        if parent_dir not in files_by_folder:
            files_by_folder[parent_dir] = []
        files_by_folder[parent_dir].append(file_path)
    
    # Select one file per folder
    selected_files = []
    for folder_path, files in files_by_folder.items():
        folder_name = os.path.basename(folder_path)
        if len(files) >= 1:
            files_with_creation_time = []
            for file_path in files:
                # Get file creation times
                creation_time = os.path.getctime(file_path)
                files_with_creation_time.append((file_path, creation_time))
            
            files_with_creation_time.sort(key=lambda x: x[1])

            # Select the first valid created file
            selected_file = files_with_creation_time[0][0] 
            logger.debug(f"Selected second-created file for folder {folder_name}: {os.path.basename(selected_file)}")
            selected_files.append(selected_file)
        else:
            logger.warning(f"No files found for folder {folder_name}")
 
    if not selected_files:
        raise ValueError("No valid files selected after applying date selection rules")
    
    logger.info(f"Selected {len(selected_files)} files (one per day) from {len(files_by_folder)} date directories")

    dataframes = []

    for file_path in csv_files:
        try:
            # Load CSV
            df = pd.read_csv(file_path)

            # Convert extract_date to datetime if it's not already
            df["extract_date"] = pd.to_datetime(df["extract_date"])

            # Convert time_played to minutes
            df["time_played_mins"] = df["time_played"].apply(parse_time_to_minutes)
            logger.debug(f"Successfully loaded {file_path} with {len(df)} records")

            dataframes.append(df)

        except Exception as e:
            logger.exception(f"Error loading {file_path}: {e}")
            continue

    if not dataframes:
        logger.error("No valid dataframes loaded from CSV files")
        raise ValueError("No files could be loaded successfully")

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Loaded {len(dataframes)} files from {base_directory}")
    logger.info(f"Total records loaded: {len(combined_df)}")

    return combined_df


def calculate_playtime_deltas(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily playtime deltas using group operations

    Parameters:
        input_df (pd.DataFrame): Combined dataframe with extract data including time_played_mins and extract_date columns

    Returns:
        pd.DataFrame: Processed dataframe with columns: date, name, playtime_mins, platform, days_gap, multi_day_flag
    """
    logger= get_logger('switch_playtime_pipeline')

    # Create a copy to avoid modifying original
    df = input_df.copy()

    # Sort by game, platform, and extract_date
    df = df.sort_values(["game_name", "platform", "extract_date"])

    # Group by game and platform
    grouped = df.groupby(["game_name", "platform"])

    # Calculate previous playtime within each group
    df["prev_time_played_mins"] = grouped["time_played_mins"].shift(1)

    # If previous playtime is NaN, set it to 0
    df["prev_time_played_mins"] = df["prev_time_played_mins"].fillna(0)

    # Calculate delta (current - previous)
    df["delta_mins"] = df["time_played_mins"] - df["prev_time_played_mins"]

    # Filter to only positive playtime deltas (actual playtime)
    positive_deltas = df[df["delta_mins"] > 0].copy()

    # Create a DataFrame for unique extract dates and add previous extract date
    extract_dates = pd.DataFrame(df["extract_date"].unique(), columns=['extract_date'])
    extract_dates['prev_extract_date'] = extract_dates['extract_date'].shift(1)

    # Calculate extract date gaps and multi_day_flag
    extract_dates["days_between_extracts"] = (extract_dates["extract_date"] - extract_dates["prev_extract_date"]).dt.days
    extract_dates["multi_day_flag"] = extract_dates["days_between_extracts"] > 1

    # Log extract dates with a mutli_day_flag
    if not extract_dates[extract_dates['multi_day_flag']].empty:
        logger.warning(f"One or more extract dates contains a gap:\n{extract_dates[extract_dates['multi_day_flag']]}")
        logger.warning(f"If no games were played during the gap, it is recommended to copy the previous extract file to fill the gap.")

    # Merge with extract dates (excluding the first one) to get previous extract dates and multi_day_flags
    positive_deltas = positive_deltas.merge(
        extract_dates[extract_dates["prev_extract_date"].notna()],
        left_on="extract_date",
        right_on="extract_date",
        how="inner"
    )

    # Create the processed dataset
    processed_df = pd.DataFrame(
        {
            "date": positive_deltas[
                "prev_extract_date"
            ].dt.date,  # Play date = previous extract_date
            "name": positive_deltas["game_name"],
            "playtime_mins": positive_deltas["delta_mins"],
            "platform": positive_deltas["platform"],
            "extract_date_gap_flag": positive_deltas["days_between_extracts"] > 1,
        }
    )

    # Reset index
    processed_df = processed_df.reset_index(drop=True)

    # Sort values by date
    processed_df = processed_df.sort_values(by=["date"])

    return processed_df


def create_switch_daily_playtime_csv(
    directory_path: str,
    output_path: str,
    output_filename: str = "switch_daily_playtimes.csv",
) -> None:
    """
    Main function to create daily playtime DataFrame from all CSV files in directory.

    Parameters:
        directory_path (str): Path to the directory containing CSV files
        output_path (str): Path where the processed DataFrame will be saved
        output_filename (str): Name of the output CSV file (default: "switch_daily_playtimes.csv")

    """
    logger = get_logger('switch_playtime_pipeline')
    logger.info("Beginning daily playtime data processing")

    # Load all extract files
    logger.info(f"Loading all CSV files from {directory_path}...")
    combined_df = load_all_extract_files(directory_path)

    # Calculate deltas and create final DataFrame
    logger.info("Calculating daily playtime deltas...")
    daily_playtime_df = calculate_playtime_deltas(combined_df)

    # Save to output path
    logger.info(f"Saving processed daily playtime data to {output_path}/{output_filename}...")
    daily_playtime_df.to_csv(f"{output_path}/{output_filename}", index=False)
    logger.info(f"Total records loaded: {len(daily_playtime_df)}")

    logger.info(f"COMPLETE: Processed daily playtime data saved to {output_path}")
