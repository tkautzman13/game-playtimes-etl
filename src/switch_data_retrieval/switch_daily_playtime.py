import pandas as pd
import glob
import os
from datetime import datetime, timedelta
import re


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
    search_pattern = os.path.join(base_directory, "**", "*.csv")
    csv_files = glob.glob(search_pattern, recursive=True)

    if not csv_files:
        raise ValueError(f"No CSV files found in {base_directory}")

    dataframes = []

    for file_path in csv_files:
        try:
            # Load CSV
            df = pd.read_csv(file_path)

            # Convert extract_date to datetime if it's not already
            df["extract_date"] = pd.to_datetime(df["extract_date"])

            # Convert time_played to minutes
            df["time_played_mins"] = df["time_played"].apply(parse_time_to_minutes)

            dataframes.append(df)

        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    if not dataframes:
        raise ValueError("No files could be loaded successfully")

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"Loaded {len(dataframes)} files from {base_directory}")
    print(f"Total records loaded: {len(combined_df)}")

    return combined_df


def calculate_playtime_deltas(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily playtime deltas using group operations

    Parameters:
        input_df (pd.DataFrame): Combined dataframe with extract data including time_played_mins and extract_date columns

    Returns:
        pd.DataFrame: Processed dataframe with columns: date, name, playtime_mins, platform, days_gap, multi_day_flag
    """
    # Create a copy to avoid modifying original
    df = input_df.copy()

    # Sort by game, platform, and extract_date
    df = df.sort_values(["game_name", "platform", "extract_date"])

    # Group by game and platform
    grouped = df.groupby(["game_name", "platform"])

    # Calculate previous playtime within each group
    df["prev_time_played_mins"] = grouped["time_played_mins"].shift(1)
    df["prev_extract_date"] = grouped["extract_date"].shift(1)

    # Calculate delta (current - previous)
    df["delta_mins"] = df["time_played_mins"] - df["prev_time_played_mins"]

    # Calculate days between extracts for flagging
    df["days_between_extracts"] = (df["extract_date"] - df["prev_extract_date"]).dt.days

    # Filter to only rows with valid deltas (not first occurrence)
    valid_deltas = df[df["prev_time_played_mins"].notna()].copy()

    # Filter to only positive deltas (actual playtime)
    positive_deltas = valid_deltas[valid_deltas["delta_mins"] > 0].copy()

    # Create the processed dataset
    processed_df = pd.DataFrame(
        {
            "date": positive_deltas[
                "prev_extract_date"
            ].dt.date,  # Play date = previous extract_date
            "name": positive_deltas["game_name"],
            "playtime_mins": positive_deltas["delta_mins"],
            "platform": positive_deltas["platform"],
            "days_gap": positive_deltas["days_between_extracts"],
            "multi_day_flag": positive_deltas["days_between_extracts"] > 1,
        }
    )

    # Reset index
    processed_df = processed_df.reset_index(drop=True)

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
    # Load all extract files
    combined_df = load_all_extract_files(directory_path)

    # Calculate deltas and create final DataFrame
    daily_playtime_df = calculate_playtime_deltas(combined_df)

    # Save to output path
    daily_playtime_df.to_csv(f"{output_path}/{output_filename}", index=False)

    print(f"Processed daily playtime data saved to {output_path}")
