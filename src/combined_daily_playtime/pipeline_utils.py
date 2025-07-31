import pandas as pd
import os
import glob
from src.utils import get_logger
from fuzzywuzzy import fuzz, process

def collect_latest_extract_date(directory, extract_date_column, skip_first_row=False):
    """
    Collect the latest extract date from CSV files in a directory.

    Parameters:
        directory (str): Path to the directory containing CSV files.
        extract_date_column (str): Name of the column containing the extract date.
    Returns:
        datetime.date: The latest extract date found in the CSV files.
    """

    logger = get_logger("combined_playtime_pipeline")

    # Ensure the directory exists
    if not os.path.exists(directory):
        logger.error(f"Directory does not exist: {directory}")
    else:
        search_pattern = os.path.join(directory, "**", "*.csv")
        csv_files = glob.glob(search_pattern, recursive=True)

    # Grab the most recent CSV files from and its extract date
    if not csv_files:
        logger.error(f"No CSV files found in directory: {directory}")
    else:
        most_recent_file = max(csv_files, key=os.path.getctime)       
        # If skip_first_row is True, the first row is not relevant for the latest extract date (This is the case for Playnite data)
        if skip_first_row:
            latest_extract_date = pd.to_datetime(pd.read_csv(most_recent_file, skiprows=1, header=0)[extract_date_column]).dt.date.max()
        else:
            latest_extract_date = pd.to_datetime(pd.read_csv(most_recent_file)[extract_date_column]).dt.date.max() 

    return latest_extract_date


def check_for_matching_extract_dates(switch_extract_directory, playnite_extract_directory):
    """
    Check if the extract dates in the Switch and Playnite directories match.

    Parameters:
        switch_directory (str): Path to the Switch playtime data extracts directory.
        playnite_directory (str): Path to the Playnite playtime data extracts directory.

    Returns:
        bool: True if dates match, False otherwise.
    """
    logger = get_logger("combined_playtime_pipeline")

    logger.info("Checking if latest extract dates match between Switch and Playnite data...")

    # Collect the latest extract dates from both directories
    switch_extract_date = collect_latest_extract_date(switch_extract_directory, 'extract_date')
    playnite_extract_date = collect_latest_extract_date(playnite_extract_directory, 'ExportDate', skip_first_row=True)

    if switch_extract_date == playnite_extract_date:
        logger.info("Extract dates match.")
        return True
    else:
        logger.warning("Extract dates do not match.")
        return False


def switch_playtime_library_fuzzy_matching(
    switch_playtime_file, library_metadata_file, threshold=80
):
    """
    Fuzzy match game titles from the Switch playtime data to Playnite library data.

    Parameters:
        switch_df (pd.DataFrame): DataFrame containing Switch playtime data with 'name' column.
        library_igdb_df (pd.DataFrame): DataFrame containing Playnite library data with 'name' column.
        threshold (int): Minimum similarity score for a match (default is 80).

    Returns:
        pd.DataFrame: DataFrame with matched titles and their corresponding Game IDs.
    """
    logger = get_logger("combined_playtime_pipeline")

    logger.info(
        "Starting fuzzy matching of Switch playtime data with Playnite library..."
    )

    # Load CSV data
    switch_df = pd.read_csv(switch_playtime_file)
    library_df = pd.read_csv(library_metadata_file)

    # Filter library data to only include Switch and Switch 2 titles
    switch_library_df = library_df[
        library_df["platforms"].isin(["Nintendo Switch", "Nintendo Switch 2"])
    ]

    # Create a mapping of titles to their IDs
    switch_library_dict = dict(zip(switch_library_df["name"], switch_library_df["id"]))

    # Function to find the best match for a title
    def find_best_match(name):
        best_match, score = process.extractOne(
            name, switch_library_dict.keys(), scorer=fuzz.ratio
        )
        if score >= threshold:
            return switch_library_dict[best_match]
        return None

    # Apply fuzzy matching to the Switch DataFrame
    switch_df["id"] = switch_df["name"].apply(find_best_match)

    # Filter out rows where no match was found and log unmatched titles
    unmatched_switch_df = switch_df[switch_df["id"].isnull()]
    if not unmatched_switch_df.empty:
        logger.warning(
            f"Fuzzy matching found {len(unmatched_switch_df)} unmatched titles in Switch data."
        )
        logger.warning(
            "Unmatched titles:\n" + unmatched_switch_df[["name"]].to_string(index=False)
        )

    matched_switch_df = switch_df[switch_df["id"].notnull()]

    logger.info(f"Fuzzy matching complete. {len(matched_switch_df)} matches found.")

    return matched_switch_df


def combine_daily_playtime(matched_switch_playtime_df, playnite_playtime_df):
    """
    Combine daily Switch and PC (Playnite) playtime data.

    Parameters:
        matched_switch_playtime_df (pd.DataFrame): DataFrame containing matched Switch playtime data.
        playnite_playtime_df (pd.DataFrame): DataFrame containing Playnite playtime data.

    Returns:
        pd.DataFrame: Combined DataFrame with daily playtime data.
    """
    logger = get_logger("combined_playtime_pipeline")

    logger.info("Creating combined daily palytime dataset...")

    # Drop platforms field from the Switch playtime DataFrame
    matched_switch_playtime_df = matched_switch_playtime_df.drop(columns=["platform"])

    # Union the two DataFrames
    combined_df = pd.concat(
        [matched_switch_playtime_df, playnite_playtime_df], ignore_index=True
    )

    logger.info(f"Combined DataFrame created with {len(combined_df)} rows.")

    return combined_df
