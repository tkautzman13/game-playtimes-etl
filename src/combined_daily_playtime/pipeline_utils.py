import pandas as pd
import os
import glob
import logging
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

    # Ensure the directory exists
    if not os.path.exists(directory):
        logging.error(f"Directory does not exist: {directory}")
    else:
        search_pattern = os.path.join(directory, "**", "*.csv")
        csv_files = glob.glob(search_pattern, recursive=True)

    # Grab the most recent CSV files from and its extract date
    if not csv_files:
        logging.error(f"No CSV files found in directory: {directory}")
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
    

    logging.info("Checking if latest extract dates match between Switch and Playnite data...")

    # Collect the latest extract dates from both directories
    switch_extract_date = collect_latest_extract_date(switch_extract_directory, 'extract_date')
    playnite_extract_date = collect_latest_extract_date(playnite_extract_directory, 'ExportDate', skip_first_row=True)

    if switch_extract_date == playnite_extract_date:
        logging.info("Extract dates match.")
        return True
    else:
        logging.warning("Extract dates do not match.")
        return False


def playtime_library_fuzzy_matching(
    playtime_file:str, library_metadata_file:str, platform:str, threshold:int=80
):
    """
    Fuzzy match game titles from the games within playtime data to Playnite library data.

    Parameters:
        playtime_file (str): CSV containing playtime data with 'name' column.
        library_metadata_file (str): CSV containing Playnite library data with 'name' column.
        platform (str): Platform of the playtime data (either 'Switch' or 'Emulator').
        threshold (int): Minimum similarity score for a match (default is 80).

    Returns:
        pd.DataFrame: DataFrame with matched titles and their corresponding Game IDs.
    """
    

    logging.info(
        f"Beginning fuzzy matching of {platform} playtimes with Playnite library..."
    )

    # Load CSV data
    playtime_df = pd.read_csv(playtime_file)
    library_df = pd.read_csv(library_metadata_file)

    if platform == 'Switch':
        # Filter library data to only include Switch and Switch 2 titles
        filtered_library_df = library_df[
            library_df["platforms"].isin(["Nintendo Switch", "Nintendo Switch 2"])
        ]
    elif platform == 'Emulator':
        # Filter library data to only include Emulator titles
        filtered_library_df = library_df[library_df["platforms"].isin([
                "Nintendo Game Boy",
                "Nintendo Game Boy Color",
                "Nintendo Game Boy Advance",
                "Nintendo DS",
                "Nintendo 3DS",
                "Nintendo NES",
                "Nintendo SNES",
                "Nintendo 64",
                "Nintendo Gamecube",
                "Sony PlayStation",
                "Sony PlayStation 2"
            ])
]
    else:
        raise ValueError(
            f"Unsupported platform: {platform}. Supported platforms are 'Switch' and 'Emulator'."
        )

    # Create a mapping of titles to their IDs
    library_dict = dict(zip(filtered_library_df["name"], filtered_library_df["id"]))

    # Function to find the best match for a title
    def find_best_match(name):
        best_match, score = process.extractOne(
            name, library_dict.keys(), scorer=fuzz.ratio
        )
        if score >= threshold:
            return library_dict[best_match]
        return None

    playtime_df["id"] = playtime_df["name"].apply(find_best_match)

    # Filter out rows where no match was found and log unmatched titles
    unmatched_playtime_df = playtime_df[playtime_df["id"].isnull()]
    if not unmatched_playtime_df.empty:
        unmatched_titles = unmatched_playtime_df["name"].drop_duplicates()
        logging.warning(
            f"Fuzzy matching found {len(unmatched_titles)} unmatched {platform} titles in playtime data."
        )
        logging.warning(
            f"Unmatched {platform} titles:\n{unmatched_titles}"
        )

    matched_playtime_df = playtime_df[playtime_df["id"].notnull()]

    if platform == 'Switch':
        # Drop platforms field from the Switch playtime DataFrame
        matched_playtime_df = matched_playtime_df.drop(columns=["platform"])

    logging.info(f"Fuzzy matching complete. {matched_playtime_df['name'].nunique()} matches found.")

    return matched_playtime_df


def combine_daily_playtime(dataframes_list:list) -> pd.DataFrame:
    """
    Union a list of playtime dataframes into a single combined dataframe.

    Parameters:
        dataframes_list (list): List of DataFrames containing daily playtime data.

    Returns:
        pd.DataFrame: Combined DataFrame with daily playtime data.
    """
    

    logging.info("Creating combined daily palytime dataset...")

    # Union the two DataFrames
    combined_df = pd.concat(
        dataframes_list, ignore_index=True
    )

    logging.info(f"Combined DataFrame created.")

    return combined_df
