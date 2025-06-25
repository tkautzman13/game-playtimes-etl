from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.utils import create_date_folder_path, get_logger


def load_latest_html_file(
    path: str
) -> pd.DataFrame:
    """
    Loads the most recent HTML file from a specified folder based on file modification time.

    Parameters:
    -----------
    path : str
        Path to the folder containing HTML files.

    Returns:
    --------
    pd.DataFrame
        The most recently created HTML source file.
    """
    logger = get_logger()

    # List all CSV files in the folder
    files = list(Path(path).rglob("*.html"))

    # Filter and find the most recently modified file
    if files:
        most_recent_file = max(files, key=lambda f: f.stat().st_mtime)
        # Read the HTML file content
        with open(most_recent_file, 'r', encoding='utf-8') as file:
            html_content = file.read()
        logger.debug(f"Loaded most recent file: {most_recent_file.name}")
        return html_content
    else:
        raise FileNotFoundError("No HTML files found in the specified folder.")


def parse_html_data(html_content: str) -> pd.DataFrame:
    """
    Parse switch playtime data from scraped HTML content.

    Parameters:
        html_content (str): HTML content to parse

    Returns:
        pd.DataFrame: DataFrame containing game data
    """
    soup = BeautifulSoup(html_content, "html.parser")

    games = []
    hours_played = []
    last_played_dates = []
    platforms = []

    # Find all game list items
    for li in soup.select("ul.list-unordered-base li"):
        game_name_tag = li.select_one("h3 a")
        game_name = game_name_tag.text.strip() if game_name_tag else "N/A"

        playtime_tag = li.select_one("span.hours")
        playtime = playtime_tag.text.strip() if playtime_tag else "N/A"

        last_played_tag = li.select_one("div.lastplayed")
        last_played = last_played_tag.text.strip() if last_played_tag else "N/A"

        platform_tag = li.select_one("div.platforms span") or li.select_one(
            "div.nintendo-profile-pf span"
        )
        platform = platform_tag.text.strip() if platform_tag else "N/A"

        games.append(game_name)
        hours_played.append(playtime)
        last_played_dates.append(last_played)
        platforms.append(platform)

    # Create DataFrame
    df = pd.DataFrame(
        {
            "game_name": games,
            "time_played": hours_played,
            "last_played": last_played_dates,
            "platform": platforms,
            "extract_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    # Replace "N/A" with 0
    df.replace("N/A", 0, inplace=True)

    return df


def save_dataframe_to_csv(df: pd.DataFrame, folder_path: Path, filename: str) -> None:
    """
    Save DataFrame to CSV file in specified folder.

    Parameters:
        df (pd.DataFrame): DataFrame to save
        folder_path (Path): Path to the folder
        filename (str): Name of the CSV file
    """
    csv_file_path = folder_path / filename
    df.to_csv(csv_file_path, index=False)
    print(f"Data saved to: {csv_file_path}")


def process_switch_playtimes(
    html_file_path: str,
    base_output_path: str,
    csv_filename: str = "switch_daily_playtime.csv",
) -> pd.DataFrame:
    """
    Main function to process Exophase HTML and save to CSV.

    Parameters:
        html_file_path (str): Path to the HTML file
        base_output_path (str): Base path for output directory
        csv_filename (str): Name for the output CSV file

    Returns:
        pd.DataFrame: Processed game data
    """
    # Load HTML content
    html_content = load_latest_html_file(html_file_path)

    # Parse game data
    df = parse_html_data(html_content)

    # Create date-based folder structure
    date_folder = create_date_folder_path(base_output_path)

    # Save to CSV
    save_dataframe_to_csv(df, date_folder, csv_filename)