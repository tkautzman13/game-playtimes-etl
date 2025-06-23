from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path

# Load the saved HTML file
with open("C:/Users/tk231/Desktop/Exophase Scraping Testing/profile.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

games = []
hours_played = []
last_played_dates = []
platforms = []

# Find all game list items
for li in soup.select("ul.list-unordered-base li"):
    # Extract game name
    game_name_tag = li.select_one("h3 a")
    game_name = game_name_tag.text.strip() if game_name_tag else "N/A"
    
    # Extract playtime text
    playtime_tag = li.select_one("span.hours")
    playtime = playtime_tag.text.strip() if playtime_tag else "N/A"
    
    # Extract last played date
    last_played_tag = li.select_one("div.lastplayed")
    last_played = last_played_tag.text.strip() if last_played_tag else "N/A"
    
    # Extract platform
    platform_tag = li.select_one("div.platforms span") or li.select_one("div.nintendo-profile-pf span")
    platform = platform_tag.text.strip() if platform_tag else "N/A"
    
    games.append(game_name)
    hours_played.append(playtime)
    last_played_dates.append(last_played)
    platforms.append(platform)

# Create a DataFrame
df = pd.DataFrame({
    "game_name": games,
    "time_played": hours_played,
    "last_played": last_played_dates,
    "platform": platforms,
    "extract_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
})

# Replace "N/A" with 0
df.replace("N/A", 0, inplace=True)

# Get current date
current_date = datetime.now()

# Extract date components
year = current_date.strftime("%Y")
month = current_date.strftime("%m")
day = current_date.strftime("%d")

# Build the folder path
base_path = "C:/Users/tk231/OneDrive/Documents/Video Games/Playtimes - Testing/Switch"
date_folder = Path(base_path) / year / month / day

# Create the folder structure if it doesn't exist
date_folder.mkdir(parents=True, exist_ok=True)

# Build the full CSV file path
csv_file_path = date_folder / "Switch Playtime.csv"

# Write the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)