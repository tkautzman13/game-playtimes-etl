import sys
import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.retroarch_playtime.pipeline_utils import load_log_data, parse_time_to_seconds, extract_rom_title
from src.utils import load_config, ensure_directories_exist


def retroarch_playtime_pipeline(config) -> pd.DataFrame:

    logging.info("="*90)
    logging.info('Beginning Retroarch playtime data pipeline')
    logging.info("="*90)

    # File/directory paths
    raw_dir = config['data']['retroarch_raw_path']
    output_file = os.path.join(
        config['data']['retroarch_processed_path'],
        'retroarch_daily_playtimes.csv'
    )

    # Load and process Retroarch log data
    logs_df = load_log_data(raw_dir)
    logs_df['runtime_seconds'] = logs_df['runtime'].apply(parse_time_to_seconds)
    logs_df = logs_df[logs_df['runtime_seconds'] > 30] # Exclude short sessions
    logs_df['name'] = logs_df['content_file'].apply(extract_rom_title)

    # Extract date from filename
    logs_df['date'] = logs_df['filename'].str.extract(r"(\d{4}_\d{2}_\d{2})")
    logs_df['date'] = logs_df["date"].str.replace("_", "-", regex=False)
    logs_df['date'] = pd.to_datetime(logs_df['date'], format='%Y-%m-%d')

    # Calculate playtime in minutes and aggregate by date and game name
    logs_df['playtime_mins'] = round(logs_df['runtime_seconds'] / 60, 0)
    logs_df = logs_df[['date', 'name', 'playtime_mins']]
    logs_df = logs_df.groupby(['date', 'name'], as_index=False).agg({'playtime_mins': 'sum'})

    # Save the processed playtime data to a CSV file
    logging.info(f"Saving processed daily playtime data to {output_file}...")
    logs_df.to_csv(output_file, index=False)
    logging.info(f"Total records loaded: {len(logs_df)}")
    logging.info(f"Processed playtime data saved to {output_file}")

    logging.info('COMPLETE: Retroarch playtime data pipeline has finished')


if __name__ == "__main__":
    log_filename = (
        f"logs/playnite_playtime_pipeline_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO, 
        filename=log_filename, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Load pipeline configuration
    config = load_config("config.yaml")

    # Ensure directories found in config exist
    directories = [
        value for key, value in config['data'].items()
        if isinstance(value, str) and value.endswith('/')
    ]

    ensure_directories_exist(directories)

    retroarch_playtime_pipeline(config)