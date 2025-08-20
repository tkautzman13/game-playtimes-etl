import sys
import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from combined_daily_playtime.pipeline_utils import (
    check_for_matching_extract_dates, playtime_library_fuzzy_matching, combine_daily_playtime
)
from utils import load_config, ensure_directories_exist

def combined_playtime_pipeline(config):

    logging.info("="*90)
    logging.info('Beginning combined playtime data pipeline')
    logging.info("="*90)

    # File/directory paths
    switch_raw_path = config['data']['switch_raw_path']
    playnite_raw_path = config['data']['playnite_raw_path']
    library_metadata_file = config['data']['playnite_library_igdb_file']
    switch_playtime_file = os.path.join(
        config['data']['switch_processed_path'],
        'switch_daily_playtimes.csv'
    )
    retroarch_playtime_file = os.path.join(
        config['data']['retroarch_processed_path'],
        'retroarch_daily_playtimes.csv'
    )
    playnite_playtime_file = os.path.join(
        config['data']['playnite_processed_path'],
        'playnite_daily_playtimes.csv'
    )
    output_file = os.path.join(
        config['data']['combined_playtime_path'],
        'daily_playtimes.csv'
    )

    # Ensure Switch and Playnite Extract dates match
    if not check_for_matching_extract_dates(switch_raw_path, playnite_raw_path):
        raise ValueError(
            'Latest extract dates do not match between Switch and Playnite data. ' \
            'Check the latest files in the raw data directories.'
        )

    # Fuzzy matching
    matched_switch_playtime_df = playtime_library_fuzzy_matching(
        switch_playtime_file, 
        library_metadata_file, 
        platform='Switch'
    )
    matched_retroarch_playtime_df = playtime_library_fuzzy_matching(
        retroarch_playtime_file, 
        library_metadata_file, 
        platform='Emulator'
    )

    # Load Playnite playtime data
    playnite_playtime_df = pd.read_csv(playnite_playtime_file)
    combined_daily_playtime_df = combine_daily_playtime([
        matched_switch_playtime_df, 
        matched_retroarch_playtime_df, 
        playnite_playtime_df
    ])
    combined_daily_playtime_df = combined_daily_playtime_df.sort_values(by=['date']).reset_index(drop=True)
    
    # Save the combined daily playtime data
    logging.info(f"Saving processed daily playtime data to {output_file}...") 
    combined_daily_playtime_df.to_csv(f'{output_file}', index=False)
    logging.info(f"Total records loaded: {len(combined_daily_playtime_df)}")
    logging.info(f"Processed daily playtime data saved to {output_file}")

    logging.info(f'COMPLETE: combined daily playtime data pipeline has finished.')


if __name__ == "__main__":
    log_filename = f"logs/playnite_playtime_pipeline_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

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

    combined_playtime_pipeline(config)