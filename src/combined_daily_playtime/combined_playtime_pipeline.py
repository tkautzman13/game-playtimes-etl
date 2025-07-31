import sys
import os
from pathlib import Path
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.combined_daily_playtime.pipeline_utils import check_for_matching_extract_dates, switch_playtime_library_fuzzy_matching, combine_daily_playtime
from src.utils import setup_logger, load_config, ensure_directories_exist

def combined_playtime_pipeline():
    
    logger = setup_logger(name='combined_playtime_pipeline')
    logger.info('Beginning combined playtime data pipeline')

    try:
        # Load pipeline configuration
        pipeline_config = load_config("config.yaml")

        # Ensure directories found in config exist
        directories = [
            value for key, value in pipeline_config['data'].items()
            if isinstance(value, str) and value.endswith('/')
        ]

        ensure_directories_exist(directories)

        # Define file paths
        switch_raw_path = pipeline_config['data']['switch_raw_path']
        playnite_raw_path = pipeline_config['data']['playnite_raw_path']
        switch_playtime_file = pipeline_config['data']['switch_processed_path'] + 'switch_daily_playtimes.csv'
        playnite_playtime_file = pipeline_config['data']['playnite_processed_path'] + 'playnite_daily_playtimes.csv'
        library_metadata_file = pipeline_config['data']['playnite_library_igdb_file']
        output_path = pipeline_config['data']['combined_playtime_path']

        # Ensure Switch and Playnite Extract dates match
        if not check_for_matching_extract_dates(switch_raw_path, playnite_raw_path):
            logger.error('Extract dates do not match between Switch and Playnite data. Exiting pipeline.')
            raise ValueError('Latest extract dates do not match between Switch and Playnite data. Check the latest files in the raw data directories.')

        # Fuzzy match Switch titles to Playnite library
        matched_switch_playtime_df = switch_playtime_library_fuzzy_matching(switch_playtime_file, library_metadata_file)

        # Load Playnite playtime data
        playnite_playtime_df = pd.read_csv(playnite_playtime_file)

        # Combine the matched Switch playtime with Playnite playtime
        combined_daily_playtime_df = combine_daily_playtime(matched_switch_playtime_df, playnite_playtime_df)
        combined_daily_playtime_df = combined_daily_playtime_df.sort_values(by=['date']).reset_index(drop=True)

        # Save the combined DataFrame to CSV
        combined_daily_playtime_df.to_csv(f'{output_path}daily_playtimes.csv', index=False)

        logger.info(f'COMPLETE: combined daily playtime data pipeline has finished. File saved to {output_path}daily_playtimes.csv')

    except Exception as e:
        logger.exception('Pipeline failed with error')
        raise


if __name__ == "__main__":
    combined_playtime_pipeline()