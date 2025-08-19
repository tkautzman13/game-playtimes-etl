import sys
import os
from pathlib import Path
import logging
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.playnite_playtime.pipeline_utils import load_all_extract_files, calculate_playtime_deltas, filter_playnite_playtime_data
from src.utils import load_config, ensure_directories_exist

def playnite_playtime_pipeline(config):

    logging.info("="*90)
    logging.info('Beginning playnite playtime data pipeline')
    logging.info("="*90)

    # File/directory paths
    directory_path = config['data']['playnite_raw_path']
    output_file = os.path.join(
        config['data']['playnite_processed_path'], 
        "playnite_daily_playtimes.csv"
    )

    # Load, filter, and calculate playtime deltas
    combined_df = load_all_extract_files(directory_path)
    combined_df = filter_playnite_playtime_data(combined_df)
    daily_playtime_df = calculate_playtime_deltas(combined_df)

    # Save the processed daily playtime data to a CSV file
    logging.info(f"Saving processed daily playtime data to {output_file}...")
    daily_playtime_df.to_csv(f"{output_file}", index=False)
    logging.info(f"Total records loaded: {len(daily_playtime_df)}")
    logging.info(f"Processed daily playtime data saved to {output_file}")

    logging.info('COMPLETE: Playnite playtime data pipeline has finished')



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

    playnite_playtime_pipeline(config)
    