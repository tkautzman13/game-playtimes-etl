import sys
import os
from pathlib import Path
import logging
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.nintendo_switch_playtime.exophase_scraper import scrape_switch_playtimes
from src.nintendo_switch_playtime.exophase_parser import process_switch_playtimes
from src.nintendo_switch_playtime.pipeline_utils import load_all_extract_files, calculate_playtime_deltas
from src.utils import load_config, ensure_directories_exist


def switch_playtime_pipeline(config, skip_exophase_scraping=False):
    
    logging.info("="*90)
    logging.info('Beginning switch playtime data pipeline')
    logging.info("="*90)

    # File/directory paths
    html_directory = config['data']['switch_html_extract_path']
    csv_raw_directory = config['data']['switch_raw_path']
    output_file = os.path.join(
        config['data']['switch_processed_path'], 
        "switch_daily_playtimes.csv"
    )

    if not skip_exophase_scraping:
        # Scrape switch playtimes from Exophase
        scrape_switch_playtimes(
            username=config['exophase']['username'],
            password=config['exophase']['password'],
            output_path=config['data']['switch_html_extract_path'],
            url=config['exophase']['url'],
        )

        # Parse the scraped HTML file and save the data to CSV
        process_switch_playtimes(
            html_file_path=html_directory,
            base_output_path=csv_raw_directory
        )
    else:
        logging.info("Skipping Exophase scraping as per configuration.")

    # Load all extracted CSV files and calculate daily playtime deltas
    combined_df = load_all_extract_files(csv_raw_directory)
    daily_playtime_df = calculate_playtime_deltas(combined_df)

    # Save the processed daily playtime data to a CSV file
    logging.info(f"Saving processed daily playtime data to {output_file}...")
    daily_playtime_df.to_csv(f"{output_file}", index=False)
    logging.info(f"Total records loaded: {len(daily_playtime_df)}")
    logging.info(f"Processed daily playtime data saved to {output_file}")
    logging.info('COMPLETE: Switch playtime data pipeline has finished')


if __name__ == "__main__":
    log_filename = f"logs/switch_playtime_pipeline_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

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

    switch_playtime_pipeline(config)
