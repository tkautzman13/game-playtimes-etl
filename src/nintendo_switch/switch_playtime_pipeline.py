import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.nintendo_switch.exophase_scraper import scrape_switch_playtimes
from src.nintendo_switch.exophase_parser import process_switch_playtimes
from src.nintendo_switch.pipeline_utils import load_all_extract_files, calculate_playtime_deltas
from src.utils import setup_logger, load_config, ensure_directories_exist

def switch_playtime_pipeline(skip_exophase_scraping=False):
    
    logger = setup_logger(name='switch_playtime_pipeline')
    logger.info('Beginning switch playtime data pipeline')

    try:
        # Load pipeline configuration
        pipeline_config = load_config("config.yaml")

        # Ensure directories found in config exist
        directories = [
            value for key, value in pipeline_config['data'].items()
            if isinstance(value, str) and value.endswith('/')
        ]

        ensure_directories_exist(directories)

        # File/directory paths
        html_directory = pipeline_config['data']['switch_html_extract_path']
        csv_raw_directory = pipeline_config['data']['switch_raw_path']
        output_file = pipeline_config['data']['switch_processed_path'] + "switch_daily_playtimes.csv"

        if not skip_exophase_scraping:
            # Scrape switch playtimes from Exophase
            scrape_switch_playtimes(
                username=pipeline_config['exophase']['username'],
                password=pipeline_config['exophase']['password'],
                output_path=pipeline_config['data']['switch_html_extract_path'],
                url=pipeline_config['exophase']['url'],
            )

            # Parse the scraped HTML file and save the data to CSV
            process_switch_playtimes(
                html_file_path=html_directory,
                base_output_path=csv_raw_directory
            )

        logger.info("Beginning daily playtime data processing")

        logger.info(f"Loading all CSV files from {csv_raw_directory}...")
        combined_df = load_all_extract_files(csv_raw_directory)

        logger.info("Calculating daily playtime deltas...")
        daily_playtime_df = calculate_playtime_deltas(combined_df)

        logger.info(f"Saving processed daily playtime data to {output_file}...")
        daily_playtime_df.to_csv(f"{output_file}", index=False)
        logger.info(f"Total records loaded: {len(daily_playtime_df)}")

        logger.info(f"COMPLETE: Processed daily playtime data saved to {output_file}")

        logger.info('COMPLETE: Switch playtime data pipeline has finished')

    except Exception as e:
        logger.exception('Pipeline failed with error')
        raise


if __name__ == "__main__":
    switch_playtime_pipeline()
