import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.switch_data_retrieval.exophase_scraper import scrape_switch_playtimes
from src.switch_data_retrieval.exophase_parser import process_switch_playtimes
from src.switch_data_retrieval.switch_daily_playtime import create_switch_daily_playtime_csv
from src.utils import setup_logger, load_config, ensure_directories_exist

def main():
    
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

        # Scrape switch playtimes from Exophase
        scrape_switch_playtimes(
            username=pipeline_config['exophase']['username'],
            password=pipeline_config['exophase']['password'],
            output_path=pipeline_config['data']['switch_html_extract_path'],
            url=pipeline_config['exophase']['url'],
        )

        # Parse the scraped HTML file and save the data to CSV
        process_switch_playtimes(
            html_file_path=pipeline_config['data']['switch_html_extract_path'],
            base_output_path=pipeline_config['data']['switch_raw_path']
        )

        # Create daily playtimes CSV file
        create_switch_daily_playtime_csv(
            directory_path=pipeline_config['data']['switch_raw_path'],
            output_path=pipeline_config['data']['switch_processed_path']
        )

        logger.info('COMPLETE: Switch playtime data pipeline has finished')

    except Exception as e:
        logger.exception('Pipeline failed with error')
        raise




if __name__ == "__main__":
    main()