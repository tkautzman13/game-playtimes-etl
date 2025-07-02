import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.playnite_daily_playtime import create_playnite_daily_playtime_csv
from src.utils import setup_logger, load_config, ensure_directories_exist

def main():
    
    logger = setup_logger(name='playnite_playtime_pipeline')
    logger.info('Beginning playnite playtime data pipeline')

    try:
        # Load pipeline configuration
        pipeline_config = load_config("config.yaml")

        # Ensure directories found in config exist
        directories = [
            value for key, value in pipeline_config['data'].items()
            if isinstance(value, str) and value.endswith('/')
        ]

        ensure_directories_exist(directories)

        # Create daily playtimes CSV file
        create_playnite_daily_playtime_csv(
            directory_path=pipeline_config['data']['playnite_raw_path'],
            output_path=pipeline_config['data']['playnite_processed_path']
        )

        logger.info('COMPLETE: Playnite playtime data pipeline has finished')

    except Exception as e:
        logger.exception('Pipeline failed with error')
        raise




if __name__ == "__main__":
    main()