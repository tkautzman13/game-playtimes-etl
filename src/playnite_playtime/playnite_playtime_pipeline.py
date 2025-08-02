import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.playnite_playtime.pipeline_utils import load_all_extract_files, calculate_playtime_deltas, filter_playnite_playtime_data
from src.utils import setup_logger, load_config, ensure_directories_exist

def playnite_playtime_pipeline():
    
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

        # File/directory paths
        directory_path = pipeline_config['data']['playnite_raw_path']
        output_file = pipeline_config['data']['playnite_processed_path'] + "playnite_daily_playtimes.csv"

        logger.info("Beginning daily playtime data processing")

        logger.info(f"Loading all CSV files from {directory_path}...")
        combined_df = load_all_extract_files(directory_path)

        logger.info("Filtering combined playtime data...")
        combined_df = filter_playnite_playtime_data(combined_df)

        logger.info("Calculating daily playtime deltas...")
        daily_playtime_df = calculate_playtime_deltas(combined_df)

        logger.info(f"Saving processed daily playtime data to {output_file}...")
        daily_playtime_df.to_csv(f"{output_file}", index=False)
        logger.info(f"Total records loaded: {len(daily_playtime_df)}")

        logger.info(f"COMPLETE: Processed daily playtime data saved to {output_file}")

        logger.info('COMPLETE: Playnite playtime data pipeline has finished')

    except Exception as e:
        logger.exception('Pipeline failed with error')
        raise


if __name__ == "__main__":
    playnite_playtime_pipeline()
    