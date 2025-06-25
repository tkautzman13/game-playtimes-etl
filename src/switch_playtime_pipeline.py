from src.switch_data_retrieval.exophase_scraper import scrape_switch_playtimes
from src.switch_data_retrieval.exophase_parser import process_switch_playtimes
from src.utils import setup_logger, load_config

logger = setup_logger(name='switch_playtime_pipeline')

pipeline_config = load_config("config.yaml")

# Scrape switch playtimes from Exophase
scrape_switch_playtimes(
    username=pipeline_config['exophase_credentials']['username'],
    password=pipeline_config['exophase_credentials']['password'],
    output_path=pipeline_config['html_extract_path'],
)

# Parse the scraped HTML file and save the data to CSV
process_switch_playtimes(
    html_file_path=pipeline_config['html_extract_path'],
    base_output_path=pipeline_config['csv_data']['switch_daily_playtime_raw_path'],
    csv_filename="switch_daily_playtime.csv"
)