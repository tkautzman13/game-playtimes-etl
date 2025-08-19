# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #
import sys
import os
from pathlib import Path
import logging
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.playnite_playtime.pipeline import playnite_playtime_pipeline
from src.nintendo_switch_playtime.pipeline import switch_playtime_pipeline
from src.retroarch_playtime.pipeline import retroarch_playtime_pipeline
from src.combined_daily_playtime.pipeline import combined_playtime_pipeline
from src.utils import load_config, ensure_directories_exist

log_filename = f"logs/daily_playtime_orchestrator_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(level=logging.INFO, filename=log_filename, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------------------------------------------------------------- #
#                     Daily Playtime Orchestrator Function                     #
# ---------------------------------------------------------------------------- #

def daily_playtime_orchestrator():

    logging.info('Beginning daily playtime orchestration')

    # Load pipeline configuration
    config = load_config("config.yaml")

    # Ensure directories found in config exist
    directories = [
        value for key, value in config['data'].items()
        if isinstance(value, str) and value.endswith('/')
    ]

    ensure_directories_exist(directories)

    # 1. Run Switch playtime pipeline
    switch_playtime_pipeline(config)

    # 2. Run Playnite playtime pipeline
    playnite_playtime_pipeline(config)

    # 3. Run Retroarch playtime pipeline
    retroarch_playtime_pipeline(config)

    # 4. Combine the processed playtime datasets
    combined_playtime_pipeline(config)

    logging.info('Orchestration complete: All daily playtime pipelines have finished successfully')


if __name__ == "__main__":
    daily_playtime_orchestrator()