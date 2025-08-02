import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.playnite_playtime.playnite_playtime_pipeline import playnite_playtime_pipeline
from src.nintendo_switch_playtime.switch_playtime_pipeline import switch_playtime_pipeline
from src.combined_daily_playtime.combined_playtime_pipeline import combined_playtime_pipeline

def daily_playtime_orchestrator():
    try:
        # 1. Run Switch playtime pipeline
        switch_playtime_pipeline(skip_exophase_scraping=False)

        # 2. Run Playnite playtime pipeline
        playnite_playtime_pipeline()

        # 3. Combine the processed playtime datasets
        combined_playtime_pipeline()

    except Exception as e:
        print(f"An error occurred during the daily playtime orchestration: {e}")
        raise


if __name__ == "__main__":
    daily_playtime_orchestrator()