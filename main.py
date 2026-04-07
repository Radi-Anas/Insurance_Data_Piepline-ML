"""
main.py
Pipeline entry point - ETL orchestration.

Runs: extract → transform → validate → load
Tracks metrics and state for production-grade monitoring.

Usage:
    python main.py

Configuration:
    Set USE_SCRAPER = True to scrape live from Avito.ma
    Set USE_SCRAPER = False to use existing CSV data
    Set LOAD_MODE = "replace" or "upsert" for incremental loading
"""

import logging
import subprocess
import sys
from datetime import datetime

# Setup logging first
from pipeline.logging_config import setup_logging
setup_logging()

from pipeline.extract import scrape_avito, extract_from_csv
from pipeline.transform import transform, transform_scraped, save_clean_csv
from pipeline.validate import validate
from pipeline.load import load_to_postgres
from pipeline.pipeline_metrics import PipelineMetrics
from pipeline.pipeline_state import get_state

logger = logging.getLogger(__name__)

RAW_DATA_PATH = "data/raw/listings.csv"
CLEAN_DATA_PATH = "data/clean/listings_clean.csv"
TABLE_NAME = "listings"
USE_SCRAPER = False
LOAD_MODE = "replace"  # or "upsert" for incremental


def run_pipeline():
    """Execute the ETL pipeline with metrics tracking."""
    # Initialize metrics and state
    metrics = PipelineMetrics()
    state = get_state()
    
    try:
        state.mark_run_start(metrics.run_id)
        metrics.start_extraction()
        
        logger.info("=== Pipeline started ===")

        # ----- EXTRACT -----
        if USE_SCRAPER:
            raw_df = scrape_avito(max_pages=3)
            if raw_df.empty:
                raise Exception("Scraping returned no data")
            clean_df = transform_scraped(raw_df)
        else:
            raw_df = extract_from_csv(RAW_DATA_PATH)
            clean_df = transform(raw_df)

        # Track extraction metrics
        metrics.end_extraction(len(raw_df))

        if clean_df.empty:
            raise Exception("No clean data after transformation")

        save_clean_csv(clean_df, CLEAN_DATA_PATH)

        # ----- VALIDATE -----
        metrics.start_transformation()
        min_rows = 20 if USE_SCRAPER else 5
        rows_dropped = len(raw_df) - len(clean_df)
        
        if not validate(clean_df, min_rows=min_rows, strict_url=False):
            raise Exception("Validation failed")
        
        metrics.end_transformation(len(clean_df), rows_dropped)

        # ----- LOAD -----
        metrics.start_load()
        
        load_result = load_to_postgres(clean_df, TABLE_NAME, mode=LOAD_MODE)
        rows_loaded = (
            load_result.get("rows_inserted", 0) + 
            load_result.get("rows_updated", 0)
        )
        metrics.end_load(rows_loaded)

        # ----- FINALIZE -----
        final_metrics = metrics.finalize()
        final_metrics["processed_urls"] = clean_df["url"].tolist() if "url" in clean_df else []
        
        # Update state
        state.mark_run_success(final_metrics)
        
        # Save metrics to file
        metrics.save_to_file()
        
        logger.info("=== Pipeline finished successfully ===")
        print(f"\n{metrics.get_summary()}\n")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Pipeline failed: {error_msg}")
        
        metrics.add_error(error_msg)
        metrics.finalize()
        
        state.mark_run_failure(error_msg)
        
        raise


def main():
    """Main entry point with dashboard auto-start."""
    # Check if we should run (rate limiting)
    state = get_state()
    if state.should_rerun(min_interval_hours=1):
        run_pipeline()
    else:
        last_run = state.get_last_run()
        time_since = last_run.get("time_since_last", "Never") if last_run else "Never"
        logger.info(f"Skipping run. Last: {time_since}")
        print(f"Last run was {time_since}. Run anyway? (y/n)")
        response = input()
        if response.lower() != 'y':
            return
        run_pipeline()

    print("\n" + "=" * 50)
    print("Pipeline complete! Starting dashboard...")
    print("=" * 50)

    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "dashboard.py", "--server.port", "8501"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print("Dashboard: http://localhost:8501")


if __name__ == "__main__":
    main()