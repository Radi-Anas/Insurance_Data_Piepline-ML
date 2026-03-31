"""
main.py
Entry point — now uses live scraping instead of CSV.
Switch USE_SCRAPER to False to fall back to CSV during testing.
"""

import logging
from pipeline.extract import scrape_avito, extract_from_csv
from pipeline.transform import transform, transform_scraped, save_clean_csv
from pipeline.load import load_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

RAW_DATA_PATH   = "data/raw/listings.csv"
CLEAN_DATA_PATH = "data/clean/listings_clean.csv"
TABLE_NAME      = "listings"
USE_SCRAPER     = True


def run_pipeline():
    logger.info("=== Pipeline started ===")

    # Step 1: Extract
    if USE_SCRAPER:
        raw_df = scrape_avito(max_pages=3)
        if raw_df.empty:
            logger.error("Scraping returned no data. Aborting pipeline.")
            return
    else:
        raw_df = extract_from_csv(RAW_DATA_PATH)

    # Step 2: Transform
    if USE_SCRAPER:
        clean_df = transform_scraped(raw_df)
    else:
        clean_df = transform(raw_df)

    if clean_df.empty:
        logger.error("No clean data after transformation. Aborting pipeline.")
        return

    save_clean_csv(clean_df, CLEAN_DATA_PATH)

    # Step 3: Load
    load_to_postgres(clean_df, TABLE_NAME)

    logger.info("=== Pipeline finished successfully ===")


if __name__ == "__main__":
    run_pipeline()