"""
main.py
Pipeline entry point.
Runs: extract → transform → validate → load

Set USE_SCRAPER = False to use CSV mode instead of live scraping.
"""

import logging
from pipeline.extract   import scrape_avito, extract_from_csv
from pipeline.transform import transform, transform_scraped, save_clean_csv
from pipeline.validate  import validate
from pipeline.load      import load_to_postgres

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

    # --- Step 1: Extract ---
    if USE_SCRAPER:
        raw_df = scrape_avito(max_pages=3)
        if raw_df.empty:
            logger.error("Scraping returned no data. Aborting.")
            return
    else:
        raw_df = extract_from_csv(RAW_DATA_PATH)

    # --- Step 2: Transform ---
    if USE_SCRAPER:
        clean_df = transform_scraped(raw_df)
    else:
        clean_df = transform(raw_df)

    if clean_df.empty:
        logger.error("No clean data after transformation. Aborting.")
        return

    save_clean_csv(clean_df, CLEAN_DATA_PATH)

    # --- Step 3: Validate ---
    if not validate(clean_df):
        logger.error("Validation failed. Data NOT loaded into PostgreSQL.")
        return

    # --- Step 4: Load ---
    load_to_postgres(clean_df, TABLE_NAME)

    logger.info("=== Pipeline finished successfully ===")


if __name__ == "__main__":
    run_pipeline()