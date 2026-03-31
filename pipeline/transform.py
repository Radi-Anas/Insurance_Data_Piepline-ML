"""
pipeline/transform.py
Cleans and standardizes the raw data.
Rule: nothing leaves this step dirty.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning and transformation steps to the raw DataFrame.

    Steps:
        1. Remove duplicate rows
        2. Standardize column names
        3. Clean the price column (handle non-numeric values)
        4. Drop rows where price or surface is missing or zero
        5. Standardize city names (strip whitespace, title case)
        6. Parse listing_date as a proper datetime
        7. Add a derived column: price_per_m2

    Args:
        df: Raw DataFrame from extract.py

    Returns:
        A clean, analysis-ready DataFrame.
    """
    logger.info("Starting transformation...")
    initial_count = len(df)

    # --- Step 1: Remove exact duplicate rows ---
    df = df.drop_duplicates()

    # --- Step 2: Standardize column names ---
    # Lowercase + replace spaces with underscores (good habit for SQL compatibility)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # --- Step 3: Clean the price column ---
    # Convert to numeric; anything that can't be parsed becomes NaN
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # --- Step 4: Drop rows with invalid price or surface ---
    before_drop = len(df)
    df = df.dropna(subset=["price"])          # Remove missing prices
    df = df[df["surface_m2"] > 0]             # Remove zero or negative surface area
    dropped = before_drop - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with invalid price or surface.")

    # --- Step 5: Standardize text fields ---
    df["city"]         = df["city"].str.strip().str.title()
    df["neighborhood"] = df["neighborhood"].str.strip().str.title()
    df["type"]         = df["type"].str.strip().str.title()

    # --- Step 6: Parse dates ---
    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")

    # --- Step 7: Add price per square meter (useful for analysis) ---
    df["price_per_m2"] = (df["price"] / df["surface_m2"]).round(2)

    # --- Final report ---
    final_count = len(df)
    logger.info(f"Transformation complete. {initial_count} → {final_count} rows kept.")

    return df


def save_clean_csv(df: pd.DataFrame, output_path: str) -> None:
    """Save the clean DataFrame to CSV as a checkpoint (useful for debugging)."""
    df.to_csv(output_path, index=False)
    logger.info(f"Clean data saved to {output_path}")
    
    
def transform_scraped(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean scraped data from Avito.
    Lighter version of transform() — scraped data has fewer columns.
    """
    logger.info("Transforming scraped data...")

    # Drop rows where both title and price are missing
    df = df.dropna(subset=["title", "price"], how="all")

    # Clean price: remove currency text, spaces, and convert to numeric
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)  # Keep digits only
        .replace("", None)
    )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Drop duplicates based on URL (each listing should be unique)
    df = df.drop_duplicates(subset=["url"])

    # Drop rows with no price
    df = df.dropna(subset=["price"])

    logger.info(f"Scraped transform complete. {len(df)} clean rows.")
    return df