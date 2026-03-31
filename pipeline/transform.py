"""
pipeline/transform.py

Cleans and standardizes real estate listing data.
Handles two data sources:
  - CSV mode:     richer data (price, surface, rooms, type, date)
  - Scraper mode: leaner data (title, price, city, category, url)

Rule: nothing dirty leaves this step.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV MODE
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw CSV data.

    Steps:
        1. Remove duplicate rows
        2. Standardize column names
        3. Clean price column (handle non-numeric values)
        4. Drop rows with invalid price or surface area
        5. Standardize text fields (city, neighborhood, type)
        6. Parse listing_date as datetime
        7. Add derived column: price_per_m2
        8. Add derived column: price_range label
        9. Log cleaning summary
    """
    logger.info("Starting CSV transformation...")
    initial_count = len(df)

    # --- Step 1: Remove exact duplicate rows ---
    df = df.drop_duplicates()

    # --- Step 2: Standardize column names ---
    # Lowercase + underscores = SQL-friendly and consistent
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # --- Step 3: Clean price column ---
    # Convert to numeric — anything unparseable becomes NaN
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # --- Step 4: Drop rows with invalid price or surface area ---
    before_drop = len(df)
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]
    df = df[df["price"] >= 1_000]
    df = df[df["price"] <= 100_000_000]
    df = df[df["surface_m2"] > 0]
    dropped = before_drop - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with invalid price or surface area.")

    # --- Step 5: Standardize text fields ---
    df["city"]         = df["city"].astype(str).str.strip().str.title()
    df["neighborhood"] = df["neighborhood"].astype(str).str.strip().str.title()
    df["type"]         = df["type"].astype(str).str.strip().str.title()

    # --- Step 6: Parse listing date ---
    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")

    # --- Step 7: Add price per m² ---
    df["price_per_m2"] = (df["price"] / df["surface_m2"]).round(2)

    # --- Step 8: Add price range label ---
    df["price_range"] = df["price"].apply(_label_price)

    # --- Step 9: Cleaning summary ---
    final_count = len(df)
    _log_summary(initial_count, final_count, city_col="city")

    return df


# ---------------------------------------------------------------------------
# SCRAPER MODE
# ---------------------------------------------------------------------------

def transform_scraped(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate scraped data from Avito.

    Steps:
        1. Drop rows missing both title and price
        2. Clean and convert price to numeric
        3. Validate price range (remove zeros and outliers)
        4. Standardize text fields (title, city, category, seller)
        5. Normalize None/nan strings to actual nulls
        6. Remove duplicates by URL
        7. Add derived column: price_range label
        8. Log cleaning summary
    """
    logger.info("Starting scraper transformation...")
    initial_count = len(df)

    # --- Step 1: Drop rows missing critical fields ---
    df = df.dropna(subset=["title", "price"], how="all")

    # --- Step 2: Clean price column ---
    # Strip everything that isn't a digit, then convert
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", None)
    )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # --- Step 3: Validate price range ---
    before_price = len(df)
    df = df[df["price"] > 0]
    df = df[df["price"] >= 1_000]           # Below 1,000 DH is not a real listing
    df = df[df["price"] <= 100_000_000]     # Above 100M DH is likely a data error
    dropped_price = before_price - len(df)
    if dropped_price > 0:
        logger.warning(f"Dropped {dropped_price} rows outside valid price range.")

    # --- Step 4: Standardize text fields ---
    df["title"]    = df["title"].astype(str).str.strip()
    df["city"]     = df["city"].astype(str).str.strip().str.title()
    df["category"] = df["category"].astype(str).str.strip().str.title()

    # --- Step 5: Normalize null-like strings in seller column ---
    if "seller" in df.columns:
        df["seller"] = df["seller"].astype(str).str.strip()
        df["seller"] = df["seller"].replace({"None": None, "nan": None, "": None})

    # --- Step 6: Remove duplicates by URL ---
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["url"])
    dropped_dedup = before_dedup - len(df)
    if dropped_dedup > 0:
        logger.warning(f"Dropped {dropped_dedup} duplicate listings.")

    # --- Step 7: Add price range label ---
    df["price_range"] = df["price"].apply(_label_price)

    # --- Step 8: Cleaning summary ---
    final_count = len(df)
    _log_summary(initial_count, final_count, city_col="city")

    return df


# ---------------------------------------------------------------------------
# SHARED UTILITIES
# ---------------------------------------------------------------------------

def _label_price(price: float) -> str:
    """
    Categorize a listing price into a human-readable range.
    Useful for analysis and dashboards when surface area is unavailable.

    Brackets (in DH):
        Budget:    < 300,000
        Mid-range: 300,000 – 999,999
        Premium:   1,000,000 – 4,999,999
        Luxury:    5,000,000+
    """
    if price < 300_000:
        return "Budget"
    elif price < 1_000_000:
        return "Mid-range"
    elif price < 5_000_000:
        return "Premium"
    else:
        return "Luxury"


def _log_summary(initial: int, final: int, city_col: str = "city") -> None:
    """
    Log a concise cleaning summary after transformation completes.
    """
    dropped = initial - final
    logger.info(
        f"Transformation complete: {initial} → {final} rows kept "
        f"({dropped} dropped, {round(dropped/initial*100, 1)}% removal rate)."
    )


def save_clean_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the cleaned DataFrame to CSV as a checkpoint.
    Useful for debugging the transform step without re-running the scraper.
    """
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"Clean data saved to {output_path}")