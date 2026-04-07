import pandas as pd
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV MODE
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting CSV transformation...")
    initial_count = len(df)

    df = df.drop_duplicates()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    before_drop = len(df)
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]

    if "surface_m2" in df.columns:
        df = df[df["surface_m2"] > 0]

    dropped = before_drop - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} invalid rows.")

    df["city"] = df["city"].astype(str).str.strip().str.title()

    if "listing_date" in df.columns:
        df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")

    if "surface_m2" in df.columns:
        df["price_per_m2"] = df["price"] / df["surface_m2"]

    df["price_range"] = df["price"].apply(_label_price)

    _log_summary(initial_count, len(df))
    return df


# ---------------------------------------------------------------------------
# SCRAPER MODE 
# ---------------------------------------------------------------------------

def transform_scraped(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting scraper transformation...")
    initial_count = len(df)

    # --- Step 1: drop empty rows ---
    df = df.dropna(subset=["title", "price"], how="all")

    # --- Step 2: clean price ---
    df["price_raw"] = df["price"]  # keep original for debug

    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # DEBUG
    valid_prices = df["price"].notna().sum()
    logger.info(f"Valid numeric prices: {valid_prices}/{len(df)}")

    # --- Step 3: drop only truly invalid ---
    before_price = len(df)

    df = df[df["price"].notna()]
    df = df[df["price"] > 0]

    df = df[df["price"] < 100_000_000]

    dropped_price = before_price - len(df)
    if dropped_price > 0:
        logger.warning(f"Dropped {dropped_price} rows due to invalid price.")

    # --- Step 4: text cleanup ---
    df["title"] = df["title"].astype(str).str.strip()
    df["city"] = df["city"].astype(str).str.strip().str.title()
    df["category"] = df["category"].astype(str).str.strip().str.title()

    # --- Step 5: seller cleanup ---
    if "seller" in df.columns:
        df["seller"] = df["seller"].astype(str).str.strip()
        df["seller"] = df["seller"].replace({"None": None, "nan": None, "": None})

    # --- Step 6: deduplicate safely ---
    if "url" in df.columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["url"])
        logger.info(f"Deduplicated {before_dedup - len(df)} rows (URL).")

    # --- Step 7: price range ---
    df["price_range"] = df["price"].apply(_label_price)

    # --- Step 8: price per m2 ---
    df["price_per_m2"] = pd.Series(dtype=float)
    mask = df["surface_m2"].notna() & (df["surface_m2"] > 0)
    df.loc[mask, "price_per_m2"] = (df.loc[mask, "price"] / df.loc[mask, "surface_m2"]).astype(float)

    # --- Step 9: final check ---
    final_count = len(df)

    if final_count == 0:
        logger.error("ALL DATA DROPPED — CHECK SCRAPER OUTPUT")
        logger.error("Sample raw prices:")
        logger.error(df["price_raw"].head(10))
    else:
        _log_summary(initial_count, final_count)

    return df


# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def _label_price(price: float) -> str:
    if price < 300_000:
        return "Budget"
    elif price < 1_000_000:
        return "Mid-range"
    elif price < 5_000_000:
        return "Premium"
    else:
        return "Luxury"


def _log_summary(initial: int, final: int):
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