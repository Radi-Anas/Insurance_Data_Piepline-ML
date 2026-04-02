"""
pipeline/validate.py

Data quality validation layer.
Runs between transform and load — if critical checks fail,
the pipeline aborts before writing bad data to PostgreSQL.

Two levels of checks:
    - CRITICAL: pipeline aborts if these fail
    - WARNING:  pipeline continues but logs the issue
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VALIDATION CHECKS
# ---------------------------------------------------------------------------

def check_minimum_rows(df: pd.DataFrame, minimum: int = 10) -> tuple:
    """
    CRITICAL: Ensure we have enough rows to be worth loading.
    If the scraper failed silently, we might get 0-5 rows.
    """
    count = len(df)
    passed = count >= minimum
    msg = (
        f"Row count: {count} (min required: {minimum})"
        if passed
        else f"Too few rows: {count} (min required: {minimum}) — scraper may have failed"
    )
    return passed, "CRITICAL", msg


def check_no_empty_dataframe(df: pd.DataFrame) -> tuple:
    """
    CRITICAL: DataFrame must not be empty.
    """
    passed = not df.empty
    msg = "DataFrame is not empty" if passed else "DataFrame is completely empty"
    return passed, "CRITICAL", msg


def check_required_columns(df: pd.DataFrame) -> tuple:
    """
    CRITICAL: These columns must exist in every run.
    """
    required = ["title", "price", "city", "url"]
    missing  = [col for col in required if col not in df.columns]
    passed   = len(missing) == 0
    msg = (
        "All required columns present"
        if passed
        else f"Missing required columns: {missing}"
    )
    return passed, "CRITICAL", msg


def check_price_not_null(df: pd.DataFrame) -> tuple:
    """
    CRITICAL: Price column must have no nulls after transformation.
    If price is null, the transform step failed.
    """
    null_count = df["price"].isna().sum()
    passed     = null_count == 0
    msg = (
        "No null prices"
        if passed
        else f"{null_count} null prices found — transform may have failed"
    )
    return passed, "CRITICAL", msg


def check_no_duplicate_urls(df: pd.DataFrame) -> tuple:
    """
    CRITICAL: Every listing must have a unique URL.
    Duplicates mean we're loading the same listing multiple times.
    """
    if "url" not in df.columns:
        return True, "CRITICAL", "URL column not present — skipping duplicate check"
    duplicate_count = df["url"].duplicated().sum()
    passed          = duplicate_count == 0
    msg = (
        "No duplicate URLs"
        if passed
        else f"{duplicate_count} duplicate URLs found"
    )
    return passed, "CRITICAL", msg


def check_price_range(df: pd.DataFrame) -> tuple:
    """
    WARNING: Flag if average price looks suspicious.
    If avg price < 10,000 DH something is wrong with parsing.
    If avg price > 50,000,000 DH something is wrong with the data.
    """
    avg_price = df["price"].mean()
    passed    = 10_000 <= avg_price <= 50_000_000
    msg = (
        f"Average price looks healthy: {avg_price:,.0f} DH"
        if passed
        else f"Suspicious average price: {avg_price:,.0f} DH — check price parsing"
    )
    return passed, "WARNING", msg


def check_city_not_null(df: pd.DataFrame) -> tuple:
    """
    WARNING: City should be populated for most listings.
    More than 50% null cities suggests a scraper structural change.
    """
    null_pct = df["city"].isna().mean() * 100
    passed   = null_pct <= 50
    msg = (
        f"City null rate: {null_pct:.1f}%"
        if passed
        else f"High city null rate: {null_pct:.1f}% — check URL parsing"
    )
    return passed, "WARNING", msg


def check_surface_coverage(df: pd.DataFrame) -> tuple:
    """
    WARNING: Track surface_m2 coverage across runs.
    Below 30% suggests the regex patterns need updating.
    """
    if "surface_m2" not in df.columns:
        return True, "WARNING", "surface_m2 column not present — skipping"
    coverage = df["surface_m2"].notna().mean() * 100
    passed   = coverage >= 30
    msg = (
        f"Surface m² coverage: {coverage:.1f}%"
        if passed
        else f"Low surface m² coverage: {coverage:.1f}% — regex may need updating"
    )
    return passed, "WARNING", msg


def check_listing_type_distribution(df: pd.DataFrame) -> tuple:
    """
    WARNING: Listing type should have both Vente and Location.
    If 100% is one type, detection logic may have broken.
    """
    if "listing_type" not in df.columns:
        return True, "WARNING", "listing_type column not present — skipping"
    distribution = df["listing_type"].value_counts(normalize=True) * 100
    passed       = len(distribution) > 1
    msg = (
        f"Listing type distribution: {distribution.to_dict()}"
        if passed
        else f"Only one listing type found: {distribution.to_dict()} — check detection"
    )
    return passed, "WARNING", msg


# ---------------------------------------------------------------------------
# MAIN VALIDATION RUNNER
# ---------------------------------------------------------------------------

def validate(
    df: pd.DataFrame,
    min_rows: int = 10,
    strict_url: bool = True,
) -> bool:
    """
    Run all validation checks on the transformed DataFrame.

    Executes every check and logs the result.
    If any CRITICAL check fails → returns False (pipeline aborts).
    If only WARNING checks fail → returns True (pipeline continues).

    Args:
        df: Cleaned DataFrame from transform step.
        min_rows: Minimum rows required (default: 10, set lower for dev)
        strict_url: Require URL column (default: True, set False for CSV mode)

    Returns:
        True if pipeline should continue, False if it should abort.
    """
    logger.info("=== Running data quality validation ===")

    checks = [
        check_no_empty_dataframe,
        lambda d: check_minimum_rows(d, min_rows),
        check_price_not_null,
    ]

    if strict_url:
        checks.append(check_required_columns)
        checks.append(check_no_duplicate_urls)

    checks.extend([
        check_price_range,
        check_city_not_null,
        check_surface_coverage,
        check_listing_type_distribution,
    ])

    passed_count  = 0
    warning_count = 0
    failed_count  = 0
    abort         = False

    for check in checks:
        try:
            passed, level, msg = check(df)

            if passed:
                logger.info(f"  [PASS] {msg}")
                passed_count += 1
            else:
                if level == "CRITICAL":
                    logger.error(f"  [FAIL] {msg}")
                    failed_count += 1
                    abort = True
                else:
                    logger.warning(f"  [WARN] {msg}")
                    warning_count += 1

        except Exception as e:
            logger.error(f"  [ERROR] Check {check.__name__} raised an exception: {e}")
            abort = True

    # --- Summary ---
    total = len(checks)
    logger.info(
        f"=== Validation complete: "
        f"{passed_count} passed, "
        f"{warning_count} warnings, "
        f"{failed_count} failed "
        f"out of {total} checks ==="
    )

    if abort:
        logger.error("CRITICAL checks failed — pipeline aborted. Data NOT loaded.")
    else:
        logger.info("All critical checks passed — proceeding to load.")

    return not abort